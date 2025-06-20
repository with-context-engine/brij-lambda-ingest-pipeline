terraform {
  required_version = "~> 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "5.43.0"
    }
    klayers = {
      version = "~> 1.0.0"
      source  = "ldcorentin/klayer"
    }
  }
}

variable "aws_region" {
  description = "The AWS region to deploy resources in."
  type        = string
  default     = "us-east-1"
}

provider "aws" {
  region = var.aws_region
}

# ----------------------------------------
# 1. Reference existing S3 bucket
# ----------------------------------------
data "aws_s3_bucket" "brij_v1_bucket" {
  bucket = "brij-v1-bucket"
}

# ----------------------------------------
# 2. Create SQS queue for decoupling
# ----------------------------------------
resource "aws_sqs_queue" "brij_v1_upload_queue" {
  name                       = "brij-v1-upload-queue"
  visibility_timeout_seconds = 1500
  message_retention_seconds  = 86400
}

# ----------------------------------------
# 3. Allow S3 to SendMessage to SQS Queue
# ----------------------------------------
resource "aws_sqs_queue_policy" "allow_s3_send_message" {
  queue_url = aws_sqs_queue.brij_v1_upload_queue.id

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Sid       = "Allow-S3-SendMessage",
        Effect    = "Allow",
        Principal = { Service = "s3.amazonaws.com" },
        Action    = "sqs:SendMessage",
        Resource  = aws_sqs_queue.brij_v1_upload_queue.arn,
        Condition = {
          ArnEquals = { "aws:SourceArn" = data.aws_s3_bucket.brij_v1_bucket.arn }
        }
      }
    ]
  })
}

# ----------------------------------------
# 4. IAM Role & Policies for Lambda
# ----------------------------------------
resource "aws_iam_role" "brij_v1_lambda_role" {
  name = "brij-v1-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action    = "sts:AssumeRole",
      Effect    = "Allow",
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
}

# Attach basic execution policy for CloudWatch logs
resource "aws_iam_role_policy_attachment" "brij_v1_lambda_logging" {
  role       = aws_iam_role.brij_v1_lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Inline S3 read/write policy
resource "aws_iam_role_policy" "brij_v1_lambda_s3_policy" {
  name = "brij-v1-lambda-s3-policy"
  role = aws_iam_role.brij_v1_lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject",
          "s3:DeleteObject"
        ],
        Resource = [
          data.aws_s3_bucket.brij_v1_bucket.arn,
          "${data.aws_s3_bucket.brij_v1_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Inline SQS receive policy for Lambda to poll the queue
resource "aws_iam_role_policy" "brij_v1_lambda_sqs_policy" {
  name = "brij-v1-lambda-sqs-policy"
  role = aws_iam_role.brij_v1_lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "sqs:ReceiveMessage",
          "sqs:DeleteMessage",
          "sqs:GetQueueAttributes"
        ],
        Resource = aws_sqs_queue.brij_v1_upload_queue.arn
      }
    ]
  })
}

# ----------------------------------------
# 5. S3 Bucket Notification → SQS
# ----------------------------------------
resource "aws_s3_bucket_notification" "upload_notify" {
  bucket = data.aws_s3_bucket.brij_v1_bucket.id

  queue {
    queue_arn     = aws_sqs_queue.brij_v1_upload_queue.arn
    events        = ["s3:ObjectCreated:Put"]
    filter_prefix = "upload/"
  }

  depends_on = [
    aws_sqs_queue.brij_v1_upload_queue,
    aws_sqs_queue_policy.allow_s3_send_message
  ]
}

# ----------------------------------------
# 6. Create Lambda deployment package
# ----------------------------------------
data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/lambda_function.zip"

  source {
    content  = file("${path.module}/src/ingest_pipeline/main.py")
    filename = "main.py"
  }

  source {
    content  = file("${path.module}/src/ingest_pipeline/__init__.py")
    filename = "__init__.py"
  }
}

# ----------------------------------------
# 7. Lambda Layer for PyMuPDF using Klayers
# ----------------------------------------
data "klayers_package_latest_version" "pymupdf" {
  name           = "PyMuPDF"
  python_version = "3.12"
  region         = var.aws_region
}

# ----------------------------------------
# 8. Lambda Function using zip file with PyMuPDF layer
# ----------------------------------------
resource "aws_lambda_function" "converter" {
  function_name = "labelstudio-preprocessor"
  role          = aws_iam_role.brij_v1_lambda_role.arn

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  runtime = "python3.12"
  handler = "main.lambda_handler"
  layers  = [data.klayers_package_latest_version.pymupdf.arn]

  timeout = 900
  ephemeral_storage { size = 4096 }
  memory_size = 4096

  depends_on = [
    data.archive_file.lambda_zip
  ]
}

# ----------------------------------------
# 9. Allow SQS to invoke Lambda
# ----------------------------------------
resource "aws_lambda_permission" "allow_sqs" {
  statement_id  = "AllowSQSInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.converter.function_name
  principal     = "sqs.amazonaws.com"
  source_arn    = aws_sqs_queue.brij_v1_upload_queue.arn
}

# ----------------------------------------
# 10. Event Source Mapping: SQS → Lambda
# ----------------------------------------
resource "aws_lambda_event_source_mapping" "sqs_to_lambda" {
  event_source_arn = aws_sqs_queue.brij_v1_upload_queue.arn
  function_name    = aws_lambda_function.converter.arn
  batch_size       = 5
  enabled          = true
}
