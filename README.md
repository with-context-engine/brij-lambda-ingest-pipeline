# Ingest Pipeline

A serverless AWS Lambda pipeline that processes PDF files uploaded to S3, converts them to PNG images, and creates Label Studio annotation tasks.

## Architecture

The pipeline uses:
- **S3**: File storage (PDFs uploaded to `upload/`, PNGs stored in `raw/`, tasks in `ingest/`)
- **SQS**: Decouples S3 events from Lambda processing
- **Lambda**: Processes files using PyMuPDF for PDF-to-PNG conversion
- **PyMuPDF**: Fast PDF rendering library for high-quality PNG output

## Features

- 📄 **PDF Processing**: Converts multi-page PDFs to individual PNG images
- 🖼️ **PNG Support**: Direct PNG file processing and task creation
- 📊 **Label Studio Integration**: Automatically creates annotation tasks
- ⚡ **Serverless**: Scales automatically with upload volume
- 🔄 **Error Handling**: Robust error handling and logging

## File Processing

### PDF Files
- Input: `s3://bucket/upload/document.pdf`
- Output: `s3://bucket/raw/document_0001.png`, `document_0002.png`, etc.
- Tasks: Creates JSON task files in `s3://bucket/ingest/TASK_XXXXXXX.json`
- Original: PDF remains in `upload/` folder

### PNG Files
- Input: `s3://bucket/upload/image.png`
- Output: `s3://bucket/raw/image.png` (moved from upload)
- Tasks: Creates JSON task file in `s3://bucket/ingest/TASK_XXXXXXX.json`

## Deployment

### Prerequisites

- AWS CLI configured with appropriate permissions
- Terraform >= 1.6
- Python 3.11
- pip
- zip

### Quick Deployment

```bash
# Make scripts executable (if not already)
chmod +x scripts/build_layer.sh scripts/deploy.sh

# Deploy everything
./scripts/deploy.sh
```

### Manual Deployment

1. **Build PyMuPDF Layer**:
   ```bash
   ./scripts/build_layer.sh
   ```

2. **Deploy Infrastructure**:
   ```bash
   terraform init
   terraform plan
   terraform apply
   ```

## Development

### Local Testing

```bash
# Install development dependencies
pip install -r requirements.txt

# Run tests
pytest __test__/
```

### Project Structure

```
ingest-pipeline/
├── src/ingest_pipeline/
│   ├── main.py              # Lambda handler
│   └── __init__.py
├── __test__/
│   ├── test_main.py         # Test suite
│   └── artifacts/
│       └── test_pdf.pdf     # Test file
├── scripts/
│   ├── build_layer.sh       # Build PyMuPDF layer
│   └── deploy.sh           # Complete deployment
├── terraform.tf            # Infrastructure as code
├── requirements.txt         # Python dependencies
└── README.md
```

## Configuration

The pipeline is configured for:
- **S3 Bucket**: `brij-v1-bucket`
- **Upload Prefix**: `upload/`
- **Raw Storage**: `raw/`
- **Task Storage**: `ingest/`
- **Lambda Timeout**: 15 minutes
- **SQS Visibility**: 25 minutes

## Monitoring

- **CloudWatch Logs**: Lambda execution logs
- **SQS Metrics**: Queue depth and processing rates
- **S3 Events**: File upload notifications

## Image Quality

PNG files are generated with:
- **2x scaling**: High-resolution output for better annotation quality
- **Lossless compression**: Preserves image fidelity
- **Standard PNG format**: Compatible with Label Studio

## Error Handling

The pipeline handles:
- Invalid PDF files
- Corrupted uploads
- Processing timeouts
- S3 access errors
- Memory limitations

Failed processing attempts are logged to CloudWatch for debugging.

## Cost Optimization

- **Lambda**: Only runs when files are uploaded
- **SQS**: Buffers requests to handle traffic spikes
- **S3**: Lifecycle policies can archive old files
- **Layers**: PyMuPDF shared across deployments