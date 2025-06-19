# Label Studio Connector

```mermaidjs
sequenceDiagram
    autonumber
    participant User
    participant Uppy as Uppy Upload UI
    participant S3Upload as S3 Bucket (upload/)
    participant SQSQueue as SQS Queue
    participant Lambda as Lambda (Docker Image)
    participant S3Raw as S3 Bucket (raw/)
    participant S3Ingest as S3 Bucket (ingest/)
    participant LabelStudio

    User->>Uppy: Select ~100 PDF/TIFF files
    Uppy->>S3Upload: Multipart upload to upload/
    loop for each file
      Note over S3Upload,SQSQueue: S3:ObjectCreated:Put
      S3Upload->>SQSQueue: Enqueue S3 event message
    end

    Note over SQSQueue,Lambda: AWS Lambda service polls SQS in batches of 5
    loop for each batch
      SQSQueue->>Lambda: Invoke function with batch of messages
      loop for each message in batch
        Lambda->>S3Upload: Download upload/<file>
        Lambda->>S3Raw: Copy to raw/<file> & delete upload/<file>
        Lambda->>Lambda: Convert PDF→TIFF or normalize TIFF
        Lambda->>S3Raw: Upload raw/<file>.tiff
        Lambda->>S3Ingest: Upload ingest/<file>.tiff + TASK_XXXXX.json
      end
      Lambda-->>SQSQueue: Delete processed messages
    end

    Note over S3Ingest,LabelStudio: Label Studio polls ingest/ prefix
    S3Ingest->>LabelStudio: New tasks appear for annotation
```