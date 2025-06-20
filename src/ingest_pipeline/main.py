import os
import re
import json
import tempfile
import boto3
import urllib.parse
import fitz
import requests

s3 = boto3.client("s3")


def get_next_task_sequence(bucket, prefix="ingest/"):
    """Get the next available task sequence number."""
    seq = 1
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    if "Contents" in resp:
        nums = []
        for obj in resp["Contents"]:
            name = os.path.basename(obj["Key"])
            m = re.match(r"TASK_(\d+)\.json", name)
            if m:
                nums.append(int(m.group(1)))
        if nums:
            seq = max(nums) + 1
    return seq


def create_and_upload_task(bucket, image_s3_path, seq_num, tmp_dir):
    """Create a JSON task file and upload it to S3."""
    json_name = f"TASK_{seq_num:07d}.json"
    ingest_key = f"ingest/{json_name}"
    local_js = os.path.join(tmp_dir, f"task_{seq_num}.json")
    
    task = {"data": {"image": image_s3_path}}
    with open(local_js, "w") as f:
        json.dump([task], f, indent=4)
    
    s3.upload_file(local_js, bucket, ingest_key)
    print(f"[LOG] Uploaded JSON task to {ingest_key}")
    return ingest_key


def process_pdf_page(page, page_num, base_name, bucket, tmp_dir):
    """Process a single PDF page: save as PNG and create task."""
    # Create unique filename for this page
    page_base = f"{base_name}_{page_num:04d}"
    page_png = os.path.join(tmp_dir, f"{page_base}.png")
    
    # Render page as PNG using PyMuPDF
    pix = page.get_pixmap(matrix=fitz.Matrix(2.0, 2.0))
    pix.save(page_png)
    pix = None
    print(f"[LOG] Saved page {page_num} as PNG: {page_png}")
    
    # Upload page PNG to "raw/"
    raw_png_key = f"raw/{page_base}.png"
    s3.upload_file(page_png, bucket, raw_png_key)
    print(f"[LOG] Uploaded page {page_num} PNG to {raw_png_key}")
    
    # Create and upload task
    seq = get_next_task_sequence(bucket)
    image_path = f"s3://{bucket}/{raw_png_key}"
    create_and_upload_task(bucket, image_path, seq, tmp_dir)
    
    return raw_png_key


def process_pdf(local_file, base_name, bucket, tmp_dir):
    """Convert PDF to PNG images and create tasks for each page."""
    print(f"[LOG] Converting PDF to PNG: {local_file}")
    
    # Open PDF with PyMuPDF
    doc = fitz.open(local_file)
    if doc.page_count == 0:
        doc.close()
        raise Exception("No pages found in PDF")
    
    print(f"[LOG] PDF has {doc.page_count} pages")
    
    # Process each page
    processed_keys = []
    for page_num in range(doc.page_count):
        page = doc[page_num]
        key = process_pdf_page(page, page_num + 1, base_name, bucket, tmp_dir)
        processed_keys.append(key)
    
    doc.close()
    return processed_keys


def process_png(local_file, base_name, bucket, tmp_dir):
    """Process PNG file and create task."""
    print(f"[LOG] Processing PNG: {local_file}")
    
    # Copy PNG to standardized location
    local_png = os.path.join(tmp_dir, f"{base_name}.png")
    # Just copy the file (no processing needed)
    with open(local_file, 'rb') as src, open(local_png, 'wb') as dst:
        dst.write(src.read())
    print(f"[LOG] Copied PNG to: {local_png}")
    
    # Upload to S3
    raw_png_key = f"raw/{base_name}.png"
    s3.upload_file(local_png, bucket, raw_png_key)
    print(f"[LOG] Uploaded PNG to {raw_png_key}")
    
    # Create and upload task
    seq = get_next_task_sequence(bucket)
    image_path = f"s3://{bucket}/{raw_png_key}"
    create_and_upload_task(bucket, image_path, seq, tmp_dir)
    
    return [raw_png_key]


def move_original_to_raw(local_file, bucket, original_key, base_name, ext):
    """Move the original uploaded file to the raw/ prefix."""
    raw_orig_key = f"raw/{base_name}{ext}"
    s3.upload_file(local_file, bucket, raw_orig_key)
    s3.delete_object(Bucket=bucket, Key=original_key)
    print(f"[LOG] Moved original {original_key} -> {raw_orig_key}")
    return raw_orig_key


def process_s3_record(record):
    """Process a single S3 record from SQS message."""
    # Parse the SQS message body which contains the S3 event
    s3_event = json.loads(record["body"])
    
    # Extract S3 details from the event
    s3rec = s3_event["Records"][0]["s3"]
    bucket = s3rec["bucket"]["name"]
    # The object key is URL-encoded in the S3 event
    key = urllib.parse.unquote_plus(s3rec["object"]["key"])
    
    # Ensure we only process files in "upload/" prefix
    if not key.startswith("upload/"):
        print(f"[LOG] Skipping non-upload key: {key}")
        return
    
    base, ext = os.path.splitext(os.path.basename(key))
    ext = ext.lower()
    if ext not in (".pdf", ".png"):
        print(f"[LOG] Skipping unsupported file type: {key}")
        return
    
    # Work in a temp directory
    with tempfile.TemporaryDirectory() as tmp:
        local_in = os.path.join(tmp, "input" + ext)
        
        # Download original file
        s3.download_file(bucket, key, local_in)
        
        # Only move PNGs to raw/, leave PDFs in upload/
        if ext == ".png":
            move_original_to_raw(local_in, bucket, key, base, ext)
        
        # Process based on file type
        try:
            if ext == ".pdf":
                process_pdf(local_in, base, bucket, tmp)
            else:
                process_png(local_in, base, bucket, tmp)
        except Exception as e:
            print(f"[ERROR] Failed to process {key}: {str(e)}")
            raise


def lambda_handler(event, context):
    """Main Lambda handler function."""
    # Process each SQS message
    for record in event["Records"]:
        try:
            process_s3_record(record)
        except Exception as e:
            print(f"[ERROR] Error processing record: {record}, Error: {e}")

    # After processing all records, trigger storage sync
    sync_status = "failed"
    sync_response_json = {}
    try:
        url = "https://brij-annotate.with-context.co/api/storages/s3/1/sync"
        token = os.getenv("CONTEXT_TOKEN")
        headers = {"Authorization": f"Token {token}"}

        print("[LOG] Triggering storage sync.")
        response = requests.post(url, headers=headers)
        response.raise_for_status()  # Raises an HTTPError for bad responses

        sync_response_json = response.json()

        if sync_response_json.get("status") == "completed":
            sync_status = "success"
            print("[LOG] Storage sync completed successfully.")
        else:
            print(f"[LOG] Storage sync status: {sync_response_json.get('status')}")

    except requests.exceptions.RequestException as e:
        print(f"[ERROR] Failed to trigger storage sync: {e}")
        sync_response_json = {"error": str(e)}

    return {
        "status": "success",
        "sync": {
            "status": sync_status,
            "response": sync_response_json,
        },
    }
