import os
import re
import json
import tempfile
import boto3
import urllib.parse
from PIL import Image
from pdf2image import convert_from_path

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
    """Process a single PDF page: save as TIFF and create task."""
    # Create unique filename for this page
    page_base = f"{base_name}_page{page_num:03d}"
    page_tif = os.path.join(tmp_dir, f"{page_base}.tiff")
    
    # Save page as TIFF
    page.save(page_tif, format="TIFF", compression="tiff_lzw")
    print(f"[LOG] Saved page {page_num} as TIFF: {page_tif}")
    
    # Upload page TIFF to "raw/"
    raw_tif_key = f"raw/{page_base}.tiff"
    s3.upload_file(page_tif, bucket, raw_tif_key)
    print(f"[LOG] Uploaded page {page_num} TIFF to {raw_tif_key}")
    
    # Create and upload task
    seq = get_next_task_sequence(bucket)
    image_path = f"s3://{bucket}/{raw_tif_key}"
    create_and_upload_task(bucket, image_path, seq, tmp_dir)
    
    return raw_tif_key


def process_pdf(local_file, base_name, bucket, tmp_dir):
    """Convert PDF to TIFF images and create tasks for each page."""
    print(f"[LOG] Converting PDF to TIFF: {local_file}")
    
    # Convert PDF pages to images
    pages = convert_from_path(local_file, dpi=300)
    if not pages:
        raise Exception("No pages were converted from PDF")
    
    print(f"[LOG] Converted {len(pages)} pages from PDF")
    
    # Process each page
    processed_keys = []
    for page_num, page in enumerate(pages, start=1):
        key = process_pdf_page(page, page_num, base_name, bucket, tmp_dir)
        processed_keys.append(key)
    
    return processed_keys


def process_tiff(local_file, base_name, bucket, tmp_dir):
    """Normalize TIFF file and create task."""
    print(f"[LOG] Normalizing TIFF: {local_file}")
    
    # Normalize and save TIFF
    local_tif = os.path.join(tmp_dir, f"{base_name}.tiff")
    img = Image.open(local_file)
    img.save(local_tif, format="TIFF", compression="tiff_lzw")
    print(f"[LOG] Successfully normalized TIFF to: {local_tif}")
    
    # Upload to S3
    raw_tif_key = f"raw/{base_name}.tiff"
    s3.upload_file(local_tif, bucket, raw_tif_key)
    print(f"[LOG] Uploaded TIFF to {raw_tif_key}")
    
    # Create and upload task
    seq = get_next_task_sequence(bucket)
    image_path = f"s3://{bucket}/{raw_tif_key}"
    create_and_upload_task(bucket, image_path, seq, tmp_dir)
    
    return [raw_tif_key]


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
    if ext not in (".pdf", ".tiff", ".tif"):
        print(f"[LOG] Skipping unsupported file type: {key}")
        return
    
    # Work in a temp directory
    with tempfile.TemporaryDirectory() as tmp:
        local_in = os.path.join(tmp, "input" + ext)
        
        # Download original file
        s3.download_file(bucket, key, local_in)
        
        # Move the original file to "raw/" prefix
        move_original_to_raw(local_in, bucket, key, base, ext)
        
        # Process based on file type
        try:
            if ext == ".pdf":
                process_pdf(local_in, base, bucket, tmp)
            else:
                process_tiff(local_in, base, bucket, tmp)
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
    
    return {"status": "success"}
