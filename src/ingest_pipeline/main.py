import os
import re
import json
import tempfile
import boto3
from PIL import Image
from pdf2image import convert_from_path

s3 = boto3.client("s3")

def lambda_handler(event, context):
    # Process each SQS message
    for record in event["Records"]:
        try:
            # Parse the SQS message body which contains the S3 event
            s3_event = json.loads(record["body"])
            
            # Extract S3 details from the event
            s3rec = s3_event["Records"][0]["s3"]
            bucket = s3rec["bucket"]["name"]
            key = s3rec["object"]["key"]
            
            # 1. Ensure we only process files in "upload/" prefix
            if not key.startswith("upload/"):
                print(f"[LOG] Skipping non-upload key: {key}")
                continue

            base, ext = os.path.splitext(os.path.basename(key))
            ext = ext.lower()
            if ext not in (".pdf", ".tiff", ".tif"):
                print(f"[LOG] Skipping unsupported file type: {key}")
                continue

            # Work in a temp dir
            with tempfile.TemporaryDirectory() as tmp:
                local_in = os.path.join(tmp, "input" + ext)
                local_tif = os.path.join(tmp, f"{base}.tiff")
                local_js = os.path.join(tmp, "task.json")

                # Download original
                s3.download_file(bucket, key, local_in)

                # 2. Move the original file to "raw/" prefix
                raw_orig_key = f"raw/{base}{ext}"
                s3.upload_file(local_in, bucket, raw_orig_key)
                s3.delete_object(Bucket=bucket, Key=key)
                print(f"[LOG] Moved original {key} -> {raw_orig_key}")

                # 3. Convert PDF to TIFF or normalize TIFF
                if ext == ".pdf":
                    pages = convert_from_path(local_in, dpi=300, fmt="tiff")
                    pages[0].save(local_tif, save_all=True)
                else:
                    img = Image.open(local_in)
                    img.save(local_tif, format="TIFF")

                # 4. Upload converted TIFF to "raw/"
                raw_tif_key = f"raw/{base}.tiff"
                s3.upload_file(local_tif, bucket, raw_tif_key)
                print(f"[LOG] Uploaded TIFF to {raw_tif_key}")

                # 5. Determine next JSON filename in "ingest/"
                seq = 1
                resp = s3.list_objects_v2(Bucket=bucket, Prefix="ingest/")
                if "Contents" in resp:
                    nums = []
                    for obj in resp["Contents"]:
                        name = os.path.basename(obj["Key"])
                        m = re.match(r"TASK_(\d+)\.json", name)
                        if m:
                            nums.append(int(m.group(1)))
                    if nums:
                        seq = max(nums) + 1

                json_name = f"TASK_{seq:07d}.json"
                ingest_key = f"ingest/{json_name}"

                # 6. Write and upload JSON task
                task = {"data": {"image": f"s3://{bucket}/{raw_tif_key}"}}
                with open(local_js, "w") as f:
                    json.dump([task], f, indent=4)

                s3.upload_file(local_js, bucket, ingest_key)
                print(f"[LOG] Uploaded JSON to {ingest_key}")

        except Exception as e:
            print(f"[ERROR] Error processing record: {record}, Error: {e}")

    return {"status": "success"}
