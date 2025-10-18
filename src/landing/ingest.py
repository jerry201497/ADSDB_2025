import os
from pathlib import Path
from dotenv import load_dotenv
from src.common.minio_client import get_s3, ensure_bucket
from src.common.utils import timestamp, safe_name

load_dotenv()

LANDING_BUCKET = os.getenv("S3_BUCKET_LANDING", "landing_zone")

def upload_dir(local_dir: str, subprefix: str):
    s3 = get_s3()
    bucket = ensure_bucket(LANDING_BUCKET)
    tmp_prefix = f"temporal_landing/{subprefix}/"
    for root, _, files in os.walk(local_dir):
        for fn in files:
            src = Path(root)/fn
            key = tmp_prefix + safe_name(fn)
            bucket.upload_file(str(src), key)
            print("Uploaded:", key)

def persist_all():
    s3 = get_s3()
    bucket = ensure_bucket(LANDING_BUCKET)
    objs = list(bucket.objects.filter(Prefix="temporal_landing/"))
    ingest_ts = timestamp()
    for obj in objs:
        parts = obj.key.split("/", 2)
        if len(parts) < 3:
            continue
        subdir = parts[1]
        filename = parts[2]
        new_key = f"persistent_landing/{subdir}/{ingest_ts}/{filename}"
        s3.Object(bucket.name, new_key).copy_from(CopySource={'Bucket': bucket.name, 'Key': obj.key})
        s3.Object(bucket.name, obj.key).delete()
        print("Persisted:", new_key)

if __name__ == "__main__":
    upload_dir("data/raw_samples/images", "images")
    upload_dir("data/raw_samples/text", "text")
    persist_all()
