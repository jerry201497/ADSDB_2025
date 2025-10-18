import io, os
from PIL import Image
import imagehash
from dotenv import load_dotenv
from src.common.minio_client import get_s3, ensure_bucket

load_dotenv()
FORMATTED_BUCKET = os.getenv("S3_BUCKET_FORMATTED", "formatted_zone")
TRUSTED_BUCKET = os.getenv("S3_BUCKET_TRUSTED", "trusted_zone")

MIN_W, MIN_H = 256, 256

def process_images():
    s3 = get_s3()
    ensure_bucket(TRUSTED_BUCKET)
    src_bucket = s3.Bucket(FORMATTED_BUCKET)
    dst_bucket = s3.Bucket(TRUSTED_BUCKET)

    seen_hashes = set()
    for obj in src_bucket.objects.filter(Prefix="images/"):
        if not obj.key.lower().endswith(".png"):
            continue
        body = s3.Object(src_bucket.name, obj.key).get()['Body'].read()
        try:
            img = Image.open(io.BytesIO(body))
        except Exception:
            continue
        if img.width < MIN_W or img.height < MIN_H:
            print("Drop low-res:", obj.key)
            continue
        h = imagehash.average_hash(img, hash_size=16)
        if str(h) in seen_hashes:
            print("Drop duplicate:", obj.key)
            continue
        seen_hashes.add(str(h))
        dst_key = obj.key.replace("images/", "images_qc/")
        dst_bucket.put_object(Key=dst_key, Body=body)
        print("Trusted image:", dst_key)

if __name__ == "__main__":
    process_images()
