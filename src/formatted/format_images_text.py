import io, os
from PIL import Image
from dotenv import load_dotenv
from src.common.minio_client import get_s3, ensure_bucket

load_dotenv()
LANDING_BUCKET = os.getenv("S3_BUCKET_LANDING", "landing_zone")
FORMATTED_BUCKET = os.getenv("S3_BUCKET_FORMATTED", "formatted_zone")

def ensure_buckets():
    ensure_bucket(FORMATTED_BUCKET)

def convert_images_to_png():
    s3 = get_s3()
    ensure_buckets()
    src_bucket = s3.Bucket(LANDING_BUCKET)
    dst_bucket = s3.Bucket(FORMATTED_BUCKET)

    for obj in src_bucket.objects.filter(Prefix="persistent_landing/images/"):
        if obj.key.endswith('/'):
            continue
        body = s3.Object(src_bucket.name, obj.key).get()['Body'].read()
        try:
            img = Image.open(io.BytesIO(body)).convert("L")
        except Exception:
            print("Skip non-image:", obj.key)
            continue
        out = io.BytesIO()
        img.save(out, format="PNG")
        out.seek(0)
        target_key = obj.key.replace("persistent_landing", "images").rsplit(".", 1)[0] + ".png"
        dst_bucket.put_object(Key=target_key, Body=out.read())
        print("Formatted image:", target_key)

def normalize_texts():
    s3 = get_s3()
    ensure_buckets()
    src_bucket = s3.Bucket(LANDING_BUCKET)
    dst_bucket = s3.Bucket(FORMATTED_BUCKET)
    for obj in src_bucket.objects.filter(Prefix="persistent_landing/text/"):
        if obj.key.endswith('/'):
            continue
        body = s3.Object(src_bucket.name, obj.key).get()['Body'].read()
        text = body.decode('utf-8', errors='ignore')
        text = ' '.join(text.split())
        target_key = obj.key.replace("persistent_landing", "text").rsplit(".", 1)[0] + ".txt"
        dst_bucket.put_object(Key=target_key, Body=text.encode('utf-8'))
        print("Formatted text:", target_key)

if __name__ == "__main__":
    convert_images_to_png()
    normalize_texts()
