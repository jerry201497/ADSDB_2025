import os, re
from dotenv import load_dotenv
from src.common.minio_client import get_s3, ensure_bucket

load_dotenv()
FORMATTED_BUCKET = os.getenv("S3_BUCKET_FORMATTED", "formatted_zone")
TRUSTED_BUCKET = os.getenv("S3_BUCKET_TRUSTED", "trusted_zone")

def anonymize(text: str):
    text = re.sub(r'\b[A-Z][a-z]{2,}\s[A-Z][a-z]{2,}\b', '[NAME]', text)
    text = re.sub(r'\b\d{6,}\b', '[ID]', text)
    return text

def process_texts():
    s3 = get_s3()
    ensure_bucket(TRUSTED_BUCKET)
    src_bucket = s3.Bucket(FORMATTED_BUCKET)
    dst_bucket = s3.Bucket(TRUSTED_BUCKET)

    for obj in src_bucket.objects.filter(Prefix="text/"):
        if not obj.key.lower().endswith(".txt"):
            continue
        body = s3.Object(src_bucket.name, obj.key).get()['Body'].read()
        text = body.decode('utf-8', errors='ignore')
        text = anonymize(text)
        dst_key = obj.key.replace("text/", "text_qc/")
        dst_bucket.put_object(Key=dst_key, Body=text.encode('utf-8'))
        print("Trusted text:", dst_key)

if __name__ == "__main__":
    process_texts()
