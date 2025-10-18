import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv

load_dotenv()

def get_s3():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    region = os.getenv("MINIO_REGION", "us-east-1")
    access_key = os.getenv("MINIO_ROOT_USER")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD")
    return boto3.resource(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name=region,
    )

def ensure_bucket(bucket_name: str):
    s3 = get_s3()
    try:
        s3.create_bucket(Bucket=bucket_name)
    except Exception:
        pass
    return s3.Bucket(bucket_name)
