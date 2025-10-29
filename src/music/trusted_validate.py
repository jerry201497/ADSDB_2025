
import os, json
from pathlib import Path
from dotenv import load_dotenv
import boto3
import pandas as pd
from PIL import Image
import imagehash

def s3():
    load_dotenv()
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD"),
    )

def download(bucket, key, local):
    Path(local).parent.mkdir(parents=True, exist_ok=True)
    s3().download_file(bucket, key, str(local))

def upload(bucket, local, key, ct=None):
    extra = {"ContentType": ct} if ct else {}
    s3().upload_file(str(local), bucket, key, ExtraArgs=extra)

def main():
    load_dotenv()
    b_in = os.getenv("S3_BUCKET_FORMATTED","formatted_zone")
    b_out = os.getenv("S3_BUCKET_TRUSTED","trusted_zone")

    download(b_in, "music/tracks.parquet", "cache/formatted/tracks.parquet")
    df = pd.read_parquet("cache/formatted/tracks.parquet")

    before = len(df)
    df = df.drop_duplicates(subset=["track_name","artist_name","album_name"])
    df = df[(df["track_name"]!="") & (df["artist_name"]!="")]
    after = len(df)

    Path("cache/trusted").mkdir(parents=True, exist_ok=True)
    df.to_parquet("cache/trusted/tracks_qc.parquet", index=False)
    upload(b_out, "cache/trusted/tracks_qc.parquet", "music/tracks_qc.parquet")

    res = s3().list_objects_v2(Bucket=b_in, Prefix="music/covers/")
    kept, dropped = 0, 0
    seen = set()
    for obj in (res.get("Contents") or []):
        key = obj["Key"]
        if not key.lower().endswith(".png"): continue
        local = Path("cache/formatted/covers")/Path(key).name
        download(b_in, key, local)
        h = str(imagehash.average_hash(Image.open(local)))
        if h in seen:
            dropped += 1
            continue
        seen.add(h)
        kept += 1
        upload(b_out, local, f"music/covers_qc/{Path(local).name}", ct="image/png")

    report = {"tracks_before": before, "tracks_after": after, "covers_kept": kept, "covers_dropped": dropped}
    Path("reports").mkdir(exist_ok=True)
    Path("reports/music_quality_report.json").write_text(json.dumps(report, indent=2))
    upload(b_out, "reports/music_quality_report.json", "music/reports/quality_report.json", ct="application/json")

    print("=== Trusted summary ===")
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
