
import os, json, re
from pathlib import Path
from dotenv import load_dotenv
import boto3
import pandas as pd
from PIL import Image

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

def upload(bucket, local, key, content_type=None):
    extra = {"ContentType": content_type} if content_type else {}
    s3().upload_file(str(local), bucket, key, ExtraArgs=extra)

def normalize_text(s):
    if pd.isna(s): return ""
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)
    return s

def main():
    load_dotenv()
    b_in = os.getenv("S3_BUCKET_LANDING","landing_zone")
    b_out = os.getenv("S3_BUCKET_FORMATTED","formatted_zone")

    download(b_in, "music/persistent_landing/tracks_enriched.csv", "cache/landing/tracks_enriched.csv")
    df = pd.read_csv("cache/landing/tracks_enriched.csv")

    for col in ["track_name","artist_name","album_name","genre"]:
        if col in df.columns:
            df[col] = df[col].apply(normalize_text).str.lower()

    if "spotify_genres" in df.columns:
        df["spotify_genres"] = df["spotify_genres"].fillna("").astype(str)
    if "genre" not in df.columns:
        df["genre"] = ""
    df["genre_unified"] = df.apply(lambda r: (r["genre"] + " " + r["spotify_genres"]).strip(), axis=1)

    out_meta = Path("cache/formatted/tracks.parquet"); out_meta.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_meta, index=False)
    upload(b_out, out_meta, "music/tracks.parquet")

    res = s3().list_objects_v2(Bucket=b_in, Prefix="music/persistent_landing/covers/")
    converted = 0
    for obj in (res.get("Contents") or []):
        key = obj["Key"]
        if not key.lower().endswith(".jpg"): continue
        local = Path("cache/landing")/Path(key).name
        download(b_in, key, local)
        im = Image.open(local).convert("RGB").resize((256,256))
        outp = Path("cache/formatted/covers")/Path(local).with_suffix(".png").name
        outp.parent.mkdir(parents=True, exist_ok=True)
        im.save(outp)
        upload(b_out, outp, f"music/covers/{outp.name}", content_type="image/png")
        converted += 1

    summary = {"formatted_rows": int(len(df)), "covers_converted": converted}
    Path("cache/formatted/format_summary.json").write_text(json.dumps(summary, indent=2))
    upload(b_out, "cache/formatted/format_summary.json", "music/summary/format_summary.json", content_type="application/json")

    print("=== Formatted summary ===")
    print(json.dumps(summary, indent=2))

if __name__ == "__main__":
    main()
