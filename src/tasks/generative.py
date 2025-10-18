import argparse
from PIL import Image
from transformers import pipeline

def caption_image(path: str):
    cap = pipeline("image-to-text", model="Salesforce/blip-image-captioning-base")
    img = Image.open(path).convert("RGB")
    out = cap(img, max_new_tokens=50)
    print(out[0]["generated_text"])

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--image_path", required=True)
    args = ap.parse_args()
    caption_image(args.image_path)
