import subprocess, sys

steps = [
    ("Landing: ingest", ["python", "-m", "src.landing.ingest"]),
    ("Formatted: homogenize", ["python", "-m", "src.formatted.format_images_text"]),
    ("Trusted: image QC", ["python", "-m", "src.trusted.image_qc"]),
    ("Trusted: text QC", ["python", "-m", "src.trusted.text_qc"]),
    ("Exploitation: build embeddings", ["python", "-m", "src.exploitation.build_embeddings"]),
]

for name, cmd in steps:
    print(f"\n=== {name} ===")
    rc = subprocess.call(cmd)
    if rc != 0:
        print(f"Step failed: {name}")
        sys.exit(rc)

print("\nPipeline finished successfully.")
