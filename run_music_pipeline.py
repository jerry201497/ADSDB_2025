
import subprocess, sys

steps = [
    ("Landing: ingest + enrich", "python -m src.music.landing_ingest"),
    ("Formatted: normalize + covers->png", "python -m src.music.formatted_prepare"),
    ("Trusted: QC dedupe + hash", "python -m src.music.trusted_validate"),
    ("Exploitation: index chroma", "python -m src.music.exploitation_index"),
]

def run_step(title, cmd):
    print(f"\n=== {title} ===")
    r = subprocess.run(cmd, shell=True)
    if r.returncode != 0:
        print(f"Step failed: {title}")
        sys.exit(1)

if __name__ == "__main__":
    for t, c in steps:
        run_step(t, c)
    print("\nMusic pipeline finished successfully.")
