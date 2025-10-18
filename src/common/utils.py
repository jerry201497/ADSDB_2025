import re, hashlib
from datetime import datetime

def timestamp():
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

def safe_name(name: str):
    return re.sub(r'[^a-zA-Z0-9._-]+', '_', name)

def file_hash(path: str, chunk=8192):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        while True:
            b = f.read(chunk)
            if not b: break
            h.update(b)
    return h.hexdigest()[:16]
