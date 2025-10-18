import os, argparse
import chromadb
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction
from dotenv import load_dotenv

load_dotenv()
CHROMA_DIR = os.getenv("CHROMADB_DIR", "./chroma_db")
COLL_CLIP = "clip_multimodal"

def main(text: str, top_k: int):
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    ef = OpenCLIPEmbeddingFunction()
    coll = client.get_collection(COLL_CLIP, embedding_function=ef)
    results = coll.query(query_texts=[text], n_results=top_k)
    for i,(id_,meta) in enumerate(zip(results["ids"][0], results["metadatas"][0]), start=1):
        print(f"{i}. {id_}  |  modality={meta.get('modality')}  | s3={meta.get('s3_key')}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--text", required=True)
    ap.add_argument("--top_k", type=int, default=5)
    args = ap.parse_args()
    main(args.text, args.top_k)
