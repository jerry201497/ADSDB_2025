import os, argparse
import chromadb
from chromadb.utils.embedding_functions import OpenCLIPEmbeddingFunction
from dotenv import load_dotenv

load_dotenv()
CHROMA_DIR = os.getenv("CHROMADB_DIR", "./chroma_db")
COLL_IMAGES = "images_embeddings"

def main(query_path: str, top_k: int):
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    ef = OpenCLIPEmbeddingFunction()
    coll = client.get_collection(COLL_IMAGES, embedding_function=ef)
    results = coll.query(query_texts=[query_path], n_results=top_k)
    for i,(id_,meta) in enumerate(zip(results["ids"][0], results["metadatas"][0]), start=1):
        print(f"{i}. {id_}  |  {meta}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--query_path", required=True)
    ap.add_argument("--top_k", type=int, default=5)
    args = ap.parse_args()
    main(args.query_path, args.top_k)
