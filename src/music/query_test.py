import chromadb

def query_music(prompt: str, n_results=5):
    client = chromadb.PersistentClient(path="chroma_db")
    col = client.get_or_create_collection("music_multimodal")

    print(f"\n🎧 Query: {prompt}")
    res = col.query(query_texts=[prompt], n_results=n_results)

    for i, (doc, meta) in enumerate(zip(res["documents"][0], res["metadatas"][0])):
        print(f"{i+1}. {meta.get('artist')} - {meta.get('track')} ({meta.get('modality')})")

if __name__ == "__main__":
    query_music("energetic rock with female vocals")
