
# Music Recommendation Pipeline (P1)

End-to-end DataOps-style pipeline that ingests a Kaggle `tracks.csv`, enriches via the Spotify API, downloads album covers, validates/cleans data, and indexes a multimodal collection in ChromaDB for simple text/genre/artist queries.

## Quickstart
1) `pip install -r requirements.txt`
2) `copy .env.example .env` (Windows) or `cp .env.example .env` (macOS/Linux)
3) Put your Kaggle CSV at `data/music/raw/tracks.csv`
4) Start MinIO: `docker compose up -d` (console at http://localhost:9001)
5) Run: `python run_music_pipeline.py`
6) Query:
```python
import chromadb
client = chromadb.PersistentClient(path="chroma_db")
col = client.get_or_create_collection("music_multimodal")
res = col.query(query_texts=["indie rock female vocals"], n_results=5)
for i,(doc,meta) in enumerate(zip(res['documents'][0], res['metadatas'][0])):
    print(i+1, meta.get('artist'), "-", meta.get('track'), "|", meta.get('modality'))
```

Artifacts per zone are saved in MinIO (`landing_zone`, `formatted_zone`, `trusted_zone`) and locally (`chroma_db/`, `reports/`).

