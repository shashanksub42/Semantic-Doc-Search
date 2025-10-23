from fastapi import FastAPI
from sentence_transformers import SentenceTransformer
import psycopg2

app = FastAPI()
model = SentenceTransformer("all-MiniLM-L6-v2")
conn = psycopg2.connect('postgresql://neondb_owner:npg_AMtQ8hO1blCf@ep-noisy-frost-afpa7316-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require&options=endpoint%3Dep-noisy-frost-afpa7316')

@app.get('/search')
def search(q: str):
    query_emb = model.encode([q])[0].tolist()
    cur = conn.cursor()
    cur.execute("""
        SELECT content, 1 - (embedding <=> %s::vector) AS score
        FROM wiki_embeddings
        ORDER BY embedding <=> %s::vector
        LIMIT 5;
    """, (query_emb, query_emb))
    results = cur.fetchall()
    cur.close()
    return [{"content": r[0], "score": r[1]} for r in results]


