from sentence_transformers import SentenceTransformer
import psycopg2
from prefect import flow, task
import requests

# ------------------------------
#  Define your Prefect tasks
# ------------------------------

@task
def fetch_wikipedia_articles(titles):
    """Fetch article content from Wikipedia API."""
    base_url = "https://en.wikipedia.org/api/rest_v1/page/summary/"
    articles = []
    for title in titles:
        res = requests.get(base_url + title)
        if res.status_code == 200:
            data = res.json()
            articles.append({"title": data["title"], "content": data["extract"]})
    return articles


@task
def generate_embeddings(articles):
    """Generate sentence embeddings."""
    model = SentenceTransformer("all-MiniLM-L6-v2")
    for a in articles:
        a["embedding"] = model.encode(a["content"]).tolist()
    return articles


@task
def store_in_neon(articles):
    """Upsert embeddings into Neon (Postgres + pgvector)."""
    conn = psycopg2.connect("postgresql://neondb_owner:npg_AMtQ8hO1blCf@ep-noisy-frost-afpa7316-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require&options=endpoint%3Dep-noisy-frost-afpa7316")
    cur = conn.cursor()
    for a in articles:
        cur.execute("""
            INSERT INTO wiki_embeddings (title, content, embedding)
            VALUES (%s, %s, %s)
            ON CONFLICT (title) DO UPDATE
            SET content = EXCLUDED.content,
                embedding = EXCLUDED.embedding;
        """, (a["title"], a["content"], a["embedding"]))
    conn.commit()
    cur.close()
    conn.close()

# ------------------------------
#  Define your Prefect flow
# ------------------------------

@flow(name="Wikipedia Embedding Update")
def update_wiki_embeddings():
    titles = ["Medieval England", "William the Conqueror", "Harold Godwinson"]
    articles = fetch_wikipedia_articles(titles)
    embedded = generate_embeddings(articles)
    store_in_neon(embedded)

if __name__ == "__main__":
    update_wiki_embeddings()
