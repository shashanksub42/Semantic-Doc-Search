from wikipedia import page
from langchain.text_splitter import RecursiveCharacterTextSplitter
from sentence_transformers import SentenceTransformer
import psycopg2

doc = page("Battle of Hastings", auto_suggest=False)
text = doc.content
print(doc.title)

splitter = RecursiveCharacterTextSplitter(chunk_size=500, chunk_overlap=50)
chunks = splitter.split_text(text)
print(len(chunks))

model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = model.encode(chunks)
print('Embedding done')

conn = psycopg2.connect('postgresql://neondb_owner:************@ep-noisy-frost-afpa7316-pooler.c-2.us-west-2.aws.neon.tech/neondb?sslmode=require&options=endpoint%3Dep-noisy-frost-afpa7316')
cur = conn.cursor()

for text, emb in zip(chunks, embeddings):
    cur.execute(
        "INSERT INTO wiki_embeddings (title, content, embedding) VALUES (%s, %s, %s)",
        ("Battle of Hastings", text, emb.tolist()),
    )

conn.commit()
cur.close()
conn.close()

