"""Assign cluster IDs to pliegos_secciones rows for hybrid-field titles.

Uses nearest-centroid assignment in SentenceTransformer embedding space.
Centroids are computed from the per-cluster sample texts in master_schemas.json.
"""
import os
from dotenv import load_dotenv

load_dotenv()


import json
import time
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

import sys
sys.path.insert(0, ".")
from src.etl.transformers.title_mapping import SLUG_TO_TITLE_NORM

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": 5433,
    "dbname": "dncp",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
}
MASTER_SCHEMA_PATH = Path("data/processed/section_clustering/master_schemas.json")
EMBEDDING_MODEL = "paraphrase-multilingual-MiniLM-L12-v2"
BATCH_SIZE = 64


def get_title_mapping(master_data):
    """Map DB (lowercase) titles to master_schema (title-case) keys."""
    schema_titles = set(master_data.keys())
    result = {}
    for slug, db_title in SLUG_TO_TITLE_NORM.items():
        for st in schema_titles:
            if st.lower() == db_title.lower():
                result[db_title] = st
                break
    return result


def compute_centroids(embedder, samples_per_cluster):
    centroids, cluster_ids = [], []
    for cid_str, samples in samples_per_cluster.items():
        texts = [s["text"] for s in samples]
        emb = embedder.encode(texts, show_progress_bar=False, batch_size=BATCH_SIZE)
        centroids.append(emb.mean(axis=0))
        cluster_ids.append(int(cid_str))
    return np.array(centroids), cluster_ids


def ensure_cluster_id_column(conn):
    cur = conn.cursor()
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'dncp' AND table_name = 'pliegos_secciones'
        AND column_name = 'cluster_id'
    """)
    if not cur.fetchone():
        print("Adding cluster_id column to dncp.pliegos_secciones...")
        cur.execute("ALTER TABLE dncp.pliegos_secciones ADD COLUMN cluster_id INTEGER;")
        conn.commit()
        print("Added.")
    cur.close()


def update_batch(cur, assignments, nros, db_title):
    values = [(str(n), int(a)) for n, a in zip(nros, assignments)]
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS tmp_assign (nro_licitacion VARCHAR, cluster_id INTEGER) ON COMMIT DROP")
    cur.execute("DELETE FROM tmp_assign")
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO tmp_assign (nro_licitacion, cluster_id) VALUES %s",
        values,
        template="(%s, %s)",
    )
    cur.execute("""
        UPDATE dncp.pliegos_secciones ps SET cluster_id = t.cluster_id
        FROM tmp_assign t
        WHERE ps.nro_licitacion = t.nro_licitacion
        AND LOWER(ps.title_normalized) = LOWER(%s)
    """, (db_title,))


def main():
    embedder = SentenceTransformer(EMBEDDING_MODEL)
    master_data = json.loads(MASTER_SCHEMA_PATH.read_text())
    title_mapping = get_title_mapping(master_data)
    conn = psycopg2.connect(**DB_CONFIG)
    ensure_cluster_id_column(conn)

    for db_title, schema_title in sorted(title_mapping.items()):
        print(f"\n{'=' * 60}")
        print(f"Processing: {schema_title}")
        t0 = time.time()

        title_data = master_data.get(schema_title)
        if title_data is None:
            print(f"  WARNING: {schema_title} not found, skipping")
            continue

        spc = title_data.get("samples_per_cluster")
        if not spc:
            print(f"  WARNING: no samples_per_cluster for {schema_title}, skipping")
            continue

        centroids, cluster_ids = compute_centroids(embedder, spc)
        print(f"  Centroids: {len(cluster_ids)} clusters")

        df = pd.read_sql_query(
            "SELECT nro_licitacion, content_text FROM dncp.pliegos_secciones "
            "WHERE LOWER(title_normalized) = LOWER(%s)",
            conn,
            params=(db_title,),
        )
        print(f"  Rows: {len(df)}")
        if len(df) == 0:
            continue

        texts = df["content_text"].tolist()
        print(f"  Embedding {len(texts)} texts...")
        t1 = time.time()
        embeddings = embedder.encode(texts, batch_size=BATCH_SIZE, show_progress_bar=True)
        print(f"  Embedded in {time.time() - t1:.1f}s, shape={embeddings.shape}")

        sim = cosine_similarity(embeddings, centroids)
        assignments = [cluster_ids[i] for i in np.argmax(sim, axis=1)]
        nros = df["nro_licitacion"].tolist()

        cur = conn.cursor()
        for offset in range(0, len(nros), 5000):
            end = offset + 5000
            update_batch(cur, assignments[offset:end], nros[offset:end], db_title)
            conn.commit()
            print(f"    Updated {end}/{len(nros)}", end="\r")
        print()
        print(f"  Done in {time.time() - t0:.1f}s")
        cur.close()

    with conn.cursor() as cur:
        cur.execute("ANALYZE dncp.pliegos_secciones;")
    conn.commit()
    conn.close()
    print(f"\n{'=' * 60}")
    print("All titles processed. Cluster IDs assigned.")


if __name__ == "__main__":
    main()
