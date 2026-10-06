"""Build few-shot example database for cluster-aware LLM prompting.

For each hybrid field, queries document_features_v2 rows where conf=1.0,
joins with pliegos_secciones to get content_text + cluster_id,
and exports a JSON mapping: {title_slug: {cluster_id: {field: [examples]}}}.
"""
import os
from dotenv import load_dotenv

load_dotenv()


import json
from pathlib import Path
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": 5433,
    "dbname": "dncp",
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
}

SCHEMAS_PATH = Path("data/processed/section_clustering_v2/master_schemas_v2.json")
OUTPUT_PATH = Path("data/processed/section_clustering_v2/few_shot_db.json")
MAX_EXAMPLES_PER_GROUP = 3


def load_schemas():
    with open(SCHEMAS_PATH) as f:
        data = json.load(f)
    title_schemas = {k: v for k, v in data.items() if k != "meta"}
    return title_schemas


def build_few_shot_db():
    title_schemas = load_schemas()
    conn = psycopg2.connect(**DB_CONFIG)

    # {title_slug: {cluster_id_str: {field_name: [example_dict]}}}
    few_shot_db: Dict[str, Dict[str, Dict[str, List[dict]]]] = {}

    def serialize_val(v):
        from decimal import Decimal
        if isinstance(v, Decimal):
            return float(v)
        return v

    for slug, schema in title_schemas.items():
        hybrid = [f for f in schema["fields"] if f.get("extraction_method") == "hybrid"]
        if not hybrid:
            continue

        title_name = slug.replace("_", " ")
        print(f"\nProcessing: {slug}")

        for field in hybrid:
            field_name = field["name"]
            conf_col = f"{field_name}_conf"
            val_col = field_name

            sql = f"""
                SELECT ps.content_text, ps.cluster_id, fv2.{val_col} AS val
                FROM dncp.document_features_v2 fv2
                JOIN dncp.pliegos_secciones ps
                  ON fv2.nro_licitacion = ps.nro_licitacion
                 AND ps.title_normalized = %s
                WHERE fv2.{conf_col} = 1.0
                  AND fv2.{val_col} IS NOT NULL
                  AND ps.cluster_id IS NOT NULL
                ORDER BY ps.cluster_id, fv2.nro_licitacion
            """
            with conn.cursor() as cur:
                cur.execute(sql, (title_name,))
                rows = cur.fetchall()

            if not rows:
                print(f"  {field_name}: 0 examples")
                continue

            # Group by cluster_id
            cluster_groups: Dict[str, List[dict]] = {}
            for text, cid, val in rows:
                cid_str = str(cid)
                if cid_str not in cluster_groups:
                    cluster_groups[cid_str] = []
                if len(cluster_groups[cid_str]) < MAX_EXAMPLES_PER_GROUP:
                    cluster_groups[cid_str].append({
                        "text": text[:2000],
                        "value": serialize_val(val),
                    })

            total = sum(len(v) for v in cluster_groups.values())
            print(f"  {field_name}: {total} examples across {len(cluster_groups)} clusters")

            # Store in few_shot_db
            if slug not in few_shot_db:
                few_shot_db[slug] = {}
            for cid_str, examples in cluster_groups.items():
                if cid_str not in few_shot_db[slug]:
                    few_shot_db[slug][cid_str] = {}
                few_shot_db[slug][cid_str][field_name] = examples

    conn.close()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    class DecimalEncoder(json.JSONEncoder):
        def default(self, obj):
            from decimal import Decimal
            if isinstance(obj, Decimal):
                return float(obj)
            return super().default(obj)

    OUTPUT_PATH.write_text(
        json.dumps(few_shot_db, ensure_ascii=False, indent=2, cls=DecimalEncoder),
        encoding="utf-8",
    )
    total_examples = sum(
        len(ex)
        for slug_data in few_shot_db.values()
        for cluster_data in slug_data.values()
        for ex_list in cluster_data.values()
        for ex in ex_list
    )
    print(f"\n{'=' * 60}")
    print(f"Few-shot DB saved to {OUTPUT_PATH}")
    print(f"Total examples: {total_examples}")
    print(f"Total (slug, cluster, field) combos: {sum(len(c) for s in few_shot_db.values() for c in s.values())}")


if __name__ == "__main__":
    build_few_shot_db()
