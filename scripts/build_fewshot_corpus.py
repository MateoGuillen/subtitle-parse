import sys, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import DB_CONFIG
import psycopg2

cfg = dict(DB_CONFIG)
if "database" in cfg and "dbname" not in cfg:
    cfg["dbname"] = cfg.pop("database")

FIELD_TITLES = {
    "dias_pago_anticipo": "solicitud de pago de anticipo",
    "dias_solicitud_anticipo": "solicitud de pago de anticipo",
}
YEARS = ["2021", "2022", "2023", "2024", "2025"]

OUT = Path("data/processed/fewshot_retrieval")
OUT.mkdir(parents=True, exist_ok=True)

GT_PATH = Path("data/processed/llm_benchmark/ground_truth_5_campos.csv")

def main():
    import pandas as pd
    gt = pd.read_csv(GT_PATH, encoding="utf-8-sig")
    gt_nros = set(str(int(x)) for x in gt["nro_licitacion"])

    conn = psycopg2.connect(**cfg)
    cur = conn.cursor()

    corpus = {}
    for field, title in FIELD_TITLES.items():
        rows = []
        for year in YEARS:
            tbl = f"dncp.pliegos_secciones_{year}"
            cur.execute(
                f"""SELECT v.nro_licitacion, v.{field}, s.content_text
                    FROM dncp.document_features_v2 v
                    JOIN {tbl} s ON s.nro_licitacion = v.nro_licitacion
                    WHERE v.{field}_conf >= 0.9 AND v.{field} IS NOT NULL
                      AND s.title_normalized = %s
                      AND s.content_text IS NOT NULL""",
                (title,),
            )
            for nro, val, text in cur.fetchall():
                if str(nro) in gt_nros:
                    continue
                rows.append({"nro": str(nro), "value": float(val), "text": text})
        corpus[field] = rows
        print(f"{field}: {len(rows)} corpus rows (value distribution: "
              f"{ {v: sum(1 for r in rows if r['value'] == v) for v in sorted(set(r['value'] for r in rows))} })")

    cur.close()
    conn.close()

    with open(OUT / "corpus.json", "w", encoding="utf-8") as f:
        json.dump(corpus, f, ensure_ascii=False)
    print("saved:", OUT / "corpus.json")

if __name__ == "__main__":
    main()
