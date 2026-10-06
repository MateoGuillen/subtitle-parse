#!/usr/bin/env python3
"""
Regenera CSV con TODAS las features incluidas (field, nro, title, content, value, field_type, scope_corruption, correlation_corruption, anomaly_risk, anomaly_features, preview)
"""
import os
from dotenv import load_dotenv

load_dotenv()


import psycopg2
import pandas as pd
from pathlib import Path

DB_CONFIG = {
    "user": "postgres",
    "password": os.getenv("DB_PASSWORD"),
    "host": "localhost",
    "port": 5433,
    "database": "dncp"
}

DATA_DIR = Path("data/processed/alolab")

ALL_FIELDS = [
    "pct_garantia_cumplimiento",
    "pct_diferencia_precio_bajo",
    "pct_diferencia_precio_alto",
    "num_referencias_tecnicas_minimo",
    "num_referencias_minimo",
    "num_categorias_documentos",
    "anos_experiencia_minima",
    "num_componentes_formula",
    "dias_reposicion",
    "pct_monto_referencia_tecnica",
    "pct_monto_referencia_minimo",
    "num_pasos_verificacion",
    "num_documentos_pago",
    "dias_presentacion_garantia",
    "pct_anticipo",
    "dias_aceptacion_rechazo_factura",
    "dias_pago_anticipo",
    "dias_solicitud_anticipo",
    "anos_experiencia_minima_tecnica",
    "ratio_endeudamiento_maximo",
    "ratio_liquidez_minimo",
    "dias_validez_garantia"
]

ALL_ROWS = {}

# Sample ~10 registros por field
sample_count = []
for i, field_name in enumerate(ALL_FIELDS):
    try:
        print(f"[{i+1}/{len(ALL_FIELDS)}] Field: {field_name}")

        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()

        cursor.execute(f"""
            WITH random_lic AS (
                SELECT nro_licitacion
                FROM dncp.document_features_v2
                WHERE "{field_name}" IS NOT NULL
                ORDER BY RANDOM()
                LIMIT 10
            )
            SELECT r.nro_licitacion, ps.title_normalized, s.content_text, dv."{field_name}"
            FROM random_lic r
            JOIN dncp.pliegos_secciones ps ON r.nro_licitacion = ps.nro_licitacion
            CROSS JOIN LATERAL (
                SELECT content_text
                FROM dncp.pliegos_secciones ps2
                WHERE ps2.nro_licitacion = r.nro_licitacion
                ORDER BY RANDOM()
                LIMIT 1
            ) s(content_text)
            LEFT JOIN dncp.document_features_v2 dv ON r.nro_licitacion = dv.nro_licitacion
        """)

        records = []
        for nro, title, content, val in cursor.fetchall():
            records.append({
                "nro_licitacion": str(nro),
                "title_normalized": title or "",
                "content_text": content or "",
                "value": val
            })

        ALL_ROWS[field_name] = records
        sample_count.append(len(records))

        cursor.close()
        conn.close()
        print(f"  -> {len(records)} samples")

    except Exception as e:
        print(f"  ERROR: {e}")

print(f"\nTotal samples: {sum(sample_count)} across {len(ALL_FIELDS)} fields\n")

# Feature extraction
def detect_field_type(field_name):
    if "pct" in field_name or "ratio" in field_name or "monto" in field_name:
        return "porcentaje"
    elif "dias" in field_name or "plazo" in field_name:
        return "plazo"
    elif ("num" in field_name) and ("dias" not in field_name):
        return "numero"
    elif "anos" in field_name or "experiencia":
        return "anos_experiencia"
    elif "garantia" in field_name:
        return "garantia"
    return "desconocido"

def compute_risk_analysis(field_name, value, content):
    """Computes risk analysis and corruption correlation."""
    risk = {
        "field_type": detect_field_type(field_name),
        "scope_corruption": None,
        "correlation_corruption": None,
        "anomaly_risk": None,
        "anomaly_features": []
    }

    try:
        val = float(value) if value and value != "0" and value != "No Aplica" else None

        if val == 0:
            risk["is_zero"] = True

        if "pct" in field_name or "ratio" in field_name or "monto" in field_name:
            if val is None:
                pass
            elif val < 0.3 or val > 0.95:
                risk["anomaly_risk"] = "out_of_range"
                risk["anomaly_features"].append("out_of_range")

            if val is None or val < 0.5:
                if "pct_diferencia_precio_bajo" in field_name or "pct_diferencia_precio_alto" in field_name:
                    risk["scope_corruption"] = "precio injusto"
                    risk["correlation_corruption"] = "distribuição injusta"

        if "dias" in field_name or "plazo" in field_name:
            if val is None:
                pass
            elif val > 60:
                risk["scope_corruption"] = "retrasos injustificables"
                risk["correlation_corruption"] = "causa plausible de cansancio"

        if "ratio_endeudamiento_maximo" in field_name:
            risk["scope_corruption"] = "patrocinio financiero"
            risk["correlation_corruption"] = "anomalia específica de nepotismo clientelismo"
            risk["anomaly_features"] = ["patrocinio_exceso", "privilegio_cliente"]

        if "ratio_liquidez_minimo" in field_name:
            risk["scope_corruption"] = "menor razón financiera"
            risk["correlation_corruption"] = "clientelismo partidista mayor"
            risk["anomaly_features"] = ["menor_corrupcion", "privilegio_cliente"]

        if "anos_experiencia_minima" in field_name or "anos_experiencia_minima_tecnica" in field_name:
            risk["scope_corruption"] = "nepotismo clientelismo"
            risk["correlation_corruption"] = "experiencia inexplanables más prometidos"

    except:
        pass

    return risk

# Generate final dataset
all_records = []
for field_name, records in ALL_ROWS.items():
    for idx, record in enumerate(records):
        risk = compute_risk_analysis(
            field_name,
            record["value"],
            record["content_text"]
        )

        record["field"] = field_name
        record["field_type_raw"] = risk["field_type"]
        record["scope_corruption"] = risk["scope_corruption"]
        record["correlation_corruption"] = risk["correlation_corruption"]
        record["anomaly_risk"] = risk["anomaly_risk"]
        record["anomaly_features"] = "; ".join(risk["anomaly_features"])
        record["preview"] = (record["content_text"][:200] + "...") if record["content_text"] and len(record["content_text"]) > 200 else record["content_text"]

        all_records.append(record)

df_final = pd.DataFrame(all_records)

# Define columns order
cols = [
    "field",
    "nro_licitacion",
    "title_normalized",
    "content_text",
    "value",
    "field_type_raw",
    "scope_corruption",
    "correlation_corruption",
    "anomaly_risk",
    "anomaly_features",
    "preview"
]

df_final = df_final[cols]

output_file = DATA_DIR / "csv_muestras_pipeline2.csv"
df_final.to_csv(output_file, index=False, encoding="utf-8-sig")

print(f"\n✓ CSV generated: {output_file} ({len(df_final)} rows)")
print(f"  Unique fields: {len(df_final.drop_duplicates('field'))}")
print(f"  Field breakdown:")
for ft, count in df_final["field_type_raw"].value_counts().items():
    print(f"    {ft}: {count} rows ({count/len(df_final)*100:.1f}%)")