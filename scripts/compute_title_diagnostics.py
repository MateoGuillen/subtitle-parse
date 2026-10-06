"""
Fase 1 — Diagnóstico de boilerplate y contenido numérico por título.

Para cada `title_normalized` en `dncp.pliegos_secciones` calcula:
    - md5_top1_share:           % de secciones cuyo hash MD5(normalized_text) = hash más frecuente
    - md5_n_distinct:           # de hashes distintos (proxy de variante textual)
    - n_docs_present:           # de licitaciones distintas que contienen el título
    - total_sections:           # total de secciones
    - mean_word_count, std_word_count, cov_word_count: dispersión de longitud
    - numeric_pattern_hits:     # promedio de matches por sección (regex sobre monto/% , días, fechas)
    - numeric_pattern_rate:     % de secciones con al menos un match numérico
    - has_numeric_content:      True si numeric_pattern_rate > 0.05

Output:
    data/processed/title_diagnostics/title_diagnostics.csv
    data/processed/title_diagnostics/title_diagnostics_report.md

No toca el pipeline existente; es una pasada de solo lectura.
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
from pathlib import Path
from typing import Dict, Iterator, List, Tuple

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import DB_CONFIG

OUTPUT_DIR = Path("data/processed/title_diagnostics")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Regex de patrones numéricos paramétricos
# ---------------------------------------------------------------------------
# Montos en Guaraníes (Gs. o guaraníes, con separadores de miles)
RE_AMOUNT = re.compile(
    r"(?:Gs\.?\s*|guaran[ií]es(?:\s*\w*)?\s*)"
    r"\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?",
    re.IGNORECASE,
)
# Porcentajes: "5%", "5,5 %", "porcentaje del 10"
RE_PCT = re.compile(
    r"\b\d{1,2}(?:[.,]\d{1,2})?\s*%"
)
# Días / plazos: "30 días", "plazo de 60 (días) hábiles"
RE_DAYS = re.compile(
    r"\b\d{1,4}\s*(?:d[ií]as?|h[iá]bil\w*|calendario\w*|meses?|semanas?)\b",
    re.IGNORECASE,
)
# Fechas DD/MM/YYYY o YYYY-MM-DD
RE_DATE = re.compile(
    r"\b\d{1,2}[/-]\d{1,2}(?:[/-]\d{2,4})?\b"
    r"|\b\d{4}-\d{2}-\d{2}\b"
)
# Anticipos / multas keyword + número cercano
RE_SPECIAL = re.compile(
    r"(?:anticipo|multa|penalidad|garant[ií]a).*?"
    r"\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?|\d{1,2}(?:[.,]\d{1,2})?\s*%",
    re.IGNORECASE,
)

NUMERIC_PATTERNS = [RE_AMOUNT, RE_PCT, RE_DAYS, RE_DATE, RE_SPECIAL]


def normalize_text(text: str) -> str:
    """Whitespace-canonical form for MD5 hashing: collapse runs to single space, strip."""
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def md5_hash(norm: str) -> str:
    return hashlib.md5(norm.encode("utf-8", errors="ignore")).hexdigest()


def count_numeric_patterns(text: str) -> int:
    if not text:
        return 0
    return sum(len(p.findall(text)) for p in NUMERIC_PATTERNS)


# ---------------------------------------------------------------------------
# Pasada 1: SQL directo para hashes + word_count stats
# ---------------------------------------------------------------------------
SQL_HASH_STATS = """
SELECT
    title_normalized,
    COUNT(*)                                   AS total_sections,
    COUNT(DISTINCT nro_licitacion)             AS n_docs_present,
    AVG(word_count)::float8                    AS mean_word_count,
    STDDEV(word_count)::float8                 AS std_word_count,
    AVG(content_length)::float8                AS mean_content_length,
    STDDEV(content_length)::float8             AS std_content_length
FROM dncp.pliegos_secciones
WHERE title_normalized IS NOT NULL
  AND word_count IS NOT NULL
  AND word_count > 5
GROUP BY title_normalized
ORDER BY total_sections DESC
"""

# Para cada título, traemos texto normalizado en chunks para construir hashes.
# Traer el contenido completo haría OOM — mejor agrupar en SQL con MD5 sobre texto normalizado.
# PostgreSQL no tiene REGEXP_REPLACE con \s+ nativo eficiente; usamos su replace recursivo.
# Approximación: normalizo en Python (es barato una vez que el texto está en memoria).
SQL_CHUNK = """
SELECT
    title_normalized,
    content_text
FROM dncp.pliegos_secciones
WHERE title_normalized = ANY(%s)
  AND word_count > 5
ORDER BY ctid
LIMIT %s OFFSET %s
"""


def fetch_hash_stats(conn) -> pd.DataFrame:
    print(">> Pasada 1: estadísticas por título (SQL directo)...")
    with conn.cursor() as cur:
        cur.execute(SQL_HASH_STATS)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
    df = pd.DataFrame(rows, columns=cols)
    df["cov_word_count"] = (
        df["std_word_count"] / df["mean_word_count"].replace(0, pd.NA)
    ).fillna(0.0)
    print(f"   {len(df)} títulos")
    return df


# ---------------------------------------------------------------------------
# Pasada 2: hash MD5 de texto normalizado, en chunks por title_normalized
# ---------------------------------------------------------------------------
def fetch_hashes_for_titles(conn, titles: List[str], chunk_size: int = 50_000) -> Dict[str, Dict[str, int]]:
    """
    Para cada title_normalized, cuenta hashes MD5(normalized_text).
    Valor: para títulos muy frecuentes (32182 secciones), hay que traer en chunks.
    """
    print(f">> Pasada 2: MD5 hashes sobre {len(titles)} títulos...")
    result: Dict[str, Dict[str, int]] = {t: {} for t in titles}

    # En vez de paginar por ctid (que es frágil tras updates), usamos un cursor server-side.
    with conn.cursor(name="hash_cursor") as cur:  # server-side cursor
        cur.itersize = 5000
        cur.execute(
            """
            SELECT title_normalized, content_text
            FROM dncp.pliegos_secciones
            WHERE title_normalized = ANY(%s)
              AND word_count > 5
            """,
            (list(titles),),
        )
        processed = 0
        for title, text in cur:
            if title not in result:
                continue
            h = md5_hash(normalize_text(text))
            result[title][h] = result[title].get(h, 0) + 1
            processed += 1
            if processed % 200_000 == 0:
                print(f"   ... {processed:,} secciones procesadas")
    print(f"   {processed:,} secciones procesadas total")
    return result


# ---------------------------------------------------------------------------
# Pasada 3: regex numéricos sobre muestra (100 secciones por título)
# ---------------------------------------------------------------------------
SQL_SAMPLE_FOR_NUMERIC = """
SELECT content_text
FROM dncp.pliegos_secciones
WHERE title_normalized = %s
  AND word_count > 5
ORDER BY random()
LIMIT %s
"""


def fetch_numeric_samples(conn, titles: List[str], sample_size: int = 100) -> pd.DataFrame:
    """
    Muestra aleatoria por título para estimar patrón numérico (sin cargar todo el corpus).
    """
    print(f">> Pasada 3: regex numérico sobre muestra de {sample_size} secciones por título...")
    rows: List[Tuple[str, float, float]] = []
    for i, title in enumerate(titles):
        with conn.cursor() as cur:
            cur.execute(SQL_SAMPLE_FOR_NUMERIC, (title, sample_size))
            texts = [r[0] or "" for r in cur.fetchall()]
        n = len(texts)
        if n == 0:
            rows.append((title, 0.0, 0.0))
            continue
        hits_per_section = [count_numeric_patterns(t) for t in texts]
        mean_hits = sum(hits_per_section) / n
        rate_nonzero = sum(1 for h in hits_per_section if h > 0) / n
        rows.append((title, mean_hits, rate_nonzero))
        if (i + 1) % 20 == 0:
            print(f"   {i+1}/{len(titles)} títulos")
    df = pd.DataFrame(rows, columns=["title_normalized", "numeric_pattern_hits", "numeric_pattern_rate"])
    df["has_numeric_content"] = df["numeric_pattern_rate"] > 0.05
    return df


# ---------------------------------------------------------------------------
# Pasada 4: post-merge top1 share (SequenceMatcher 0.95) opcional
# ---------------------------------------------------------------------------
def compute_post_merge_share_from_json(titles: List[str]) -> Dict[str, float]:
    """
    Lee `data/processed/section_clustering/master_schemas.json` y recupera
    post_merge_top1_share para los títulos presentes. Para ausentes, NaN.

    Lógica: las llaves de cluster_values[field] son strings como "0_2_4_6..."
    Cada token es un cluster id original. cluster_counts dict tiene originales.
    """
    path = Path("data/processed/section_clustering/master_schemas.json")
    if not path.exists():
        print(">> Pasada 4: master_schemas.json no encontrado — saltando post_merge_share")
        return {t: float("nan") for t in titles}
    print(">> Pasada 4: recompute post_merge_top1_share desde master_schemas.json...")
    import json
    with path.open("r", encoding="utf-8") as f:
        schemas = json.load(f)
    result: Dict[str, float] = {}
    # Normalizar keys
    schemas_norm = {k.strip().lower(): v for k, v in schemas.items()}
    for title in titles:
        key = title.strip().lower()
        data = schemas_norm.get(key)
        if not data:
            result[title] = float("nan")
            continue
        cluster_counts = data.get("cluster_counts", {})
        cluster_values = data.get("cluster_values", {})
        if not cluster_counts or not cluster_values:
            result[title] = float("nan")
            continue
        # tomar cualquier campo de cluster_values -> merged cluster ids como keys
        first_field = next(iter(cluster_values.values()))
        merged_ids = list(first_field.keys())  # tipo ["0_2_4_6...", "1_12_18", ...]
        # para cada merged_id, sumar counts de originales
        total = sum(cluster_counts.values()) or 1
        max_merged = 0
        for mid in merged_ids:
            ids = [int(x) for x in mid.split("_") if x.isdigit()]
            size = sum(cluster_counts.get(str(i), 0) for i in ids)
            if size > max_merged:
                max_merged = size
        result[title] = max_merged / total
    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-titles-csv", default="data/processed/title_ranking/title_ranking.csv",
        help="CSV con title_slug; se mapean a DB titles (default: ranking v1)",
    )
    parser.add_argument("--numeric-sample", type=int, default=100,
                        help="Muestras por título para regex numérico")
    parser.add_argument("--skip-numeric", action="store_true",
                        help="Saltea el computo de patrones numericos")
    parser.add_argument("--skip-post-merge", action="store_true",
                        help="Saltea post_merge_share desde master_schemas_json")
    parser.add_argument("--limit-titles", type=int, default=None,
                        help="Limitar a los N titulos mas frecuentes (para debug)")
    args = parser.parse_args()

    conn = psycopg2.connect(
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["database"],
    )

    # ---- 1) Hash stats + word_count dispersion por title_normalized ----
    df_stats = fetch_hash_stats(conn)
    if args.limit_titles:
        df_stats = df_stats.head(args.limit_titles)
    titles = df_stats["title_normalized"].tolist()

    # ---- 2) MD5 hashes por título ----
    hash_counts = fetch_hashes_for_titles(conn, titles)
    rows = []
    for t in titles:
        hc = hash_counts.get(t, {})
        total = sum(hc.values()) or 1
        distinct = len(hc)
        top1 = max(hc.values()) if hc else 0
        rows.append({
            "title_normalized": t,
            "total_sections_hashed": sum(hc.values()),
            "md5_n_distinct": distinct,
            "md5_top1_share": top1 / total,
        })
    df_hashes = pd.DataFrame(rows)

    # merge
    df = df_stats.merge(df_hashes, on="title_normalized", how="left")

    # ---- 3) Patrones numéricos sobre muestra ----
    if not args.skip_numeric:
        df_num = fetch_numeric_samples(conn, titles, sample_size=args.numeric_sample)
        df = df.merge(df_num, on="title_normalized", how="left")

    # ---- 4) post_merge_share solo para los 10 actuales ----
    if not args.skip_post_merge:
        pm_share = compute_post_merge_share_from_json(titles)
        df["post_merge_top1_share"] = df["title_normalized"].map(pm_share)
    else:
        df["post_merge_top1_share"] = float("nan")

    # ---- Criterio Filtro C: descartar si boilerplate > 0.90 sin numéricos ----
    def is_descartado(row):
        bp_threshold = 0.90
        bp_measure = max(
            row.get("md5_top1_share", 0.0) or 0.0,
            row.get("post_merge_top1_share", 0.0) or 0.0
            if not pd.isna(row.get("post_merge_top1_share", float("nan")))
            else 0.0,
        )
        has_num = row.get("has_numeric_content", False)
        if pd.isna(has_num):
            has_num = False
        return bool(bp_measure > bp_threshold and not bool(has_num))
    df["descartado_boilerplate"] = df.apply(is_descartado, axis=1)

    # ---- Ordenar y guardar ----
    df = df.sort_values("total_sections", ascending=False).reset_index(drop=True)
    csv_path = OUTPUT_DIR / "title_diagnostics.csv"
    df.to_csv(csv_path, index=False, float_format="%.4f")
    print(f"\n[OK] CSV: {csv_path}  ({len(df)} titles)")

    # ---- Reporte markdown ----
    write_report(df, OUTPUT_DIR / "title_diagnostics_report.md")

    conn.close()


def write_report(df: pd.DataFrame, path: Path):
    """Genera un .md legible humano con top-20 + descartados + top parametricos."""
    lines: List[str] = []
    lines.append("# Diagnóstico de títulos — Boilerplate & Contenido Numérico\n")
    lines.append(f"Fecha: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')}\n")
    lines.append(f"Total títulos analizados: {len(df)}\n")
    lines.append(f"Descartados (boilerplate > 90% sin numérico): {df['descartado_boilerplate'].sum()}\n")
    lines.append(f"Con contenido numérico (rate > 5%): {df['has_numeric_content'].sum() if 'has_numeric_content' in df else 'N/A'}\n\n")

    # Top 20 por aparición
    top20 = df.head(20)
    lines.append("## Top 20 títulos por # secciones\n")
    lines.append("| # | title | total_sec | n_docs | md5_top1 | cov_wc | num_hits | has_num | descart |")
    lines.append("|---|-------|----------:|------:|--------:|-------:|---------:|:------:|:-------:|")
    for i, r in top20.iterrows():
        lines.append(
            f"| {i+1} | {r['title_normalized'][:50]} | {int(r['total_sections'])} | "
            f"{int(r['n_docs_present'])} | {r['md5_top1_share']:.3f} | "
            f"{r['cov_word_count']:.3f} | "
            f"{r.get('numeric_pattern_hits', 0):.2f} | "
            f"{'✓' if r.get('has_numeric_content', False) else '✗'} | "
            f"{'❌' if r['descartado_boilerplate'] else ''} |"
        )

    # Títulos descartados
    desc = df[df["descartado_boilerplate"]]
    if len(desc) > 0:
        lines.append("\n## Descartados por Filtro C (boilerplate >90% sin numéricos)\n")
        for _, r in desc.iterrows():
            lines.append(f"- `{r['title_normalized']}` — md5_top1={r['md5_top1_share']:.3f}, post_merge={r.get('post_merge_top1_share', float('nan')):.3f}, num_hits={r.get('numeric_pattern_hits', 0):.2f}")

    # Top-15 con alta varianza + contenido numérico (candidatos parametricos)
    if "numeric_pattern_hits" in df.columns:
        par = df[~df["descartado_boilerplate"] & df["has_numeric_content"]].copy()
        par = par.sort_values("numeric_pattern_hits", ascending=False).head(15)
        lines.append("\n## Top 15 candidatos paramétricos (Fase 2 semilla)\n")
        lines.append("Ordenado por numeric_pattern_hits.\n")
        lines.append("| title | num_hits | num_rate | cov_wc | md5_top1 | total_sec |")
        lines.append("|-------|--------:|--------:|------:|--------:|----------:|")
        for _, r in par.iterrows():
            lines.append(
                f"| {r['title_normalized'][:60]} | {r['numeric_pattern_hits']:.2f} | "
                f"{r['numeric_pattern_rate']:.3f} | {r['cov_word_count']:.3f} | "
                f"{r['md5_top1_share']:.3f} | {int(r['total_sections'])} |"
            )

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] Report: {path}")


if __name__ == "__main__":
    main()
