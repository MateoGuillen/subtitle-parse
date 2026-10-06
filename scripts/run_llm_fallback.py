"""Runner del fallback LLM con fingerprint + caché + checkpoints (diseño: docs/piloto_llm_fallback.md).

- Población: filas document_features_v2 con {campo}_conf < threshold (extractor existente).
- Fingerprint al arrancar (run_fingerprint.py): distinto fingerprint = nuevo run_id obligatorio.
- Caché content-addressed (text_sha256, model, prompt, field): hit solo si coinciden los 4.
- Checkpoints por (doc, campo) en dncp.llm_fallback_runs con commit por lote;
  relanzar con el mismo run_id retoma pendientes (fingerprint verificado, si no aborta).
- Staging siempre; UPSERT a producción solo con --promote (regla no-degradar).

Ejemplos:
  Pilot 1500 docs, sin tocar producción:
    /root/vllm-env/bin/python scripts/run_llm_fallback.py --pilot-docs 1500
  Reanudar:  ... --run-id llmfb-meta-llama-3.1-8b-instruct-fp8-<fp>
  Full + promote:
    /root/vllm-env/bin/python scripts/run_llm_fallback.py --promote
"""
import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import pandas as pd
import psycopg2

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config.settings import DB_CONFIG
from src.etl.extractors.document_features_v2_llm_extractor import (
    DocumentFeaturesV2LLMExtractor)
from src.etl.transformers.document_features_v2_llm_transformer import (
    DocumentFeaturesV2LLMTransformer)
from src.etl.transformers.llm_provider import VLLMOfflineProvider
from src.etl.transformers.run_fingerprint import compute_fingerprint
from src.etl.loaders.document_features_v2_llm_loader import (
    DocumentFeaturesV2LLMLoader)

GUIDED_REGEX = r'^(-?\d+(?:[.,]\d+)?|NULL)$'
MODELS = {
    "llama-fp8": dict(
        model_id="RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8",
        quant="none", gpu_mem=0.70, max_len=8192, max_seqs=32),
    "llama-awq": dict(
        model_id="thesven/Meta-Llama-3.1-8B-Instruct-AWQ",
        quant=None, gpu_mem=0.70, max_len=8192, max_seqs=32),
}
RUNS_DIR = Path(__file__).resolve().parents[1] / "data" / "processed" / "llm_runs"


def db_conn():
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False
    return conn


def text_sha(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


INT_PREFIXES = ("dias_", "anos_", "num_")


def sanitize_value(field: str, val):
    """0 en dias/anos/num, y <=0 en ratio_ = ausencia -> NULL/conf 0.0.
    Un plazo/cantidad/ratio de 0 no existe como requisito; el modelo (o un
    determinista con bug) lo emite ante plantillas vacías. pct_ conserva el 0
    (0% sí es legítimo)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, 0.0
    v = float(val)
    if v == 0.0 and field.startswith(INT_PREFIXES):
        return None, 0.0
    if v <= 0.0 and field.startswith("ratio_"):
        return None, 0.0
    return v, 1.0


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pilot-docs", type=int, default=0,
                    help="0 = corrida completa; N = piloto con N docs distintos")
    ap.add_argument("--fields", default="",
                    help="coma-separados; vacío = todos los hybrid con fallo")
    ap.add_argument("--model", default="llama-fp8", choices=list(MODELS))
    ap.add_argument("--run-id", default="")
    ap.add_argument("--promote", action="store_true",
                    help="upsert con guardia a document_features_v2 al cerrar")
    ap.add_argument("--batch-commit", type=int, default=500)
    ap.add_argument("--conf-threshold", type=float, default=0.8)
    args = ap.parse_args()

    schemas_path = str(Path(__file__).resolve().parents[1]
                       / "data" / "processed" / "section_clustering_v2"
                       / "master_schemas_v2.json")
    system_prompt = DocumentFeaturesV2LLMTransformer._build_system_prompt()
    sampling = {"temperature": 0.0, "max_tokens": 256,
                "guided_regex": GUIDED_REGEX, "max_num_seqs": 32}
    mcfg = MODELS[args.model]
    fp = compute_fingerprint(mcfg["model_id"], mcfg["quant"], system_prompt,
                             schemas_path, sampling, args.conf_threshold)
    RUNS_DIR.mkdir(parents=True, exist_ok=True)

    if args.run_id:
        cfg_path = RUNS_DIR / f"{args.run_id}.json"
        if not cfg_path.exists():
            raise SystemExit(f"run-id desconocido (sin config): {args.run_id}")
        prev = json.loads(cfg_path.read_text(encoding="utf-8"))
        if prev["fingerprint"] != fp["fingerprint"]:
            raise SystemExit(
                "FINGERPRINT DIFIERE del run original: rehúso mezclar "
                "configuraciones. Iniciá un run nuevo (sin --run-id).")
        fp = prev
        print(f"[resume] {fp['run_id']} (fingerprint verificado)")
    else:
        (RUNS_DIR / f"{fp['run_id']}.json").write_text(
            json.dumps(fp, indent=1, ensure_ascii=False), encoding="utf-8")
        print(f"[nuevo run] {fp['run_id']}")
    print(json.dumps({k: v for k, v in fp.items() if k != "run_id"}, indent=1))

    conn = db_conn()
    cur = conn.cursor()

    # ---- 1. Población (extractor existente). En resume NO se re-expande:
    # solo se trabaja lo ya stageado para ese run_id.
    text_of: dict = {}
    if not args.run_id:
        extractor = DocumentFeaturesV2LLMExtractor(
            _db_params(), schemas_path, args.conf_threshold)
        failed = extractor.get_failed_extractions_grouped()
        if args.fields:
            want = {f.strip() for f in args.fields.split(",")}
            failed = {k: v for k, v in failed.items() if k in want}
        rows = []
        for field, df in failed.items():
            for _, r in df.iterrows():
                txt = r.get("content_text") or ""
                if txt.strip():
                    rows.append((str(r["nro_licitacion"]), field, txt,
                                 r.get("cluster_id")))
        print(f"población con texto: {len(rows)} filas-campo")
        if args.pilot_docs:
            import random
            docs = sorted({n for n, _, _, _ in rows})
            random.Random(42).shuffle(docs)
            keep = set(docs[:args.pilot_docs])
            rows = [x for x in rows if x[0] in keep]
            print(f"piloto: {len(keep)} docs -> {len(rows)} filas-campo")

        # ---- 2. Alta de pendientes en staging (idempotente) ----
        for nro, field, txt, _ in rows:
            cur.execute(
                """INSERT INTO dncp.llm_fallback_runs
                   (run_id, nro_licitacion, field, model_id, prompt_hash,
                    schema_version, code_version, status)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,'pending')
                   ON CONFLICT (run_id, nro_licitacion, field) DO NOTHING""",
                (fp["run_id"], nro, field, fp["model_id"], fp["prompt_hash"],
                 fp["schema_version"], fp["code_version"]))
            text_of[(nro, field)] = txt
        conn.commit()
    else:
        # Resume: recuperar textos para las pendientes desde la BD (secciones).
        cur.execute("SELECT DISTINCT nro_licitacion, field FROM "
                    "dncp.llm_fallback_runs WHERE run_id=%s AND status='pending'",
                    (fp["run_id"],))
        need = cur.fetchall()
        by_nro: dict = {}
        for nro, field in need:
            by_nro.setdefault(nro, []).append(field)
        title_of = _title_for_fields(schemas_path)
        for nro, fields in by_nro.items():
            for field in fields:
                cur.execute("SELECT content_text FROM dncp.pliegos_secciones "
                            "WHERE nro_licitacion=%s AND title_normalized=%s LIMIT 1",
                            (nro, title_of.get(field, "")))
                t = cur.fetchone()
                if t and t[0] and t[0].strip():
                    text_of[(nro, field)] = t[0]
    cur.execute("SELECT count(*) FROM dncp.llm_fallback_runs "
                "WHERE run_id=%s AND status='pending'", (fp["run_id"],))
    n_pend = cur.fetchone()[0]
    print(f"pendientes en staging: {n_pend}")
    # Reintentos: failed con attempts<3 vuelven a pending (diseño §4)
    cur.execute("UPDATE dncp.llm_fallback_runs SET status='pending' "
                "WHERE run_id=%s AND status='failed' AND attempts<3",
                (fp["run_id"],))
    if cur.rowcount:
        print(f"reintentos reencolados: {cur.rowcount}")
        conn.commit()

    # ---- 3. Caché: resolver sin GPU lo que ya existe ----
    cur.execute("SELECT nro_licitacion, field FROM dncp.llm_fallback_runs "
                "WHERE run_id=%s AND status='pending'", (fp["run_id"],))
    pending = cur.fetchall()
    by_field: dict = {}
    for nro, field in pending:
        by_field.setdefault(field, []).append(nro)
    todo_by_field: dict = {}
    n_hit = 0
    for field, nros in by_field.items():
        todo = []
        for nro in nros:
            txt = text_of.get((nro, field), "")
            cur.execute("SELECT value, raw_short FROM dncp.llm_fallback_cache "
                        "WHERE text_sha256=%s AND model_id=%s AND prompt_hash=%s "
                        "AND field=%s",
                        (text_sha(txt), fp["model_id"], fp["prompt_hash"], field))
            hit = cur.fetchone()
            if hit:
                n_hit += 1
                hv, hc = sanitize_value(field, hit[0])
                cur.execute("UPDATE dncp.llm_fallback_runs SET value=%s, "
                            "conf_new=%s, raw_short=%s, status='done', "
                            "attempts=attempts+1 WHERE run_id=%s AND "
                            "nro_licitacion=%s AND field=%s",
                            (hv, hc, (hit[1] or "")[:120] + " [cache]",
                             fp["run_id"], nro, field))
            else:
                todo.append((nro, txt))
        if todo:
            todo_by_field[field] = todo
    conn.commit()
    print(f"cache hits: {n_hit} | a GPU: {sum(len(v) for v in todo_by_field.values())}")

    # ---- 4. GPU: correr pendientes por campo (transformer existente) ----
    if todo_by_field:
        provider = VLLMOfflineProvider(
            model_path=mcfg["model_id"], quantization=mcfg["quant"],
            max_model_len=mcfg["max_len"],
            gpu_memory_utilization=mcfg["gpu_mem"], dtype="bfloat16",
            guided_regex=GUIDED_REGEX)
        transformer = DocumentFeaturesV2LLMTransformer(schemas_path, provider)
        t0 = time.time()
        staged = 0
        since_commit = 0
        SUB_BATCH = 2000  # checkpoint intra-campo: un corte pierde ≤1 sub-lote
        for field, items in todo_by_field.items():
            for b in range(0, len(items), SUB_BATCH):
                sub = items[b:b + SUB_BATCH]
                df = pd.DataFrame([{"nro_licitacion": n, "content_text": t}
                                   for n, t in sub])
                try:
                    df_res, metrics = transformer.compute_llm_fallback(
                        field, df, model_name=fp["model_id"])
                except Exception as e:
                    for n, _ in sub:
                        cur.execute("UPDATE dncp.llm_fallback_runs SET status='failed', "
                                    "attempts=attempts+1, last_error=%s "
                                    "WHERE run_id=%s AND nro_licitacion=%s AND field=%s",
                                    (f"{type(e).__name__}: {str(e)[:200]}",
                                     fp["run_id"], n, field))
                    conn.commit()
                    print(f"  {field}[{b}:{b+len(sub)}]: ERROR ({type(e).__name__}), "
                          f"marcado failed")
                    continue
                for _, r in df_res.iterrows():
                    txt = text_of[(str(r["nro_licitacion"]), field)]
                    sv, sc = sanitize_value(
                        field, None if pd.isna(r["llm_value"]) else float(r["llm_value"]))
                    # done = el modelo respondió (número o NULL explícito); failed = error.
                    # NULL con conf 0.0 confirma ausencia y el upsert con guardia no pisa nada.
                    cur.execute("UPDATE dncp.llm_fallback_runs SET value=%s, "
                                "conf_new=%s, raw_short=%s, status='done', "
                                "attempts=attempts+1, last_error=NULL "
                                "WHERE run_id=%s AND nro_licitacion=%s AND field=%s",
                                (sv, sc, str(r.get("llm_raw", ""))[:120],
                                 fp["run_id"], str(r["nro_licitacion"]), field))
                    if sv is not None:
                        cur.execute("INSERT INTO dncp.llm_fallback_cache "
                                    "(text_sha256, model_id, prompt_hash, field, value, "
                                    "raw_short) "
                                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                    (text_sha(txt), fp["model_id"], fp["prompt_hash"],
                                     field, sv,
                                     str(r.get("llm_raw", ""))[:120]))
                    staged += len(df_res)
                    since_commit += len(df_res)
                    if since_commit >= args.batch_commit:
                        conn.commit()
                        since_commit = 0
        conn.commit()
        print(f"GPU: {time.time()-t0:.0f}s")
        del provider
    cur.execute("SELECT status, count(*) FROM dncp.llm_fallback_runs "
                "WHERE run_id=%s GROUP BY 1", (fp["run_id"],))
    print("estado final:", dict(cur.fetchall()))

    # ---- 5. Promote opcional con guardia ----
    if args.promote:
        from src.etl.loaders.document_features_v2_llm_loader import (
            DocumentFeaturesV2LLMLoader)
        loader = DocumentFeaturesV2LLMLoader(_db_params())
        loader._conn = conn
        cur.execute("SELECT nro_licitacion AS \"nro_licitacion\", field AS field_name, "
                    "value AS llm_value, conf_new AS llm_confidence, "
                    "model_id AS llm_model FROM dncp.llm_fallback_runs "
                    "WHERE run_id=%s AND status='done' AND value IS NOT NULL",
                    (fp["run_id"],))
        cols = [d[0] for d in cur.description]
        df_all = pd.DataFrame(cur.fetchall(), columns=cols)
        if not df_all.empty:
            loader.ensure_llm_model_column()
            loader.upsert_guarded(df_all, fp["run_id"])
        else:
            print("nada para promover (sin done con valor)")
    conn.commit()
    conn.close()
    print(f"run {fp['run_id']} OK")


def _db_params():
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    return cfg


def _title_for_fields(schemas_path: str) -> dict:
    """field -> title_normalized (el extractor agrupa por título; aquí invertimos)."""
    import json as _json
    schemas = _json.loads(open(schemas_path, encoding="utf-8").read())
    out = {}
    for slug, schema in schemas.items():
        if slug == "meta":
            continue
        title = slug.replace("_", " ")
        for f in schema.get("fields", []):
            out[f["name"]] = title
    return out


if __name__ == "__main__":
    main()
