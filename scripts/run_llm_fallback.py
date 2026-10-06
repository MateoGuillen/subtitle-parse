"""Runner del fallback v2: reglas + LLM con origen explícito, fingerprint, caché y checkpoints.

Diseño: docs/piloto_llm_fallback.md + plan de corrección F3 (auditoría H1.1-H1.5, H4.3, H7.1).
El run v1 (llmfb-meta-llama-3.1-8b-instruct-fp8-2834432bb7237924, tablas v1) queda congelado.

Flujo por (doc, campo) de la población (regex no-match, conf < threshold):
  1. CPU: rule_value(campo, texto). Si la regla aplica -> done, source='rule'
     (incluso si dictamina ausencia). Si no aplica (o --llm-all) -> pending (GPU).
  2. Caché v2 (solo crudos): hit -> el valor se recalcula con resolve() actual.
  3. GPU: guided regex por tipo (int no admite decimales), crudo completo guardado,
     value/source según --mode (default rule_first = producción).
Seguridad:
  - fingerprint incluye sha de los archivos de código; árbol sucio -> aborta
    (salvo --allow-dirty, que queda registrado en dncp.llm_runs);
  - antes de cada sub-lote se re-verifica el sha del código: si cambió, aborta;
  - un sub-lote que falla se reintenta ítem por ítem (un prompt largo no tira 2000);
  - este script NO promueve a producción: usar scripts/promote_fallback.py.

Ejemplos:
  Solo reglas (CPU, sin GPU), población completa:
    /root/vllm-env/bin/python scripts/run_llm_fallback.py --no-llm
  Smoke con GPU:
    /root/vllm-env/bin/python scripts/run_llm_fallback.py --model qwen3-8b-fp8 \\
        --pilot-docs 50 --fields dias_pago_maximo,num_documentos_pago
  Reanudar: ... --run-id <run_id>
"""
import argparse
import hashlib
import json
import random
import sys
import time
from pathlib import Path

import psycopg2
import psycopg2.extras

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402
from src.etl.extractors.document_features_v2_llm_extractor import (  # noqa: E402
    DocumentFeaturesV2LLMExtractor)
from src.etl.transformers.document_features_v2_llm_transformer import (  # noqa: E402
    DocumentFeaturesV2LLMTransformer)
from src.etl.transformers.extraction_postprocess import (  # noqa: E402
    NO_RULE, PRODUCTION_MODE, resolve, rule_value)
from src.etl.transformers.run_fingerprint import (  # noqa: E402
    code_fingerprint, compute_fingerprint, generation_fingerprint)

GUIDED = {  # H4.3: los int no pueden emitir "1.5"
    "int": r"^(\d{1,4}|NULL)$",
    "float": r"^(-?\d+(?:[.,]\d+)?|NULL)$",
}
MODELS = {
    "llama-fp8": dict(model_id="RedHatAI/Meta-Llama-3.1-8B-Instruct-FP8",
                      quant=None, gpu_mem=0.70, max_len=8192),
    "llama-awq": dict(model_id="thesven/Meta-Llama-3.1-8B-Instruct-AWQ",
                      quant=None, gpu_mem=0.70, max_len=8192),
    "qwen3-8b-fp8": dict(model_id="Qwen/Qwen3-8B-FP8",
                         quant=None, gpu_mem=0.80, max_len=8192),
}
SCHEMAS = ROOT / "data/processed/section_clustering_v2/master_schemas_v2.json"
RUNS_DIR = ROOT / "data/processed/llm_runs"


def db_conn():
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    conn = psycopg2.connect(**cfg)
    conn.autocommit = False
    return conn


def text_sha(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def num(v):
    return None if v is None else float(v)


def field_schemas() -> dict:
    s = json.loads(SCHEMAS.read_text(encoding="utf-8"))
    return {f["name"]: f for k, v in s.items() if k != "meta" for f in v["fields"]}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", default="qwen3-8b-fp8", choices=list(MODELS))
    ap.add_argument("--fields", default="", help="coma-separados; vacío = todos los hybrid")
    ap.add_argument("--pilot-docs", type=int, default=0, help="0 = población completa")
    ap.add_argument("--mode", default=PRODUCTION_MODE, choices=["rule_first", "llm_only"])
    ap.add_argument("--llm-all", action="store_true",
                    help="consultar al LLM aunque la regla aplique (ablación F4.5)")
    ap.add_argument("--no-llm", action="store_true", help="solo etapa de reglas (CPU)")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--conf-threshold", type=float, default=0.8)
    ap.add_argument("--sub-batch", type=int, default=2000)
    ap.add_argument("--allow-dirty", action="store_true")
    ap.add_argument("--features-table", default="dncp.document_features_v3",
                    help="tabla de la que sale la población (v3: status='no_extraido')")
    args = ap.parse_args()

    mcfg = MODELS[args.model]
    system_prompt = DocumentFeaturesV2LLMTransformer._build_system_prompt()
    sampling = {"temperature": 0.0, "max_tokens": 16, "guided_by_type": GUIDED,
                "max_model_len": mcfg["max_len"]}
    fschemas = field_schemas()
    want = sorted(f.strip() for f in args.fields.split(",") if f.strip())
    extra = {"mode": args.mode, "llm_all": args.llm_all, "pilot_docs": args.pilot_docs,
             "fields": want, "runner": "v2", "features_table": args.features_table}
    fp = compute_fingerprint(mcfg["model_id"], mcfg["quant"], system_prompt, str(SCHEMAS),
                             sampling, args.conf_threshold, extra)
    if fp["git_dirty"] and not args.allow_dirty:
        raise SystemExit("ABORTA: archivos de código difieren de HEAD (git_dirty). "
                         "Commitear antes de correr, o --allow-dirty (queda registrado).")
    code_fp_start = code_fingerprint()
    conn = db_conn()
    cur = conn.cursor()

    # ---- 1. Registro / verificación de la corrida
    if args.run_id:
        cur.execute("SELECT fingerprint FROM dncp.llm_runs WHERE run_id=%s", (args.run_id,))
        row = cur.fetchone()
        if not row:
            raise SystemExit(f"run-id desconocido: {args.run_id}")
        if row[0] != fp["fingerprint"]:
            raise SystemExit("FINGERPRINT DIFIERE del run original (código, modelo, prompt, "
                             "schema o parámetros): iniciar un run nuevo sin --run-id.")
        run_id = args.run_id
        print(f"[resume] {run_id}")
    else:
        run_id = fp["run_id"]
        cur.execute("SELECT 1 FROM dncp.llm_runs WHERE run_id=%s", (run_id,))
        if cur.fetchone():
            raise SystemExit(f"{run_id} ya existe: usar --run-id {run_id} para reanudar")
        cur.execute("INSERT INTO dncp.llm_runs (run_id, fingerprint, config, git_sha, git_dirty)"
                    " VALUES (%s,%s,%s,%s,%s)",
                    (run_id, fp["fingerprint"], json.dumps(fp, ensure_ascii=False),
                     fp["code_version"], fp["git_dirty"]))
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        (RUNS_DIR / f"{run_id}.json").write_text(json.dumps(fp, indent=1, ensure_ascii=False),
                                                 encoding="utf-8")
        conn.commit()
        print(f"[nuevo run] {run_id} (git_dirty={fp['git_dirty']})")

        # ---- 2. Población + etapa de reglas (CPU)
        cfg = dict(DB_CONFIG)
        cfg["dbname"] = cfg.pop("database")
        extractor = DocumentFeaturesV2LLMExtractor(cfg, str(SCHEMAS), args.conf_threshold,
                                                   features_table=args.features_table)
        failed = extractor.get_failed_extractions_grouped()
        if want:
            failed = {k: v for k, v in failed.items() if k in want}
        rows = []
        for field, df in failed.items():
            for r in df.itertuples():
                if (r.content_text or "").strip():
                    rows.append((str(r.nro_licitacion), field, r.content_text,
                                 r.section_title, r.section_line_start, r.section_year))
        if args.pilot_docs:
            docs = sorted({x[0] for x in rows})
            random.Random(42).shuffle(docs)
            keep = set(docs[:args.pilot_docs])
            rows = [x for x in rows if x[0] in keep]
        t0 = time.time()
        batch = []
        n_rule = 0
        for nro, field, text, title, line, year in rows:
            rv = rule_value(field, text)
            applies = rv is not NO_RULE
            needs_llm = (not applies) or args.llm_all
            if args.mode == "llm_only":
                needs_llm = True
            done = not needs_llm
            n_rule += applies
            batch.append((run_id, nro, field, title, line, year, text_sha(text), applies,
                          num(rv) if applies else None,
                          (num(rv) if applies else None) if done else None,
                          ("rule" if done else None), args.mode,
                          "done" if done else "pending"))
        psycopg2.extras.execute_values(cur, """
            INSERT INTO dncp.llm_fallback_runs_v2
              (run_id, nro_licitacion, field, section_title, section_line_start, section_year,
               text_sha256, rule_applies, rule_value, value, source, mode, status)
            VALUES %s ON CONFLICT DO NOTHING""", batch, page_size=5000)
        conn.commit()
        print(f"población: {len(rows)} filas-campo | regla aplica: {n_rule} | "
              f"a LLM: {sum(1 for b in batch if b[-1] == 'pending')} | {time.time()-t0:.0f}s CPU")

    # Reintentos: failed con attempts < 3 vuelven a pending
    cur.execute("UPDATE dncp.llm_fallback_runs_v2 SET status='pending' "
                "WHERE run_id=%s AND status='failed' AND attempts < 3", (run_id,))
    conn.commit()
    if args.no_llm:
        _finish(conn, run_id)
        return

    # ---- 3. Pendientes: texto exacto por clave de sección + verificación de hash
    cur.execute("""
        SELECT r.nro_licitacion, r.field, r.text_sha256, s.content_text
        FROM dncp.llm_fallback_runs_v2 r
        JOIN dncp.pliegos_secciones s
          ON s.nro_licitacion = r.nro_licitacion AND s.title = r.section_title
         AND s.line_start = r.section_line_start AND s.year = r.section_year
        WHERE r.run_id=%s AND r.status='pending'""", (run_id,))
    pending = {}
    for nro, field, sha, text in cur.fetchall():
        if text_sha(text) != sha:
            cur.execute("UPDATE dncp.llm_fallback_runs_v2 SET status='failed', attempts=attempts+1,"
                        " last_error='texto de la sección cambió desde el alta' "
                        "WHERE run_id=%s AND nro_licitacion=%s AND field=%s", (run_id, nro, field))
            continue
        pending.setdefault(field, []).append((nro, text))
    conn.commit()

    rev = fp["model_revision"]
    gen_fp = {f: generation_fingerprint(mcfg["model_id"], rev, mcfg["quant"], system_prompt,
                                        fschemas[f], {**sampling, "guided": GUIDED[_ftype(fschemas, f)]})
              for f in pending}

    # ---- 4. Caché v2 (crudos) -> resolve() con el código actual
    todo = {}
    n_hit = 0
    for field, items in pending.items():
        ftype = _ftype(fschemas, field)
        for nro, text in items:
            cur.execute("SELECT raw FROM dncp.llm_fallback_cache_v2 WHERE text_sha256=%s "
                        "AND field=%s AND gen_fp=%s", (text_sha(text), field, gen_fp[field]))
            hit = cur.fetchone()
            if hit:
                n_hit += 1
                _store(cur, run_id, nro, field, text, hit[0], args.mode, ftype, from_cache=True)
            else:
                todo.setdefault(field, []).append((nro, text))
    conn.commit()
    print(f"cache hits: {n_hit} | a GPU: {sum(len(v) for v in todo.values())}")

    # ---- 5. GPU
    if todo:
        from src.etl.transformers.llm_provider import VLLMOfflineProvider
        provider = VLLMOfflineProvider(
            model_path=mcfg["model_id"], quantization=mcfg["quant"],
            max_model_len=mcfg["max_len"], gpu_memory_utilization=mcfg["gpu_mem"],
            dtype="bfloat16", guided_regex=GUIDED["float"])
        transformer = DocumentFeaturesV2LLMTransformer(str(SCHEMAS), provider)
        t0 = time.time()
        n_gpu = 0
        for field, items in todo.items():
            ftype = _ftype(fschemas, field)
            provider.guided_regex = GUIDED[ftype]
            schema = fschemas[field]
            for b in range(0, len(items), args.sub_batch):
                if code_fingerprint() != code_fp_start:
                    conn.commit()
                    raise SystemExit("ABORTA: el código cambió durante la corrida "
                                     "(fingerprint de código). Lo hecho queda en staging.")
                sub = items[b:b + args.sub_batch]
                msgs = [transformer._build_messages(schema, t, None) for _, t in sub]
                raws = _generate(provider, msgs)
                for (nro, text), raw in zip(sub, raws):
                    if isinstance(raw, Exception):
                        cur.execute("UPDATE dncp.llm_fallback_runs_v2 SET status='failed', "
                                    "attempts=attempts+1, last_error=%s WHERE run_id=%s AND "
                                    "nro_licitacion=%s AND field=%s",
                                    (f"{type(raw).__name__}: {str(raw)[:200]}", run_id, nro, field))
                        continue
                    _store(cur, run_id, nro, field, text, raw, args.mode, ftype)
                    cur.execute("INSERT INTO dncp.llm_fallback_cache_v2 (text_sha256, field, "
                                "gen_fp, raw) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                                (text_sha(text), field, gen_fp[field], raw))
                n_gpu += len(sub)
                conn.commit()  # checkpoint por sub-lote
                print(f"  {field}[{b}:{b+len(sub)}] ok ({n_gpu} en {time.time()-t0:.0f}s)")
        del provider
    _finish(conn, run_id)


def _ftype(fschemas, field):
    return "int" if fschemas[field]["type"] in ("int", "binary") else "float"


def _generate(provider, msgs):
    """Genera el sub-lote; si falla entero, reintenta ítem por ítem."""
    try:
        return provider.generate_batch_messages(msgs, max_tokens=16)
    except Exception:
        out = []
        for m in msgs:
            try:
                out.append(provider.generate_batch_messages([m], max_tokens=16)[0])
            except Exception as e:  # noqa: BLE001
                out.append(e)
        return out


def _store(cur, run_id, nro, field, text, raw, mode, ftype, from_cache=False):
    res = resolve(field, raw, text, mode=mode, ftype=ftype)
    cur.execute("""
        UPDATE dncp.llm_fallback_runs_v2
           SET raw=%s, model_value=%s, value=%s, source=%s, from_cache=%s,
               status='done', attempts=attempts+1, last_error=NULL
         WHERE run_id=%s AND nro_licitacion=%s AND field=%s""",
                (raw, num(res.llm_value), num(res.value), res.source, from_cache,
                 run_id, nro, field))


def _finish(conn, run_id):
    cur = conn.cursor()
    cur.execute("UPDATE dncp.llm_runs SET finished_at=now() WHERE run_id=%s", (run_id,))
    cur.execute("SELECT field, status, source, count(*), count(value) FROM "
                "dncp.llm_fallback_runs_v2 WHERE run_id=%s GROUP BY 1,2,3 ORDER BY 1,2,3",
                (run_id,))
    for r in cur.fetchall():
        print("  ", r)
    conn.commit()
    conn.close()
    print(f"run {run_id} OK")


if __name__ == "__main__":
    main()
