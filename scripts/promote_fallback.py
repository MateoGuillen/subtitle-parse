"""Promoción (y rollback) staging v2 -> producción, por campo y con historia (plan F3/F7).

Por defecto es DRY-RUN: imprime qué se escribiría. Para escribir: --apply.
  /root/vllm-env/bin/python scripts/promote_fallback.py --run-id <id> --fields a,b
  /root/vllm-env/bin/python scripts/promote_fallback.py --run-id <id> --fields a,b --apply
  /root/vllm-env/bin/python scripts/promote_fallback.py --run-id <id> --rollback [--apply]
--table permite apuntar a una copia (p. ej. dncp.dfv2_test) para ensayar.
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config.settings import DB_CONFIG  # noqa: E402
from src.etl.loaders.document_features_v2_llm_loader import (  # noqa: E402
    DocumentFeaturesV2LLMLoader)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--fields", default="")
    ap.add_argument("--table", default="dncp.document_features_v2")
    ap.add_argument("--rollback", action="store_true")
    ap.add_argument("--apply", action="store_true", help="escribir (sin esto: dry-run)")
    a = ap.parse_args()
    fields = [f.strip() for f in a.fields.split(",") if f.strip()]
    if not a.rollback and not fields:
        raise SystemExit("--fields es obligatorio para promover (H7.1)")
    cfg = dict(DB_CONFIG)
    cfg["dbname"] = cfg.pop("database")
    loader = DocumentFeaturesV2LLMLoader(cfg)
    loader.connect()
    try:
        if a.rollback:
            out = loader.rollback_run(a.run_id, fields or None, a.table, dry_run=not a.apply)
        else:
            out = loader.promote_run(a.run_id, fields, a.table, dry_run=not a.apply)
    finally:
        loader.disconnect()
    print(("APLICADO" if a.apply else "DRY-RUN (nada escrito)") + f" sobre {a.table}:")
    print(json.dumps(out, indent=1, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
