"""F2.0 — Congela el comportamiento ACTUAL de los dos parsers antes del refactor.

  bench = extraction_postprocess.parse_response(raw, field, text, strict=True)
          (usado por C3/C4/C5/Fase 5 y benchmark_multi_vs_gt)
  prod  = DocumentFeaturesV2LLMTransformer._parse_response(raw, ftype, field, text)[0]
          (usado por el fallback de producción, run_llm_fallback.py)

Escribe tests/fixtures/legacy_snapshot.json {cid: [bench, prod]} sobre el corpus de
tests/corpus.py. Los tests exigen que los modos legacy de `resolve()` reproduzcan
este snapshot exacto. Correr UNA vez antes de tocar extraction_postprocess.py.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))

from corpus import iter_corpus  # noqa: E402
from src.etl.transformers.extraction_postprocess import parse_response  # noqa: E402
from src.etl.transformers.document_features_v2_llm_transformer import (  # noqa: E402
    DocumentFeaturesV2LLMTransformer)

OUT = ROOT / "tests/fixtures/legacy_snapshot.json"


def norm(v):
    return None if v is None else float(v)


def main():
    if OUT.exists() and "--force" not in sys.argv:
        raise SystemExit(f"{OUT} ya existe (snapshot pre-refactor). --force para regenerar.")
    snap = {}
    for cid, field, raw, text, ftype in iter_corpus():
        bench = norm(parse_response(raw, field=field, text=text, strict=True))
        prod = norm(DocumentFeaturesV2LLMTransformer._parse_response(
            raw.strip(), ftype, field=field, text=text)[0])
        snap[cid] = [bench, prod]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(snap, separators=(",", ":")), encoding="utf-8")
    n_diff = sum(1 for b, p in snap.values() if b != p)
    print(f"{len(snap)} ítems -> {OUT.relative_to(ROOT)} | bench≠prod en {n_diff}")


if __name__ == "__main__":
    main()
