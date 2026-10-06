"""Manifiesto sha256 de los datos que sostienen el benchmark (plan F0.5, hallazgo H5.3).

Escribe data/MANIFEST.sha256 (formato `sha256sum`) con los artefactos de
reproducibilidad. Con --check verifica que nada cambió desde la última escritura.

Uso:
  /root/vllm-env/bin/python scripts/manifest_datos.py          # (re)escribe
  /root/vllm-env/bin/python scripts/manifest_datos.py --check  # verifica
"""
import argparse
import hashlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "data" / "MANIFEST.sha256"
PATTERNS = [
    "data/processed/alolab/gt_por_campo/*.csv",
    "data/processed/llm_benchmark/*.csv",
    "data/processed/llm_benchmark/*.json",
    "data/processed/llm_benchmark/planillas/*.csv",
    "data/processed/llm_benchmark/multimodel_results/**/*.json",
    "data/processed/section_clustering_v2/master_schemas_v2.json",
    "data/processed/llm_runs/*.json",
]


def sha256(path: Path) -> str:
    """sha256 con fines de línea normalizados a LF en archivos de texto.

    El repo se versiona con eol=lf (.gitattributes) pero el checkout en NTFS
    puede tener CRLF: normalizar hace que el manifiesto valga en ambos.
    """
    data = path.read_bytes()
    if b"\0" not in data[:8192]:
        data = data.replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def collect() -> dict:
    files = sorted({p for pat in PATTERNS for p in ROOT.glob(pat) if p.is_file()})
    return {str(p.relative_to(ROOT)): sha256(p) for p in files}


def read_manifest() -> dict:
    out = {}
    for line in MANIFEST.read_text(encoding="utf-8").splitlines():
        if line.strip():
            digest, rel = line.split("  ", 1)
            out[rel] = digest
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true")
    args = ap.parse_args()
    current = collect()
    if not args.check:
        MANIFEST.write_text("".join(f"{d}  {p}\n" for p, d in current.items()),
                            encoding="utf-8")
        print(f"{len(current)} archivos -> {MANIFEST.relative_to(ROOT)}")
        return 0
    if not MANIFEST.exists():
        print("sin manifiesto: correr sin --check primero")
        return 2
    saved = read_manifest()
    changed = [p for p in saved if p in current and current[p] != saved[p]]
    missing = [p for p in saved if p not in current]
    new = [p for p in current if p not in saved]
    for label, items in (("CAMBIADO", changed), ("FALTA", missing), ("NUEVO", new)):
        for p in items:
            print(f"{label:9s} {p}")
    ok = not (changed or missing)
    print(f"{'OK' if ok else 'FALLA'}: {len(saved)} en manifiesto, "
          f"{len(changed)} cambiados, {len(missing)} faltan, {len(new)} nuevos")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
