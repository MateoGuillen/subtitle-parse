"""Fingerprint de corrida del fallback LLM (diseño: docs/piloto_llm_fallback.md §3, §5).

Regla: re-run con distinto fingerprint = nuevo run_id obligatorio. La caché solo se
lee si el fingerprint coincide; si difiere, se aborta en vez de mezclar.

Sesión 23 (auditoría H1.3/H1.4, plan F3):
  - el fingerprint incluye el sha256 de los ARCHIVOS de código que determinan los
    valores (antes solo el git HEAD, que no veía cambios sin commitear);
  - `git_dirty`: si esos archivos difieren de HEAD, el runner aborta salvo
    --allow-dirty (y queda registrado en dncp.llm_runs);
  - `generation_fingerprint()`: huella por campo de lo que determina el CRUDO del
    modelo (modelo+revisión+quant+prompt+schema del campo+sampling+guided). Es la
    clave de la caché v2; NO incluye el código de reglas, porque la caché guarda
    crudos y el valor se recalcula al leer.
"""

import hashlib
import json
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

# Archivos cuyo contenido determina los valores que produce el fallback.
CODE_FILES = [
    "src/etl/transformers/extraction_postprocess.py",
    "src/etl/transformers/field_rules_v1.py",
    "src/etl/transformers/document_features_v2_llm_transformer.py",
    "src/etl/transformers/document_features_v2_transformer.py",
    "src/etl/transformers/llm_provider.py",
    "src/etl/extractors/document_features_v2_llm_extractor.py",
    "scripts/run_llm_fallback.py",
]


def sha_file(path: str) -> str:
    """sha256 con fines de línea normalizados (NTFS/CRLF vs checkout LF)."""
    data = Path(path).read_bytes().replace(b"\r\n", b"\n")
    return hashlib.sha256(data).hexdigest()


def _git(*args) -> str:
    try:
        return subprocess.run(["git", *args], capture_output=True, text=True,
                              cwd=str(REPO_ROOT), timeout=30).stdout.strip()
    except Exception:
        return ""


def git_sha() -> str:
    return _git("rev-parse", "HEAD") or "unknown"


def code_shas() -> dict:
    return {f: sha_file(str(REPO_ROOT / f)) for f in CODE_FILES}


def code_fingerprint() -> str:
    blob = json.dumps(code_shas(), sort_keys=True)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def git_dirty() -> bool:
    """True si algún archivo de CODE_FILES no coincide con HEAD (o no está trackeado)."""
    tracked = set(_git("ls-files", "--", *CODE_FILES).splitlines())
    if any(f not in tracked for f in CODE_FILES):
        return True
    for f in CODE_FILES:
        head = subprocess.run(["git", "show", f"HEAD:{f}"], capture_output=True,
                              cwd=str(REPO_ROOT)).stdout.replace(b"\r\n", b"\n")
        if hashlib.sha256(head).hexdigest() != sha_file(str(REPO_ROOT / f)):
            return True
    return False


def resolve_model_revision(model_id: str) -> str:
    """Intenta resolver el snapshot HF cacheado; si no, 'unresolved' (queda registrado)."""
    try:
        from huggingface_hub import scan_cache_dir
        for repo in scan_cache_dir().repos:
            if repo.repo_id == model_id and repo.revisions:
                return sorted(r.commit_hash for r in repo.revisions)[-1][:12]
    except Exception:
        pass
    return "unresolved"


def generation_fingerprint(model_id: str, model_revision: str, quant, system_prompt: str,
                           field_schema: dict, sampling: dict) -> str:
    """Huella de lo que determina el crudo de UN campo (clave de la caché v2)."""
    blob = json.dumps({
        "model_id": model_id, "model_revision": model_revision, "quant": quant,
        "system_prompt_sha": hashlib.sha256(system_prompt.encode()).hexdigest(),
        "field_schema": field_schema, "sampling": sampling,
    }, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(blob.encode()).hexdigest()[:16]


def compute_fingerprint(
    model_id: str,
    quant,
    system_prompt: str,
    schemas_path: str,
    sampling: dict,
    conf_threshold: float,
    extra: dict = None,
) -> dict:
    """Devuelve dict con run_id determinista + todos los componentes versionados."""
    prompt_hash = hashlib.sha256(system_prompt.encode("utf-8")).hexdigest()
    fp = {
        "model_id": model_id,
        "model_revision": resolve_model_revision(model_id),
        "quant": quant,
        "prompt_hash": prompt_hash,
        "schema_version": sha_file(schemas_path),
        "code_version": git_sha(),
        "code_files": code_shas(),
        "git_dirty": git_dirty(),
        "sampling": sampling,
        "conf_threshold": conf_threshold,
        **(extra or {}),
    }
    blob = json.dumps({k: v for k, v in fp.items() if k != "git_dirty"},
                      sort_keys=True, ensure_ascii=False)
    fp["fingerprint"] = hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]
    fp["run_id"] = (
        f"llmfb-{model_id.split('/')[-1].lower().replace('_', '-')}"
        f"-{fp['fingerprint']}"
    )
    return fp
