"""Fingerprint de corrida del fallback LLM (diseño: docs/piloto_llm_fallback.md §3, §5).

Regla: re-run con distinto fingerprint = nuevo run_id obligatorio. La caché solo se
lee si el fingerprint coincide; si difiere, se aborta en vez de mezclar.
"""

import hashlib
import json
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def sha_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True,
            cwd=str(REPO_ROOT), timeout=15,
        ).stdout.strip() or "unknown"
    except Exception:
        return "unknown"


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


def compute_fingerprint(
    model_id: str,
    quant: str,
    system_prompt: str,
    schemas_path: str,
    sampling: dict,
    conf_threshold: float,
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
        "sampling": sampling,
        "conf_threshold": conf_threshold,
    }
    blob = json.dumps(fp, sort_keys=True, ensure_ascii=False)
    fp["fingerprint"] = hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]
    fp["run_id"] = (
        f"llmfb-{model_id.split('/')[-1].lower().replace('_', '-')}"
        f"-{fp['fingerprint']}"
    )
    return fp
