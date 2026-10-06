"""Descarga los modelos candidatos de la ronda 2 del retest (sesión 24).

Criterio: oficiales o de cuantizadores reconocidos, 2025-2026, que entran en 16 GB VRAM
(RTX 4070 Ti SUPER). vLLM -> caché HF (/root/.cache/huggingface); GGUF -> /root/models/gguf
(ext4; el NTFS de /mnt/datos no tiene espacio para ~65 GB). Escribe un marcador
/root/models/gguf/.ok_<tag> al terminar cada uno (lo usa la cola de retest).

Uso: /root/vllm-env/bin/python scripts/descargar_candidatos.py [--only tag1,tag2]
"""
import argparse
import sys
from pathlib import Path

from huggingface_hub import hf_hub_download, snapshot_download

GGUF_DIR = Path("/root/models/gguf")
CANDIDATES = {
    # vLLM
    "gemma-4-12b-qat-w4a16": ("snapshot", "google/gemma-4-12B-it-qat-w4a16-ct", None),
    "qwen3.5-9b-fp8": ("snapshot", "RedHatAI/Qwen3.5-9B-FP8-dynamic", None),
    # GGUF (llama-server)
    "ministral-3-8b-gguf": ("file", "mistralai/Ministral-3-8B-Instruct-2512-GGUF",
                            "Ministral-3-8B-Instruct-2512-Q8_0.gguf"),
    "ling-3.0-tiny-gguf": ("file", "inclusionAI/Ling-3.0-tiny-GGUF", "Ling-3.0-tiny-Q8_0.gguf"),
    "mimo-v2.6-9b-gguf": ("file", "bartowski/MiMo-V2.6-Distill-Qwen-9B-GGUF",
                          "MiMo-V2.6-Distill-Qwen-9B-Q8_0.gguf"),
    "clef-flash-gguf": ("file", "ggml-org/Clef-Flash-GGUF", "Clef-Flash-Q8_0.gguf"),
    "gemma-4-26b-a4b-gguf": ("file", "unsloth/gemma-4-26B-A4B-it-qat-GGUF",
                             "gemma-4-26B-A4B-it-qat-UD-Q4_K_XL.gguf"),
    "qwen3.6-35b-a3b-gguf": ("file", "unsloth/Qwen3.6-35B-A3B-GGUF",
                             "Qwen3.6-35B-A3B-UD-IQ3_XXS.gguf"),
    # Qwen3.5-9B-FP8 (vLLM) hace OOM al cargar: el repo es multimodal (torre de visión,
    # Qwen3_5ForConditionalGeneration, 14 GB). Se prueba como GGUF de solo texto.
    "qwen3.5-9b-gguf": ("file", "unsloth/Qwen3.5-9B-GGUF", "Qwen3.5-9B-Q8_0.gguf"),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    a = ap.parse_args()
    GGUF_DIR.mkdir(parents=True, exist_ok=True)
    tags = [t.strip() for t in a.only.split(",") if t.strip()] or list(CANDIDATES)
    for tag in tags:
        kind, repo, fname = CANDIDATES[tag]
        try:
            if kind == "snapshot":
                p = snapshot_download(repo, allow_patterns=["*.json", "*.safetensors", "*.txt",
                                                            "*.model", "*.jinja", "tokenizer*"])
            else:
                p = hf_hub_download(repo, fname, local_dir=str(GGUF_DIR))
            (GGUF_DIR / f".ok_{tag}").write_text(str(p))
            print(f"OK {tag} -> {p}", flush=True)
        except Exception as e:  # noqa: BLE001
            (GGUF_DIR / f".fail_{tag}").write_text(f"{type(e).__name__}: {e}")
            print(f"FAIL {tag}: {type(e).__name__}: {e}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
