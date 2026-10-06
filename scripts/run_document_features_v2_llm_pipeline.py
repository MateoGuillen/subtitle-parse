"""
Phase 5 — LLM Fallback Pipeline entry point (vLLM offline API, WSL2).

Uses ``VLLMOfflineProvider`` (offline API, no HTTP server) with the champion
model Llama 3.1 8B-AWQ. Must be run in WSL with the vLLM env.

Usage (WSL, vLLM 0.23.0, offline API):

    VLLM_USE_V2_MODEL_RUNNER=0 /root/vllm-env/bin/python scripts/run_document_features_v2_llm_pipeline.py

Checkpoint file: ``data/processed/llm_pipeline_checkpoint.json``
    - Completed fields are saved after each field.
    - To re-run ALL fields, delete the checkpoint file.
    - To re-run specific fields, remove them from the checkpoint JSON.

Excluding the 3 "irresoluble" fields (num_componentes_formula,
num_documentos_pago, num_pasos_verificacion) saves ~64K useless GPU calls
(those fields have <5% accuracy by design — LLM cannot count). Set
``--include-irresolubles`` to process them anyway.
"""
import sys, os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

# vLLM V2 runner no funciona en WSL2 (UVA unavailable). Setear ANTES de
# cualquier import de vllm (VLLMOfflineProvider lo importa perezosamente).
os.environ.setdefault("VLLM_USE_V2_MODEL_RUNNER", "0")

import argparse
import json

from src.etl.transformers.llm_provider import VLLMOfflineProvider
from src.pipelines.document_features_v2_llm_pipeline import DocumentFeaturesV2LLMPipeline
from config.settings import DB_CONFIG


# Llama 3.1 8B-Instruct AWQ (champion Sesión 14, 100% en 5-campo GT humanas).
DEFAULT_MODEL_PATH = "thesven/Meta-Llama-3.1-8B-Instruct-AWQ"

# Campos "irresolubles": el LLM no puede contar listas/fórmulas; GT < 5%.
# Se excluyen por defecto para ahorrar ~64K llamadas inútiles en producción.
IRRESOLUBLES = {
    "num_componentes_formula",
    "num_documentos_pago",
    "num_pasos_verificacion",
}

CONF_THRESHOLD = 0.8
CHUNK_SIZE = 500


def main() -> None:
    parser = argparse.ArgumentParser(description="LLM Fallback Pipeline (vLLM offline API)")
    parser.add_argument(
        "--model", type=str, default=DEFAULT_MODEL_PATH,
        help=f"Ruta del modelo AWQ (default: {DEFAULT_MODEL_PATH})",
    )
    parser.add_argument(
        "--quantization", type=str, default="awq_marlin",
        help="Cuantización (awq_marlin para Llama/Mistral, awq para Qwen)",
    )
    parser.add_argument(
        "--max-model-len", type=int, default=8192,
        help="Max model context length (default 8192)",
    )
    parser.add_argument(
        "--max-num-seqs", type=int, default=32,
        help="vLLM max_num_seqs / concurrency (default 32)",
    )
    parser.add_argument(
        "--gpu-mem", type=float, default=0.70,
        help="gpu_memory_utilization (default 0.70, fits in 16 GiB VRAM)",
    )
    parser.add_argument(
        "--include-irresolubles", action="store_true",
        help="Incluir los 3 campos irresolubles (default: excluir para ahorrar GPU)",
    )
    parser.add_argument(
        "--exclude-fields", type=str, default="",
        help="Lista separada por coma de campos adicionales a excluir",
    )
    parser.add_argument(
        "--conf-threshold", type=float, default=CONF_THRESHOLD,
        help=f"Confianza mínima para saltear fallback (default {CONF_THRESHOLD})",
    )
    parser.add_argument(
        "--chunk-size", type=int, default=CHUNK_SIZE,
        help=f"Tamaño de lote por campo (default {CHUNK_SIZE})",
    )
    args = parser.parse_args()

    cp_path = str(
        Path(__file__).resolve().parents[1]
        / "data" / "processed" / "llm_pipeline_checkpoint.json"
    )

    exclusion = set()
    if not args.include_irresolubles:
        exclusion |= IRRESOLUBLES
    if args.exclude_fields:
        exclusion |= {f.strip() for f in args.exclude_fields.split(",") if f.strip()}

    llm = VLLMOfflineProvider(
        model_path=args.model,
        quantization=args.quantization,
        max_model_len=args.max_model_len,
        max_num_seqs=args.max_num_seqs,
        gpu_memory_utilization=args.gpu_mem,
        enforce_eager=True,
        trust_remote_code=True,
        dtype="float16",
    )

    print(f"  LLM: {llm.name()}")
    print(f"  Threshold: conf < {args.conf_threshold}")
    print(f"  Chunk size: {args.chunk_size}")
    print(f"  Checkpoint: {cp_path}")
    if exclusion:
        print(f"  Campos excluidos: {sorted(exclusion)}")
        print(f"    (irresolubles ahorran ~64K llamadas inútiles)")
    print()

    # Pasamos la lista de exclusión al extractor vía config. El pipeline la
    # inyecta en DocumentFeaturesV2LLMExtractor._hybrid_fields después de init.
    config = {
        "db_params": DB_CONFIG,
        "llm_provider": llm,
        "conf_threshold": args.conf_threshold,
        "chunk_size": args.chunk_size,
        "checkpoint_path": cp_path,
        "exclude_fields": exclusion,
    }
    pipeline = DocumentFeaturesV2LLMPipeline(config)
    pipeline.run()


if __name__ == "__main__":
    main()
