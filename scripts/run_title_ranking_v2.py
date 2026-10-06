"""
Fase 2 — Run title_ranking_pipeline v2.2 con las 8 estrategias.

Reformulación del pipeline de selección de títulos:
  - Carga title_diagnostics.csv (Fase 1)
  - Aplica Filtro C: deja fuera títulos descartados por boilerplate
  - Calcula 8 estrategias (6 heredadas + boilerplate + numeric_content)
  - Activa evaluate_optimal_k (auto_k=True) para elegir N entre 5-20
  - Output a data/processed/title_ranking_v2/ (no sobrescribe v1)

Uso:
    python scripts/run_title_ranking_v2.py
    python scripts/run_title_ranking_v2.py --no-auto-k --no-drop-descartados
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import DB_CONFIG
from src.pipelines.title_ranking_pipeline import TitleRankingPipeline


def main():
    parser = argparse.ArgumentParser(description="Title ranking v2.2 con 8 estrategias.")
    parser.add_argument(
        "--output-dir",
        default="data/processed/title_ranking_v2",
        help="Directorio de salida (default: data/processed/title_ranking_v2)",
    )
    parser.add_argument(
        "--diagnostics-path",
        default="data/processed/title_diagnostics/title_diagnostics.csv",
        help="CSV de diagnóstico de Fase 1.",
    )
    parser.add_argument(
        "--candidate-filter-path",
        default=None,
        help="CSV opcional con whitelist de title_normalized/slug. Si se omite, se usa drop_descartados.",
    )
    parser.add_argument("--no-auto-k", action="store_true", help="Desactivar evaluate_optimal_k")
    parser.add_argument(
        "--no-drop-descartados",
        action="store_true",
        help="No descartar títulos con descartado_boilerplate=True",
    )
    parser.add_argument(
        "--section-sample",
        type=int,
        default=30000,
        help="Tamaño de muestreo de secciones (default 30000, igual a v1)",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=80,
        help="Top-N raw titles a considerar (default 80)",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=None,
        help="Top-K a usar硬odedif auto_k False (default None = auto_k alone)",
    )
    parser.add_argument(
        "--run-stability",
        action="store_true",
        help="Correr bootstrap stability (30 iter, lento)",
    )
    args = parser.parse_args()

    config = {
        "db_params": DB_CONFIG,
        "top_n": args.top_n,
        "section_sample": args.section_sample,
        "top_k": args.top_k,
        "auto_k": not args.no_auto_k,
        "run_stability": args.run_stability,
        "output_dir": args.output_dir,
        "diagnostics_path": args.diagnostics_path,
        "candidate_filter_path": args.candidate_filter_path,
        "drop_descartados": not args.no_drop_descartados,
    }

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)
    pipeline = TitleRankingPipeline(config)
    summary = pipeline.run()

    print("\n=== RESUMEN ===")
    if summary:
        for k, v in summary.items():
            print(f"  {k}: {v}")


if __name__ == "__main__":
    main()
