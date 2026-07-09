"""
Full pipeline: ranking (with auto-K) → clustering seed → validation.

Usage:
    # Everything: ranking + stability + validation
    python scripts/run_full_pipeline.py --all

    # Ranking only (with auto-K)
    python scripts/run_full_pipeline.py --auto-k

    # Full with clustering
    python scripts/run_full_pipeline.py --complete

    # Without bootstrap (faster)
    python scripts/run_full_pipeline.py --all --skip-bootstrap

    # Override K
    python scripts/run_full_pipeline.py --auto-k --top-k 12
"""

import argparse
import os
import sys
import subprocess
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.settings import BASE_OUTPUT_PROCESSED_DIR


SCRIPTS_DIR = Path(__file__).resolve().parent


def run_script(name: str, label: str, args: list) -> bool:
    """Run a Python script and return success status."""
    cmd = [sys.executable, str(SCRIPTS_DIR / name)] + [str(a) for a in args]
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"  {' '.join(cmd)}")
    print(f"{'='*60}")
    result = subprocess.run(cmd, cwd=SCRIPTS_DIR.parent)
    ok = result.returncode == 0
    if not ok:
        print(f"  ERROR: {label} failed (code {result.returncode})")
    return ok


def main():
    parser = argparse.ArgumentParser(
        description="Full title selection pipeline (v2.1)"
    )
    parser.add_argument("--auto-k", action="store_true",
                        help="Step 1: Ranking with optimal K evaluation")
    parser.add_argument("--stability", action="store_true",
                        help="Step 2: Bootstrap stability + sensitivity")
    parser.add_argument("--validation", action="store_true",
                        help="Step 3: N1 + N2 validation")
    parser.add_argument("--clustering", action="store_true",
                        help="Step 4: Section clustering (uses ranking CSV)")
    parser.add_argument("--all", action="store_true",
                        help="All steps (auto-k + stability + validation)")
    parser.add_argument("--complete", action="store_true",
                        help="All steps + clustering")
    parser.add_argument("--top-k", type=int, default=None,
                        help="Override top-K (default: auto)")
    parser.add_argument("--skip-bootstrap", action="store_true",
                        help="Skip bootstrap in stability analysis")
    parser.add_argument("--skip-llm", action="store_true",
                        help="Skip LLM schema generation in clustering")
    args = parser.parse_args()

    if args.all:
        args.auto_k = True
        args.stability = True
        args.validation = True
    if args.complete:
        args.auto_k = True
        args.stability = True
        args.validation = True
        args.clustering = True

    if not any([args.auto_k, args.stability, args.validation, args.clustering]):
        print("No steps selected. Use --auto-k, --stability, --validation, "
              "--clustering, --all, or --complete")
        parser.print_help()
        return

    all_ok = True

    # Step 1: Ranking with auto-K
    if args.auto_k:
        ranking_args = ["--auto-k"]
        if args.top_k:
            ranking_args += ["--top-k", str(args.top_k)]
        ok = run_script(
            "run_title_ranking_pipeline.py",
            "Step 1/4: Title ranking with optimal K*",
            ranking_args,
        )
        all_ok = all_ok and ok

    # Step 2: Stability analysis
    if args.stability and all_ok:
        stability_args = []
        if args.skip_bootstrap:
            stability_args.append("--skip-bootstrap")
        if args.top_k:
            stability_args += ["--K", str(args.top_k)]
        ok = run_script(
            "analyze_ranking_stability.py",
            "Step 2/4: Ranking stability (S1 + S2)",
            stability_args,
        )
        all_ok = all_ok and ok

    # Step 3: Validation
    if args.validation and all_ok:
        val_args = []
        if args.skip_bootstrap:
            val_args.append("--skip-n2")
        if args.top_k:
            val_args += ["--top-k", str(args.top_k)]
        ok = run_script(
            "validate_ranking.py",
            "Step 3/4: Ranking validation (N1 + N2)",
            val_args,
        )
        all_ok = all_ok and ok

    # Step 4: Clustering
    if args.clustering and all_ok:
        default_csv = os.path.join(
            BASE_OUTPUT_PROCESSED_DIR, "title_ranking", "title_ranking.csv"
        )
        clust_args = ["--from-ranking", "--ranking-csv", default_csv]
        if args.top_k:
            clust_args += ["--top-k", str(args.top_k)]
        if args.skip_llm:
            clust_args.append("--skip-llm")
        ok = run_script(
            "run_section_clustering_pipeline.py",
            "Step 4/4: Section clustering (from ranking)",
            clust_args,
        )
        all_ok = all_ok and ok

    if all_ok:
        print(f"\n{'='*60}")
        print("  Pipeline completado exitosamente.")
        print(f"{'='*60}")
    else:
        print(f"\n{'='*60}")
        print("  Pipeline completado con errores. Revisar logs.")
        print(f"{'='*60}")
        sys.exit(1)


if __name__ == "__main__":
    main()
