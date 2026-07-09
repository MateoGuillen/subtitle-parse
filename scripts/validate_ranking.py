"""
Validate the ranking via:
  N1 — Synthetic anomaly injection + AUC measurement
  N2 — Cross-method agreement (IF, LOF, DBSCAN)

Usage:
    python scripts/validate_ranking.py
    python scripts/validate_ranking.py --n-anomalies 500 --skip-n2
"""

import argparse
import json
import os
import sys
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.cluster import DBSCAN
from sklearn.neighbors import LocalOutlierFactor
from sklearn.metrics import roc_auc_score
from scipy.spatial.distance import pdist

from config.settings import BASE_OUTPUT_PROCESSED_DIR


def estimate_eps(X, percentile=5):
    """Estimate DBSCAN eps using percentile of pairwise distances."""
    sample = X[np.random.choice(len(X), min(1000, len(X)), replace=False)]
    dists = pdist(sample)
    return float(np.percentile(dists, percentile))


def main():
    parser = argparse.ArgumentParser(
        description="Validate ranking via N1 (synthetic AUC) + N2 (cross-method agreement)"
    )
    parser.add_argument("--ranking-csv", type=str, default=None)
    parser.add_argument("--n-anomalies", type=int, default=200)
    parser.add_argument("--skip-n2", action="store_true", help="Skip cross-method agreement")
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    output_dir = os.path.join(BASE_OUTPUT_PROCESSED_DIR, "title_ranking")
    os.makedirs(output_dir, exist_ok=True)

    ranking_csv = args.ranking_csv or os.path.join(
        output_dir, "title_ranking.csv"
    )
    if not os.path.exists(ranking_csv):
        print(f"ERROR: Ranking CSV not found: {ranking_csv}")
        return

    print(f"Loading ranking from: {ranking_csv}")
    ranking = pd.read_csv(ranking_csv)
    if "title_slug" not in ranking.columns:
        print("ERROR: CSV missing 'title_slug' column")
        return

    # Load document features
    from src.etl.extractors.title_ranking_extractor import TitleRankingExtractor
    from config.settings import DB_CONFIG
    extractor = TitleRankingExtractor(DB_CONFIG)
    df_doc = extractor.get_document_features()
    if df_doc is None:
        return

    top_slugs = ranking.head(args.top_k)["title_slug"].tolist()
    feat_cols = []
    for s in top_slugs:
        for prefix in ("has_", "len_", "tok_"):
            col = f"{prefix}{s}"
            if col in df_doc.columns:
                feat_cols.append(col)
    print(f"Features: {len(feat_cols)} cols (has_* + len_* + tok_*) for top-{args.top_k}")

    X = df_doc[feat_cols].values.astype(float)
    D = X.shape[0]

    rng = np.random.default_rng(args.seed)

    # ================================================================
    # N1: Synthetic Anomaly Injection
    # ================================================================
    print(f"\n=== N1: Synthetic AUC ===")
    n_synth = min(args.n_anomalies, D // 5)
    idx = rng.choice(D, size=n_synth, replace=False)
    X_anom = X[idx].copy()

    for j in range(X_anom.shape[1]):
        mask = rng.random(n_synth)
        p99 = float(np.percentile(X[:, j], 99))
        col_min = X[:, j].min()
        col_max = X[:, j].max()
        if not np.isnan(p99) and p99 > col_min:
            X_anom[mask < 0.4, j] = p99
        X_anom[(mask >= 0.4) & (mask < 0.7), j] = 0.0

    X_all = np.vstack([X, X_anom])
    y_all = np.concatenate([np.ones(D), np.zeros(n_synth)])

    clf = IsolationForest(contamination=0.05, random_state=args.seed, n_estimators=200)
    clf.fit(X)
    scores = clf.score_samples(X_all)
    auc_n1 = roc_auc_score(y_all, scores)
    print(f"  Synthetic AUC (N1): {auc_n1:.4f}  {'OK' if auc_n1 > 0.8 else 'WARN (need > 0.80)'}")

    # Comparison: random baseline (same K, random features)
    import random
    n1_results = {"synthetic_auc": auc_n1}
    baseline_aucs = []
    all_feat_cols = [c for c in df_doc.columns if c != "nro_licitacion"]
    for _ in range(5):
        random_cols = random.sample(all_feat_cols, min(len(feat_cols), len(all_feat_cols)))
        X_rand = df_doc[random_cols].values.astype(float)
        idx_r = rng.choice(D, size=n_synth, replace=False)
        X_ranom = X_rand[idx_r].copy()
        for j in range(X_ranom.shape[1]):
            mask = rng.random(n_synth)
            p99 = float(np.percentile(X_rand[:, j], 99))
            col_min = X_rand[:, j].min()
            if not np.isnan(p99) and p99 > col_min:
                X_ranom[mask < 0.4, j] = p99
            X_ranom[(mask >= 0.4) & (mask < 0.7), j] = 0.0
        X_all_r = np.vstack([X_rand, X_ranom])
        y_all_r = np.concatenate([np.ones(D), np.zeros(n_synth)])
        clf_r = IsolationForest(contamination=0.05, random_state=args.seed, n_estimators=200)
        clf_r.fit(X_rand)
        scores_r = clf_r.score_samples(X_all_r)
        baseline_aucs.append(roc_auc_score(y_all_r, scores_r))

    mean_baseline = np.mean(baseline_aucs)
    lift = auc_n1 - mean_baseline
    print(f"  Random baseline AUC: {mean_baseline:.4f}")
    print(f"  Lift vs random: {lift:+.4f}  {'OK' if lift > 0.05 else 'WARN (need > 0.05)'}")
    n1_results["baseline_mean_auc"] = float(mean_baseline)
    n1_results["lift_vs_random"] = float(lift)

    # ================================================================
    # N2: Cross-method Agreement
    # ================================================================
    n2_results = {}
    if not args.skip_n2:
        print(f"\n=== N2: Cross-method Agreement (IF, LOF, DBSCAN) ===")

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")

            # IF
            if_pred = IsolationForest(contamination=0.05, random_state=args.seed, n_jobs=-1).fit_predict(X)
            if_anom = set(np.where(if_pred == -1)[0])

            # LOF
            lof = LocalOutlierFactor(n_neighbors=20, contamination=0.05, n_jobs=-1)
            lof_pred = lof.fit_predict(X)
            lof_anom = set(np.where(lof_pred == -1)[0])

            # DBSCAN
            eps = estimate_eps(X)
            db = DBSCAN(eps=eps, min_samples=5, n_jobs=-1)
            db_pred = db.fit_predict(X)
            db_anom = set(np.where(db_pred == -1)[0])

        # Jaccard agreements
        pairs = [
            ("IF-LOF", if_anom, lof_anom),
            ("IF-DBSCAN", if_anom, db_anom),
            ("LOF-DBSCAN", lof_anom, db_anom),
        ]
        jaccards = []
        for name, a, b in pairs:
            union = a | b
            j = len(a & b) / len(union) if union else 0.0
            jaccards.append(j)
            n2_results[f"jaccard_{name}"] = float(j)

        mean_j = np.mean(jaccards)
        n2_results["mean_jaccard"] = float(mean_j)
        print(f"  Jaccard IF-LOF: {jaccards[0]:.4f}")
        print(f"  Jaccard IF-DBSCAN: {jaccards[1]:.4f}")
        print(f"  Jaccard LOF-DBSCAN: {jaccards[2]:.4f}")
        print(f"  Mean Jaccard: {mean_j:.4f}  {'OK' if mean_j > 0.3 else 'WARN (need > 0.30)'}")

        # Triple intersection (high-confidence anomalies)
        triple = if_anom & lof_anom & db_anom
        n2_results["triple_intersection_count"] = len(triple)
        n2_results["triple_intersection_pct"] = float(len(triple) / D * 100)
        print(f"  Triple intersection (high confidence): {len(triple)} docs ({len(triple)/D*100:.2f}%)")

    # Save results
    results = {"n1": n1_results, "n2": n2_results}
    results_path = os.path.join(output_dir, "validation_results.json")
    with open(results_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved: {results_path}")
    print("Done.")


if __name__ == "__main__":
    main()
