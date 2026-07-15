"""Transformer for title anomaly-ranking pipeline.

Implements six complementary strategies and combines them into a single
relevance score per title.  Supports NMI diversity, Synthetic AUC, optimal K
selection, and bootstrap stability analysis.
"""

from typing import List, Dict, Optional, Tuple
import numpy as np
import pandas as pd
from sklearn.ensemble import (
    RandomForestClassifier,
    IsolationForest,
)
from sklearn.metrics import roc_auc_score
from scipy.stats import spearmanr
from src.utils.logging_utils import setup_logger

# Risk indicators used by the 6th strategy (economic risk correlation)
ECONOMIC_RISK_COLS = [
    "es_unico_oferente",
    "overbudget_ratio",
    "is_high_value_single_bidder",
    "winner_category_frequency",
    "is_repeat_winner",
    "winner_total_contracts",
    "bidder_diversity",
]


class TitleRankingTransformer:
    """
    Ranks titles by their relevance for anomaly detection using six
    complementary strategies.

    **Weights** (from spec):
        * Outlier frequency (Tukey IQR)          — 0.20
        * Proxy Random Forest importance          — 0.25
        * Contextual anomalies (category_id)      — 0.15
        * Section-level Isolation Forest          — 0.15
        * Document-level IF point-biserial corr.  — 0.10
        * Economic risk correlation                — 0.15

    **New in v2.1** (hybrid plan):
        * ``compute_nmi_diversity()`` — NMI-based diversity metric (B3)
        * ``compute_synthetic_auc()`` — AUC against injected anomalies (C1)
        * ``evaluate_optimal_k()`` — score compuesto z-normalizado (A1+B3+C1)
        * ``bootstrap_stability()`` — 30-iter bootstrap (S2)
        * ``sensitivity_to_k()`` — Spearman rho between K variants (S1)
    """

    # Pesos de las 6 estrategias
    STRATEGY_WEIGHTS = {
        "score_outlier": 0.20,
        "score_rf": 0.25,
        "score_contextual": 0.15,
        "score_section_if": 0.15,
        "score_corr": 0.10,
        "score_economic": 0.15,
    }

    # Pesos del score compuesto (K* optimal)
    COMPOSITE_WEIGHTS = {
        "coverage": 0.35,
        "diversity": 0.35,
        "separability": 0.30,
    }

    def __init__(self, random_state: int = 42):
        self.random_state = random_state
        self.rng = np.random.default_rng(random_state)
        self.logger = setup_logger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_ranking(
        self,
        df_doc: pd.DataFrame,
        df_sec: pd.DataFrame,
        title_slugs: List[str],
        df_econ: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """
        Run all six strategies and produce a combined ranking.
        """
        self.logger.info("=== Strategy 1/6: Tukey Outlier Frequency ===")
        s1 = self._strategy_outlier_frequency(df_doc, title_slugs)

        self.logger.info("=== Strategy 2/6: Proxy Random Forest ===")
        s2 = self._strategy_rf_importance(df_doc, title_slugs)

        self.logger.info("=== Strategy 3/6: Contextual Anomalies ===")
        s3 = self._strategy_contextual(df_doc, title_slugs)

        self.logger.info("=== Strategy 4/6: Section-level Isolation Forest ===")
        s4 = self._strategy_section_if(df_sec, title_slugs)

        self.logger.info("=== Strategy 5/6: Document-level IF Correlation ===")
        s5 = self._strategy_doc_correlation(df_doc, title_slugs)

        self.logger.info("=== Strategy 6/6: Economic Risk Correlation ===")
        s6 = self._strategy_economic_risk(df_doc, df_econ, title_slugs)

        records = []
        for slug in title_slugs:
            display = slug.replace("_", " ").title()
            records.append({
                "title_slug": slug,
                "display_name": display,
                "score_outlier": s1.get(slug, 0.0),
                "score_rf": s2.get(slug, 0.0),
                "score_contextual": s3.get(slug, 0.0),
                "score_section_if": s4.get(slug, 0.0),
                "score_corr": s5.get(slug, 0.0),
                "score_economic": s6.get(slug, 0.0),
            })

        ranking = pd.DataFrame(records)
        ranking["score_total"] = sum(
            ranking[col] * w for col, w in self.STRATEGY_WEIGHTS.items()
        )
        ranking["rank"] = ranking["score_total"].rank(ascending=False).astype(int)
        ranking.sort_values("rank", inplace=True)
        ranking.reset_index(drop=True, inplace=True)

        self.logger.info(
            "Ranking complete. Top-5: %s",
            ranking.head(5)["display_name"].tolist(),
        )
        return ranking

    # ------------------------------------------------------------------
    # New v2.1: NMI Diversity (B3)
    # ------------------------------------------------------------------

    @staticmethod
    def compute_nmi_diversity(
        df_doc: pd.DataFrame,
        slugs: List[str],
    ) -> float:
        """
        Compute mean Normalized Mutual Information (NMI) among all title pairs.

        Returns:
            Mean NMI across all unique pairs (lower = more diverse).
            NMI = MI(has_i; has_j) / sqrt(H(has_i) * H(has_j))
        """
        n = len(slugs)
        if n < 2:
            return 1.0

        def entropy(p):
            p = np.clip(p, 1e-12, 1 - 1e-12)
            return -p * np.log2(p) - (1 - p) * np.log2(1 - p)

        nmi_values = []
        for i in range(n):
            col_i = f"has_{slugs[i]}"
            if col_i not in df_doc.columns:
                continue
            vi = df_doc[col_i].values.astype(float)
            hi = entropy(vi.mean())
            if hi < 1e-12:
                continue
            for j in range(i + 1, n):
                col_j = f"has_{slugs[j]}"
                if col_j not in df_doc.columns:
                    continue
                vj = df_doc[col_j].values.astype(float)
                hj = entropy(vj.mean())
                if hj < 1e-12:
                    continue
                # Joint entropy
                pairs = np.column_stack([vi, vj])
                p_00 = ((vi == 0) & (vj == 0)).mean()
                p_01 = ((vi == 0) & (vj == 1)).mean()
                p_10 = ((vi == 1) & (vj == 1)).mean()
                p_11 = ((vi == 1) & (vj == 1)).mean()
                p_joint = np.clip([p_00, p_01, p_10, p_11], 1e-12, None)
                h_joint = -np.sum(p_joint * np.log2(p_joint))

                mi = hi + hj - h_joint
                nmi = mi / np.sqrt(hi * hj) if hi * hj > 0 else 0.0
                nmi_values.append(np.clip(nmi, 0.0, 1.0))

        return float(np.mean(nmi_values)) if nmi_values else 1.0

    # ------------------------------------------------------------------
    # New v2.1: Synthetic AUC (C1)
    # ------------------------------------------------------------------

    def compute_synthetic_auc(
        self,
        X_subset: np.ndarray,
        n_synthetic: Optional[int] = None,
        contamination: float = 0.05,
    ) -> float:
        """
        Inject synthetic anomalies and measure AUC of Isolation Forest.

        For each anomaly, randomly mutates features:
          - 40%: set to max value (over-dimensioned)
          - 30%: set to min value (omission)
          - 30%: unchanged

        For binary features, this becomes flip 0→1 and flip 1→0.
        """
        D = X_subset.shape[0]
        n_synthetic = n_synthetic or max(100, int(0.05 * D))
        n_synthetic = min(n_synthetic, D)
        rng = self.rng

        idx = rng.choice(D, size=n_synthetic, replace=False)
        X_anom = X_subset[idx].copy()

        for j in range(X_anom.shape[1]):
            mask = rng.random(n_synthetic)
            col_min = float(X_subset[:, j].min())
            col_max = float(X_subset[:, j].max())

            is_binary = (col_min == 0.0 and col_max == 1.0 and
                         set(np.unique(X_subset[:, j]).astype(int).tolist()).issubset({0, 1}))

            if is_binary:
                for row_i in range(n_synthetic):
                    if mask[row_i] < 0.4:
                        X_anom[row_i, j] = 1.0
                    elif mask[row_i] < 0.7:
                        X_anom[row_i, j] = 0.0
            else:
                p99 = float(np.percentile(X_subset[:, j], 99))
                if np.isnan(p99) or p99 <= col_min:
                    p99 = col_max
                X_anom[mask < 0.4, j] = p99
                X_anom[(mask >= 0.4) & (mask < 0.7), j] = 0.0

        X_all = np.vstack([X_subset, X_anom])
        y_all = np.concatenate([np.ones(D), np.zeros(n_synthetic)])

        clf = IsolationForest(
            contamination=contamination,
            random_state=self.random_state,
            n_estimators=200,
            n_jobs=-1,
        )
        clf.fit(X_subset)
        scores = clf.score_samples(X_all)

        return float(roc_auc_score(y_all, scores))

    # ------------------------------------------------------------------
    # New v2.1: Optimal K Evaluation (Wrapper)
    # ------------------------------------------------------------------

    def evaluate_optimal_k(
        self,
        ranking: pd.DataFrame,
        df_doc: pd.DataFrame,
        k_values: Optional[List[int]] = None,
    ) -> pd.DataFrame:
        """
        Evaluate multiple K values using coverage (A1), NMI diversity (B3),
        and synthetic AUC (C1).  Uses has_*, len_*, tok_* features.

        Best K = argmax(0.35*z(cov) + 0.35*z(div) + 0.30*z(auc)).
        """
        if k_values is None:
            k_values = [5, 8, 10, 12, 15, 20]

        results = []
        slugs = ranking["title_slug"].tolist()

        for k in k_values:
            top_slugs = slugs[:k]
            # Build feature matrix: has_* + len_* + tok_*
            feat_cols = []
            for s in top_slugs:
                for prefix in ("has_", "len_", "tok_"):
                    col = f"{prefix}{s}"
                    if col in df_doc.columns:
                        feat_cols.append(col)

            if not feat_cols:
                self.logger.warning("  K=%d: no valid columns, skipping.", k)
                continue

            X = df_doc[feat_cols].values.astype(float)
            n_docs = len(df_doc)

            # Coverage@K (based on has_* only)
            has_cols = [f"has_{s}" for s in top_slugs if f"has_{s}" in df_doc.columns]
            if has_cols:
                has_data = df_doc[has_cols].values.astype(float)
                coverage = float((has_data.sum(axis=1) > 0).mean())
            else:
                coverage = 0.0

            # Diversity@K (based on has_*)
            div = 1.0 - self.compute_nmi_diversity(df_doc, top_slugs) if len(top_slugs) >= 2 else 1.0

            # Synthetic AUC@K (based on all feat cols)
            auc_val = self.compute_synthetic_auc(X)

            results.append({
                "K": k,
                "coverage": coverage,
                "diversity": div,
                "synthetic_auc": auc_val,
            })

        if not results:
            self.logger.error("No valid K values evaluated!")
            return pd.DataFrame()

        eval_df = pd.DataFrame(results)

        # Z-normalize each column, then combine with weights
        w = self.COMPOSITE_WEIGHTS
        for col in ["coverage", "diversity", "synthetic_auc"]:
            mu, std = eval_df[col].mean(), eval_df[col].std()
            if std > 1e-12:
                eval_df[f"z_{col}"] = (eval_df[col] - mu) / std
            else:
                eval_df[f"z_{col}"] = 0.0

        eval_df["score"] = (
            w["coverage"] * eval_df["z_coverage"]
            + w["diversity"] * eval_df["z_diversity"]
            + w["separability"] * eval_df["z_synthetic_auc"]
        )
        eval_df["is_optimal"] = eval_df["score"] == eval_df["score"].max()
        eval_df.sort_values("K", inplace=True)
        eval_df.reset_index(drop=True, inplace=True)

        best_k = int(eval_df.loc[eval_df["score"].idxmax(), "K"])
        self.logger.info("Optimal K* = %d (score=%.4f)", best_k, eval_df["score"].max())
        for _, row in eval_df.iterrows():
            flag = " <- K*" if row["is_optimal"] else ""
            self.logger.info(
                "  K=%2d  coverage=%.4f  diversity=%.4f  auc=%.4f  score=%+.4f%s",
                row["K"], row["coverage"], row["diversity"],
                row["synthetic_auc"], row["score"], flag,
            )

        return eval_df

    # ------------------------------------------------------------------
    # New v2.1: Bootstrap Stability (S2)
    # ------------------------------------------------------------------

    def bootstrap_stability(
        self,
        df_doc: pd.DataFrame,
        df_sec: pd.DataFrame,
        slugs: List[str],
        K: int = 10,
        n_iter: int = 30,
    ) -> Dict[str, float]:
        """
        Bootstrap resampling to measure selection frequency for each title.

        Returns:
            Dict of {title_slug: frequency (0-1)} in top-K across n_iter
            bootstrap samples.
        """
        self.logger.info(
            "Bootstrap stability: %d iterations, K=%d, %d titles, %d docs.",
            n_iter, K, len(slugs), len(df_doc),
        )
        D = len(df_doc)
        counts = {s: 0 for s in slugs}

        for it in range(n_iter):
            if (it + 1) % 10 == 0:
                self.logger.info("  Bootstrap iteration %d/%d", it + 1, n_iter)
            idx = self.rng.choice(D, size=D, replace=True)
            df_doc_boot = df_doc.iloc[idx].reset_index(drop=True)
            idx_sec = self.rng.choice(len(df_sec), size=len(df_sec), replace=True) if df_sec is not None else None
            df_sec_boot = df_sec.iloc[idx_sec].reset_index(drop=True) if df_sec is not None else None
            ranking = self.compute_ranking(df_doc_boot, df_sec_boot, slugs)
            for s in ranking.head(K)["title_slug"].tolist():
                counts[s] += 1

        return {s: c / n_iter for s, c in counts.items()}

    # ------------------------------------------------------------------
    # New v2.1: Sensitivity to K (S1)
    # ------------------------------------------------------------------

    @staticmethod
    def sensitivity_to_k(
        ranking: pd.DataFrame,
        k_pairs: Optional[List[Tuple[int, int]]] = None,
    ) -> Dict[str, float]:
        """
        Spearman rho between top-K rankings at different K values.

        Args:
            ranking: Full ranking DataFrame (title_slug + rank columns).
            k_pairs: List of (K1, K2) pairs to compare. Default: [(8,10), (10,12), (8,12)].

        Returns:
            Dict of "{K1}-{K2}" -> Spearman rho.
        """
        if k_pairs is None:
            k_pairs = [(8, 10), (10, 12), (8, 12)]

        slugs = ranking["title_slug"].tolist()
        ranks = {s: i for i, s in enumerate(slugs)}
        results = {}

        for k1, k2 in k_pairs:
            top1 = set(slugs[:k1])
            top2 = set(slugs[:k2])
            common = list(top1 & top2)
            if len(common) < 5:
                results[f"{k1}-{k2}"] = float("nan")
                continue
            r1 = [ranks[t] for t in common]
            r2 = [ranks[t] for t in common]
            rho, _ = spearmanr(r1, r2)
            results[f"{k1}-{k2}"] = float(rho)

        return results

    # ------------------------------------------------------------------
    # Strategy implementations
    # ------------------------------------------------------------------

    # ---- 1. Tukey outlier frequency (weight 0.25) -----------------------

    @staticmethod
    def _strategy_outlier_frequency(
        df: pd.DataFrame, slugs: List[str]
    ) -> Dict[str, float]:
        """For each title, average % of documents where ``len_*`` / ``tok_*``
        fall outside Tukey's fences [Q1-1.5*IQR, Q3+1.5*IQR]."""
        scores: Dict[str, float] = {}
        for slug in slugs:
            pcts = []
            for col in (f"len_{slug}", f"tok_{slug}"):
                if col not in df.columns:
                    continue
                vals = df[col]
                Q1, Q3 = vals.quantile(0.25), vals.quantile(0.75)
                iqr = Q3 - Q1
                lo, hi = Q1 - 1.5 * iqr, Q3 + 1.5 * iqr
                pct = ((vals < lo) | (vals > hi)).mean() * 100.0
                pcts.append(pct)
            scores[slug] = np.mean(pcts) if pcts else 0.0
        return _normalize_series(pd.Series(scores)).to_dict()

    # ---- 2. Proxy Random Forest (weight 0.30) ---------------------------

    def _strategy_rf_importance(
        self, df: pd.DataFrame, slugs: List[str]
    ) -> Dict[str, float]:
        """Train an RF on original vs. noise-corrupted data and sum Gini
        importance per title across ``has_*``, ``len_*``, ``tok_*``."""
        feature_cols = [c for c in df.columns if c != "nro_licitacion"]
        X = df[feature_cols].values.astype(np.float64)
        rng = np.random.default_rng(self.random_state)

        # Build noisy copy: 5% of cells get Gaussian noise (σ=0.1×std)
        X_noise = X.copy()
        mask = rng.random(X.shape) < 0.05
        noise = rng.normal(0, 0.1, X.shape) * np.nanstd(X, axis=0, keepdims=True)
        noise[~np.isfinite(noise)] = 0.0
        X_noise[mask] += noise[mask]

        X_full = np.vstack([X, X_noise])
        y = np.concatenate([np.ones(len(X)), np.zeros(len(X_noise))])

        self.logger.info("  RF input: %d rows, %d features.", X_full.shape[0], X_full.shape[1])

        rf = RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            random_state=self.random_state,
            n_jobs=-1,
        )
        rf.fit(X_full, y)

        feat_imp = dict(zip(feature_cols, rf.feature_importances_))

        scores: Dict[str, float] = {}
        for slug in slugs:
            imp = 0.0
            for prefix in ("has_", "len_", "tok_"):
                imp += feat_imp.get(f"{prefix}{slug}", 0.0)
            scores[slug] = imp
        return _normalize_series(pd.Series(scores)).to_dict()

    # ---- 3. Contextual anomalies by category_id (weight 0.15) -----------

    @staticmethod
    def _strategy_contextual(
        df: pd.DataFrame, slugs: List[str]
    ) -> Dict[str, float]:
        """For each title, count documents whose ``len_*`` / ``tok_*``
        have a within-group ``|z| > 3`` (group = ``category_id``, NaN treated
        as a single group)."""
        df_out = df.copy()
        # Handle missing category_id as "desconocido"
        df_out["_group"] = df_out["category_id"].fillna("__desconocido__")

        scores: Dict[str, float] = {}
        n_total = len(df_out)

        for slug in slugs:
            outlier_count = 0
            for col in (f"len_{slug}", f"tok_{slug}"):
                if col not in df_out.columns:
                    continue
                grp_stats = df_out.groupby("_group")[col].agg(["mean", "std"])
                # Map group stats back
                gmeans = df_out["_group"].map(grp_stats["mean"])
                gstds = df_out["_group"].map(grp_stats["std"])
                z = (df_out[col] - gmeans) / gstds.replace(0, np.nan)
                outlier_count += (z.abs() > 3).sum()
            scores[slug] = outlier_count / n_total * 100.0
        return _normalize_series(pd.Series(scores)).to_dict()

    # ---- 4. Section-level Isolation Forest (weight 0.20) -----------------

    def _strategy_section_if(
        self, df_sec: pd.DataFrame, slugs: List[str]
    ) -> Dict[str, float]:
        """Train an IF on section-level features (content_length, tokens,
        page, line_start, line_end, year, one-hot titles).  For each title,
        compute the fraction of its sections with anomaly score above the
        95th percentile."""
        if df_sec is None or df_sec.empty:
            self.logger.warning("  No section data — returning zeros.")
            return {s: 0.0 for s in slugs}

        # Prepare feature matrix
        df_feat = df_sec.copy()

        # Numeric features
        num_cols = ["content_length", "estimated_tokens", "page",
                     "line_start", "line_end", "year"]
        X_num = df_feat[num_cols].fillna(0).values.astype(np.float64)

        # One-hot encode top-N titles
        title_set = set(slugs)
        # Map slug -> raw title (reverse of make_safe_col: underscore -> space)
        slug_to_raw = {s: s.replace("_", " ") for s in slugs}
        raw_to_slug = {v: k for k, v in slug_to_raw.items()}

        one_hot_cols = []
        one_hot_data = np.zeros((len(df_feat), len(slugs)), dtype=np.float64)
        for i, raw_title in enumerate(df_feat["title_normalized"].values):
            raw_title = str(raw_title) if raw_title is not None else ""
            slug = raw_to_slug.get(raw_title)
            if slug is not None:
                idx = slugs.index(slug)
                one_hot_data[i, idx] = 1.0

        X = np.concatenate([X_num, one_hot_data], axis=1)

        self.logger.info("  Section IF input: %d rows, %d features.", X.shape[0], X.shape[1])

        ifo = IsolationForest(
            contamination=0.05,
            n_estimators=200,
            random_state=self.random_state,
            n_jobs=-1,
        )
        ifo.fit(X)
        anomaly_scores = ifo.score_samples(X)  # more negative = more anomalous
        threshold = np.percentile(anomaly_scores, 5)  # bottom 5%

        scores: Dict[str, float] = {}
        for slug in slugs:
            slug_idx = slugs.index(slug)
            mask = one_hot_data[:, slug_idx] == 1.0
            n_total = mask.sum()
            if n_total == 0:
                scores[slug] = 0.0
                continue
            n_anom = (anomaly_scores[mask] < threshold).sum()
            scores[slug] = n_anom / n_total * 100.0
        return _normalize_series(pd.Series(scores)).to_dict()

    # ---- 5. Document-level IF point-biserial correlation (weight 0.10) --

    def _strategy_doc_correlation(
        self, df: pd.DataFrame, slugs: List[str]
    ) -> Dict[str, float]:
        """Train an IF on the full document-feature matrix and measure
        the absolute difference in mean anomaly score between documents
        that have a given title vs. those that don't."""
        feature_cols = [c for c in df.columns if c != "nro_licitacion"]
        X = df[feature_cols].values.astype(np.float64)

        ifo = IsolationForest(
            contamination=0.05,
            random_state=self.random_state,
            n_jobs=-1,
        )
        ifo.fit(X)
        doc_scores = ifo.score_samples(X)  # larger = more normal

        scores: Dict[str, float] = {}
        for slug in slugs:
            has_col = f"has_{slug}"
            if has_col not in df.columns:
                scores[slug] = 0.0
                continue
            has = df[has_col].values.astype(bool)
            if has.sum() == 0 or (~has).sum() == 0:
                scores[slug] = 0.0
                continue
            diff = abs(doc_scores[has].mean() - doc_scores[~has].mean())
            scores[slug] = diff
        return _normalize_series(pd.Series(scores)).to_dict()

    # ---- 6. Economic risk correlation (weight 0.15) --------------------

    def _strategy_economic_risk(
        self,
        df_doc: pd.DataFrame,
        df_econ: Optional[pd.DataFrame],
        slugs: List[str],
    ) -> Dict[str, float]:
        risk_cols = [c for c in ECONOMIC_RISK_COLS if c in (df_econ.columns if df_econ is not None else [])]
        if not risk_cols:
            self.logger.warning("  No economic risk columns available.")
            return {s: 0.0 for s in slugs}
        eco = df_econ[["nro_licitacion"] + risk_cols].copy()
        for c in risk_cols:
            eco[c] = pd.to_numeric(eco[c], errors="coerce").fillna(0)

        scores: Dict[str, float] = {}
        for slug in slugs:
            has_col = f"has_{slug}"
            if has_col not in df_doc.columns:
                scores[slug] = 0.0
                continue
            merged = df_doc[["nro_licitacion", has_col]].merge(eco, on="nro_licitacion", how="inner")
            if len(merged) < 100:
                scores[slug] = 0.0
                continue
            has = merged[has_col].astype(float)
            correlations = []
            for c in risk_cols:
                r = has.corr(merged[c])
                if np.isfinite(r):
                    correlations.append(abs(r))
            scores[slug] = float(np.mean(correlations)) if correlations else 0.0
        return _normalize_series(pd.Series(scores)).to_dict()


# ------------------------------------------------------------------
# Helper
# ------------------------------------------------------------------

def _normalize_series(s: pd.Series, eps: float = 1e-12) -> pd.Series:
    """Min-max scale a series to [0, 1] (0 if constant)."""
    lo, hi = s.min(), s.max()
    if hi - lo < eps:
        return s * 0.0
    return (s - lo) / (hi - lo)
