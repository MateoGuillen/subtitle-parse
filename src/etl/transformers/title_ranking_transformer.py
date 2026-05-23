"""Transformer for title anomaly-ranking pipeline.

Implements five complementary strategies and combines them into a single
relevance score per title.
"""

from typing import List, Dict, Optional
import numpy as np
import pandas as pd
from sklearn.ensemble import (
    RandomForestClassifier,
    IsolationForest,
)
from src.utils.logging_utils import setup_logger


class TitleRankingTransformer:
    """
    Ranks titles by their relevance for anomaly detection using five
    complementary strategies.

    **Weights** (from spec):
        * Outlier frequency (Tukey IQR)          — 0.25
        * Proxy Random Forest importance          — 0.30
        * Contextual anomalies (category_id)      — 0.15
        * Section-level Isolation Forest          — 0.20
        * Document-level IF point-biserial corr.  — 0.10
    """

    WEIGHTS = {
        "score_outlier": 0.25,
        "score_rf": 0.30,
        "score_contextual": 0.15,
        "score_section_if": 0.20,
        "score_corr": 0.10,
    }

    def __init__(self, random_state: int = 42):
        self.random_state = random_state
        self.logger = setup_logger(__name__)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_ranking(
        self,
        df_doc: pd.DataFrame,
        df_sec: pd.DataFrame,
        title_slugs: List[str],
    ) -> pd.DataFrame:
        """
        Run all five strategies and produce a combined ranking.

        Args:
            df_doc:  ``document_features`` DataFrame (one row per document).
            df_sec:  Sampled ``pliegos_secciones`` DataFrame.
            title_slugs:  Sorted list of unique title slugs from columns.

        Returns:
            DataFrame with columns
            ``[title_slug, display_name, score_outlier, score_rf,
              score_contextual, score_section_if, score_corr,
              score_total, rank]``
            ordered by ``rank``.
        """
        self.logger.info("=== Strategy 1/5: Tukey Outlier Frequency ===")
        s1 = self._strategy_outlier_frequency(df_doc, title_slugs)

        self.logger.info("=== Strategy 2/5: Proxy Random Forest ===")
        s2 = self._strategy_rf_importance(df_doc, title_slugs)

        self.logger.info("=== Strategy 3/5: Contextual Anomalies ===")
        s3 = self._strategy_contextual(df_doc, title_slugs)

        self.logger.info("=== Strategy 4/5: Section-level Isolation Forest ===")
        s4 = self._strategy_section_if(df_sec, title_slugs)

        self.logger.info("=== Strategy 5/5: Document-level IF Correlation ===")
        s5 = self._strategy_doc_correlation(df_doc, title_slugs)

        # Assemble ranking table
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
            })

        ranking = pd.DataFrame(records)
        ranking["score_total"] = sum(
            ranking[col] * w for col, w in self.WEIGHTS.items()
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


# ------------------------------------------------------------------
# Helper
# ------------------------------------------------------------------

def _normalize_series(s: pd.Series, eps: float = 1e-12) -> pd.Series:
    """Min-max scale a series to [0, 1] (0 if constant)."""
    lo, hi = s.min(), s.max()
    if hi - lo < eps:
        return s * 0.0
    return (s - lo) / (hi - lo)
