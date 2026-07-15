"""Transformer for document features pipeline."""

import re
import unicodedata
from typing import List
import pandas as pd
import numpy as np
from src.utils.logging_utils import setup_logger


class DocumentFeaturesTransformer:
    """
    Builds a document-level feature matrix from the section-level DataFrame.

    For each document (``nro_licitacion``) the transformer produces:

    * **Fixed aggregate features** — total sections, unique title count,
      summary statistics for ``content_length``, ``estimated_tokens`` and
      ``size_bytes``, plus the document's ``year`` and ``category_id``.

    * **Per-title pivot features** — for each of the *top-N* titles, three
      columns are created:
        ``has_<title>`` (SMALLINT, 1/0 presence flag),
        ``len_<title>`` (INT, sum of ``content_length``),
        ``tok_<title>`` (INT, sum of ``estimated_tokens``).

    * **Others catch-all** — ``has_otros``, ``len_otros``, ``tok_otros``
      for any title outside the top-N.

    Usage::

        transformer = DocumentFeaturesTransformer(top_titles)
        df_features = transformer.compute_document_features(df_sections)
    """

    # Columns produced by the document-level aggregation
    FIXED_AGG_COLS: List[str] = [
        "nro_licitacion",
        "year",
        "category_id",
        "total_sections",
        "unique_titles",
        "avg_content_length",
        "std_content_length",
        "max_content_length",
        "sum_content_length",
        "avg_tokens",
        "std_tokens",
        "max_tokens",
        "sum_tokens",
        "avg_size_bytes",
        "std_size_bytes",
        "max_size_bytes",
        "sum_size_bytes",
        "sum_word_count",
        "avg_word_count",
        "avg_page",
        "max_page",
        "gini_content_length",
        "title_entropy",
    ]

    def __init__(self, top_titles: List[str]):
        """
        Args:
            top_titles: Ordered list of title strings (most frequent first)
                        to pivot into individual feature columns.
        """
        self.top_titles = top_titles
        self.top_title_set = set(top_titles)
        self.logger = setup_logger(__name__)

    @staticmethod
    def make_safe_col(prefix: str, title: str, max_len: int = 50) -> str:
        """
        Convert a raw title string into a safe PostgreSQL column name.

        Steps:
            1. Lower-case and strip.
            2. Unicode NFKD normalization + ASCII stripping
               (e.g. ``"corrupción"`` → ``"corrupcion"``).
            3. Remove all non-word, non-space characters.
            4. Replace spaces with underscores.
            5. Truncate to *max_len* and strip trailing underscores.

        Args:
            prefix: Column prefix (``"has_"``, ``"len_"`` or ``"tok_"``).
            title: Raw ``title_normalized`` value.
            max_len: Maximum length of the name portion (without prefix).

        Returns:
            Safe column name, e.g. ``"has_fraude_y_corrupcion"``.
        """
        name = title.lower().strip()
        name = (
            unicodedata.normalize("NFKD", name)
            .encode("ascii", "ignore")
            .decode("ascii")
        )
        name = re.sub(r"[^\w\s]", "", name)
        name = name.replace(" ", "_")
        name = name[:max_len].rstrip("_")
        return f"{prefix}{name}"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compute_document_features(self, df_sections: pd.DataFrame) -> pd.DataFrame:
        """
        Produce the complete document-level feature matrix.

        Args:
            df_sections: Section-level DataFrame from the extractor with
                         columns ``nro_licitacion``, ``title_normalized``,
                         ``content_length``, ``estimated_tokens``,
                         ``size_bytes``, ``year``, ``category_id``,
                         ``word_count``, ``page``, ``line_start``,
                         ``line_end``.

        Returns:
            Feature matrix — one row per document, one column per feature.
            All missing pivot values are filled with ``0``.
        """
        self.logger.info("Computing document-level features…")

        # -- 1. Document-level aggregates -----------------------------------
        doc_agg = self._compute_aggregates(df_sections)
        self.logger.info("Aggregates computed: %d documents.", len(doc_agg))

        # -- 2. Split top-N vs. other titles --------------------------------
        mask_top = df_sections["title_normalized"].isin(self.top_title_set)
        df_top = df_sections[mask_top]
        df_other = df_sections[~mask_top]

        self.logger.info(
            "Top-title sections: %d  |  Other sections: %d",
            len(df_top),
            len(df_other),
        )

        # -- 3. Pivot tables for top titles ----------------------------------
        pivot_has = self._pivot_presence(df_top)
        pivot_len = self._pivot_sum(df_top, "content_length", "len_")
        pivot_tok = self._pivot_sum(df_top, "estimated_tokens", "tok_")
        pivot_word = self._pivot_sum(df_top, "word_count", "word_")
        pivot_count = self._pivot_agg(df_top, "content_length", "count", "count_")

        # Span and page range per title
        pivot_span, pivot_page = self._pivot_span_page(df_top)

        # -- 4. Other-title aggregates ---------------------------------------
        otros = self._compute_otros(df_other, doc_agg["nro_licitacion"])
        otros_word = self._compute_otros_word(df_other, doc_agg["nro_licitacion"])

        # -- 5. Merge --------------------------------------------------------
        features = doc_agg.copy()
        for pv in (pivot_len, pivot_has, pivot_tok, pivot_word,
                    pivot_count, pivot_span, pivot_page):
            if pv is not None and not pv.empty:
                features = features.merge(
                    pv.reset_index(),
                    on="nro_licitacion",
                    how="left",
                    suffixes=("", "_dup"),
                )

        features = features.merge(otros, on="nro_licitacion", how="left")
        features = features.merge(otros_word, on="nro_licitacion", how="left")

        # -- 6. Clean-up -----------------------------------------------------
        # Drop duplicate columns that may have been introduced by merge
        dup_cols = [c for c in features.columns if c.endswith("_dup")]
        if dup_cols:
            features.drop(columns=dup_cols, inplace=True)

        # Fill NaN values left by documents that have no top-title sections
        features.fillna(0, inplace=True)

        # Fix data types
        self._fix_dtypes(features)

        # Drop zero-variance columns
        self._drop_zero_variance(features)

        # Log potential data-leak warnings
        self._detect_leaks(features)

        self.logger.info(
            "Final feature matrix: %d rows × %d columns.",
            len(features),
            len(features.columns),
        )
        return features

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _compute_aggregates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Group sections by document and compute summary statistics."""

        # Gini coefficient per document
        gini_df = self._compute_gini(df)

        # Title entropy per document
        entropy_df = self._compute_title_entropy(df)

        agg = df.groupby("nro_licitacion", sort=False).agg(
            year=("year", "first"),
            category_id=("category_id", "first"),
            total_sections=("content_length", "count"),
            unique_titles=("title_normalized", "nunique"),
            avg_content_length=("content_length", "mean"),
            std_content_length=("content_length", "std"),
            max_content_length=("content_length", "max"),
            sum_content_length=("content_length", "sum"),
            avg_tokens=("estimated_tokens", "mean"),
            std_tokens=("estimated_tokens", "std"),
            max_tokens=("estimated_tokens", "max"),
            sum_tokens=("estimated_tokens", "sum"),
            avg_size_bytes=("size_bytes", "mean"),
            std_size_bytes=("size_bytes", "std"),
            max_size_bytes=("size_bytes", "max"),
            sum_size_bytes=("size_bytes", "sum"),
            sum_word_count=("word_count", "sum"),
            avg_word_count=("word_count", "mean"),
            avg_page=("page", "mean"),
            max_page=("page", "max"),
        ).reset_index()

        agg = agg.merge(gini_df, on="nro_licitacion", how="left")
        agg = agg.merge(entropy_df, on="nro_licitacion", how="left")
        return agg

    @staticmethod
    def _compute_gini(df: pd.DataFrame) -> pd.DataFrame:
        """Gini coefficient of content_length per document."""
        def _gini(values):
            sorted_vals = np.sort(values.values.astype(float))
            n = len(sorted_vals)
            if n == 0 or sorted_vals.sum() == 0:
                return 0.0
            cumsum = np.cumsum(sorted_vals)
            return float((2 * np.sum(cumsum) / sorted_vals.sum() - (n + 1)) / n)

        gini_df = df.groupby("nro_licitacion")["content_length"].apply(_gini).reset_index()
        gini_df.columns = ["nro_licitacion", "gini_content_length"]
        return gini_df

    @staticmethod
    def _compute_title_entropy(df: pd.DataFrame) -> pd.DataFrame:
        """Shannon entropy of title distribution per document."""
        counts = df.groupby(["nro_licitacion", "title_normalized"]).size().reset_index(name="count")
        totals = counts.groupby("nro_licitacion")["count"].sum().reset_index(name="total")
        counts = counts.merge(totals, on="nro_licitacion")
        counts["p"] = counts["count"] / counts["total"]
        counts["entropy"] = -counts["p"] * np.log2(counts["p"])
        entropy_df = counts.groupby("nro_licitacion")["entropy"].sum().reset_index(name="title_entropy")
        return entropy_df

    def _pivot_presence(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pivot table: 1 if document has the title, else 0."""
        if df.empty:
            return pd.DataFrame()
        pv = df.pivot_table(
            index="nro_licitacion",
            columns="title_normalized",
            values="content_length",
            aggfunc="count",
            fill_value=0,
        )
        pv.columns = [self.make_safe_col("has_", c) for c in pv.columns]
        return (pv > 0).astype(np.int8)

    def _pivot_sum(
        self, df: pd.DataFrame, value_col: str, prefix: str
    ) -> pd.DataFrame:
        """Pivot table: sum of *value_col* grouped by title."""
        if df.empty:
            return pd.DataFrame()
        pv = df.pivot_table(
            index="nro_licitacion",
            columns="title_normalized",
            values=value_col,
            aggfunc="sum",
            fill_value=0,
        )
        pv.columns = [self.make_safe_col(prefix, c) for c in pv.columns]
        return pv

    def _pivot_agg(
        self, df: pd.DataFrame, value_col: str, agg_func, prefix: str
    ) -> pd.DataFrame:
        """Pivot table with arbitrary agg function."""
        if df.empty:
            return pd.DataFrame()
        pv = df.pivot_table(
            index="nro_licitacion",
            columns="title_normalized",
            values=value_col,
            aggfunc=agg_func,
            fill_value=0,
        )
        pv.columns = [self.make_safe_col(prefix, c) for c in pv.columns]
        return pv

    @staticmethod
    def _pivot_span_page(df: pd.DataFrame):
        """Pivot tables for line span and page range per title."""
        if df.empty:
            return pd.DataFrame(), pd.DataFrame()
        span_data = df.groupby(["nro_licitacion", "title_normalized"]).agg(
            line_span=("line_end", lambda x: x.max() - x.min()),
            page_range=("page", lambda x: x.max() - x.min()),
        ).reset_index()
        pivot_span = span_data.pivot_table(
            index="nro_licitacion", columns="title_normalized",
            values="line_span", fill_value=0,
        )
        pivot_span.columns = [DocumentFeaturesTransformer.make_safe_col("span_", c) for c in pivot_span.columns]
        pivot_page = span_data.pivot_table(
            index="nro_licitacion", columns="title_normalized",
            values="page_range", fill_value=0,
        )
        pivot_page.columns = [DocumentFeaturesTransformer.make_safe_col("page_range_", c) for c in pivot_page.columns]
        return pivot_span, pivot_page

    def _compute_otros(
        self, df_other: pd.DataFrame, all_docs: pd.Series
    ) -> pd.DataFrame:
        """Aggregate non-top-N titles into ``*_otros`` columns."""
        if df_other.empty:
            df = pd.DataFrame({"nro_licitacion": all_docs})
            df["has_otros"] = np.int8(0)
            df["len_otros"] = 0
            df["tok_otros"] = 0
            return df

        otros = df_other.groupby("nro_licitacion", sort=False).agg(
            has_otros=("content_length", "count"),
            len_otros=("content_length", "sum"),
            tok_otros=("estimated_tokens", "sum"),
        ).reset_index()
        otros["has_otros"] = (otros["has_otros"] > 0).astype(np.int8)
        return otros

    @staticmethod
    def _compute_otros_word(
        df_other: pd.DataFrame, all_docs: pd.Series
    ) -> pd.DataFrame:
        """Aggregate word_count for non-top-N titles into ``word_otros``."""
        if df_other.empty:
            df = pd.DataFrame({"nro_licitacion": all_docs})
            df["word_otros"] = 0
            return df
        otros_word = df_other.groupby("nro_licitacion", sort=False).agg(
            word_otros=("word_count", "sum"),
        ).reset_index()
        return otros_word

    @staticmethod
    def _fix_dtypes(df: pd.DataFrame) -> None:
        """Cast columns to their intended numeric types in-place."""
        int_cols = [
            "total_sections",
            "unique_titles",
            "max_content_length",
            "sum_content_length",
            "max_tokens",
            "sum_tokens",
            "max_size_bytes",
            "sum_size_bytes",
            "sum_word_count",
            "max_page",
            "year",
        ]
        for col in int_cols:
            if col in df.columns:
                df[col] = df[col].astype(int)

        float_cols = [
            "avg_content_length",
            "std_content_length",
            "avg_tokens",
            "std_tokens",
            "avg_size_bytes",
            "std_size_bytes",
            "avg_word_count",
            "avg_page",
            "gini_content_length",
            "title_entropy",
        ]
        for col in float_cols:
            if col in df.columns:
                df[col] = df[col].astype(float)

        # has_*, len_*, tok_* columns already come from pivot as int

    @staticmethod
    def _drop_zero_variance(df: pd.DataFrame) -> None:
        """Remove columns that have only one unique value (in-place)."""
        n_unique = df.select_dtypes(include=[np.number]).nunique()
        zero_var = n_unique[n_unique <= 1].index.difference(["year"])
        if not zero_var.empty:
            df.drop(columns=zero_var, inplace=True)

    @staticmethod
    def _detect_leaks(df: pd.DataFrame) -> None:
        """Log warnings about potentially redundant or leaked features.

        Checks:
        - Perfect correlation between numeric feature pairs.
        - ``sum_content_length == total_sections * avg_content_length``
          (with tolerance), which would mean the three are redundant.
        """
        import logging
        logger = logging.getLogger(__name__)

        # -- Perfect correlation -----------------------------------------
        numeric = df.select_dtypes(include=[np.number])
        if numeric.shape[1] >= 2:
            corr = numeric.corr().abs()
            upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(bool))
            high_corr = [
                (col, row, upper.loc[row, col])
                for col in upper.columns
                for row in upper.index
                if not np.isnan(upper.loc[row, col])
                and abs(upper.loc[row, col]) > 0.999
            ]
            for c1, c2, val in high_corr:
                logger.warning(
                    "Posible fuga/redudancia: %s ~ %s (r=%.4f)",
                    c1, c2, val,
                )

        # -- Redundancy: sum == count * avg -------------------------------
        required = {"total_sections", "sum_content_length", "avg_content_length"}
        if required.issubset(df.columns):
            total = df["total_sections"].values.astype(float)
            reconstructed = total * df["avg_content_length"].values
            diff = np.abs(df["sum_content_length"].values - reconstructed)
            mask = total > 0
            if mask.sum() > 0:
                max_diff = diff[mask].max()
                if max_diff < 0.01:
                    logger.warning(
                        "sum_content_length ≈ total_sections × "
                        "avg_content_length (max diff=%.4f) — columnas "
                        "redundantes.",
                        max_diff,
                    )
