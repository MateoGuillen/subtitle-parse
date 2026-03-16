"""Transformer for content cleaning pipeline."""

import re
import unicodedata
import logging
from typing import List
import pandas as pd
import numpy as np
from src.utils.error_handler import error_handling


class ContentCleaningTransformer:
    """
    Transformer that applies a minimal, safe cleaning pipeline to the raw
    ``content`` field extracted from PDF documents.

    Design rationale
    ----------------
    Only cleaning steps that are guaranteed to be lossless (or nearly so)
    are applied.  Heuristic steps that attempt to reconstruct document
    structure (line joining, title removal, separator detection) were
    intentionally excluded because they make assumptions about the layout
    that do not hold across all documents and cause context loss for the LLM.

    The philosophy is: send the LLM clean bytes, not restructured content.
    Let the LLM handle layout interpretation — it is better at that than
    regex heuristics.

    Pipeline (4 steps only):
        1. normalize_unicode     — NFC recomposition, zero information loss
        2. remove_artifacts      — strip undecodeable bytes (\\x00, \\ufffd)
        3. normalize_quotes      — typographic -> ASCII punctuation
        4. normalize_whitespace  — collapse tabs/multiple spaces

    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    # Characters that PDF parsers insert when they cannot decode a glyph.
    # \x00  — null byte (common in broken CID fonts)
    # \ufffd — Unicode replacement character
    # \xad  — soft hyphen (invisible but wastes tokens)
    _ARTIFACT_CHARS = re.compile(r"[\x00\ufffd\xad]+")

    def __init__(self):
        """Initialize the ContentCleaningTransformer."""
        self.logger = logging.getLogger(__name__)

    # ------------------------------------------------------------------
    # Step 1 — Unicode normalization
    # ------------------------------------------------------------------

    def normalize_unicode(self, line: str) -> str:
        """
        Apply NFC Unicode normalization to a single line.

        WHY: PDF extraction libraries (pdfminer, PyMuPDF) sometimes produce
        text in NFD (decomposed) form, where a character like "é" is stored
        as two code points ("e" + combining accent).  NFC re-composes these
        into a single code point.  This is lossless — no character is removed
        or altered in meaning, only its internal byte representation changes.
        It makes string comparisons and LLM tokenization consistent.

        Args:
            line (str): Raw line from PDF extraction.

        Returns:
            str: NFC-normalized line.
        """
        return unicodedata.normalize("NFC", line)

    # ------------------------------------------------------------------
    # Step 2 — Remove extraction artifacts
    # ------------------------------------------------------------------

    def remove_artifacts(self, line: str) -> str:
        """
        Strip undecodeable PDF extraction artifacts from a line.

        WHY: Null bytes (\\x00) are inserted by some PDF parsers when a glyph
        cannot be mapped to Unicode — they represent a failed decode, not
        actual content.  The replacement character (\\ufffd) is the standard
        Unicode signal for a decoding failure.  Soft hyphens (\\xad) are
        invisible formatting hints that have no semantic value in plain text.
        None of these characters carry information from the original document.

        Args:
            line (str): Line after Unicode normalization.

        Returns:
            str: Line with artifact characters removed.
        """
        return self._ARTIFACT_CHARS.sub("", line)

    # ------------------------------------------------------------------
    # Step 3 — Normalize typographic punctuation to ASCII
    # ------------------------------------------------------------------

    def normalize_quotes(self, line: str) -> str:
        """
        Normalize typographic quotes and dashes to their ASCII equivalents.

        WHY: PDFs use "smart" Unicode punctuation (curly quotes, em-dashes,
        ellipsis character) that LLM tokenizers may split into multiple tokens
        or treat differently from their ASCII equivalents.  For example,
        the opening curly quote \\u201c and the closing \\u201d are two distinct
        tokens, while ASCII " is one.  Normalizing to ASCII reduces vocabulary
        fragmentation without changing the meaning of the text at all —
        "contrato" and "contrato" mean exactly the same thing.

        Characters normalized:
            \\u201c \\u201d \\u201e  ->  "   (double quotes)
            \\u2018 \\u2019 \\u0060  ->  '   (single quotes)
            \\u2013                  ->  -   (en dash)
            \\u2014                  ->  -   (em dash)
            \\u2026                  ->  ... (ellipsis)

        Args:
            line (str): Line after artifact removal.

        Returns:
            str: Line with normalized punctuation.
        """
        line = re.sub(r"[\u201c\u201d\u201e]", '"', line)
        line = re.sub(r"[\u2018\u2019\u0060]", "'", line)
        line = re.sub(r"[\u2013\u2014]", "-", line)
        line = re.sub(r"\u2026", "...", line)
        return line

    # ------------------------------------------------------------------
    # Step 4 — Normalize whitespace
    # ------------------------------------------------------------------

    def normalize_whitespace(self, line: str) -> str:
        """
        Collapse runs of spaces and tabs into a single space.

        WHY: PDF layout engines use multiple spaces or tabs to achieve visual
        alignment (e.g. simulating table columns or indentation).  These are
        rendered faithfully by the parser but carry no semantic information
        in plain text — "hola     mundo" and "hola mundo" mean the same thing.
        Collapsing them reduces token count slightly without any information
        loss.  Leading and trailing whitespace is also stripped.

        Args:
            line (str): Line after quote normalization.

        Returns:
            str: Line with normalized internal whitespace.
        """
        return re.sub(r"[ \t]+", " ", line).strip()

    # ------------------------------------------------------------------
    # Input normalization helper
    # ------------------------------------------------------------------

    @staticmethod
    def _to_line_list(value) -> List[str]:
        """
        Normalize the ``content`` field to ``List[str]`` regardless of the
        type that PyArrow uses when deserializing from parquet.

        The extraction pipeline stored content as a string with newline (\\n)
        as the line separator between PDF lines.  PyArrow may deliver it as
        ``str``, ``list``, ``np.ndarray``, or a scalar with ``.as_py()``,
        depending on the version and saved schema.

        Args:
            value: Raw value from the ``content`` column.

        Returns:
            List[str]: List of raw lines ready for cleaning.
        """
        if value is None:
            return []
        if isinstance(value, str):
            return value.split("\n")
        if isinstance(value, list):
            return [str(v) for v in value]
        try:

            if isinstance(value, np.ndarray):
                return [str(v) for v in value.tolist()]
        except ImportError:
            pass
        if hasattr(value, "as_py"):
            py_val = value.as_py()
            if isinstance(py_val, str):
                return py_val.split("\n")
            if isinstance(py_val, list):
                return [str(v) for v in py_val]
        return []

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    @error_handling(default_return=None)
    def clean_content_list(self, raw_lines: List[str]) -> List[str]:
        """
        Apply the 4-step cleaning pipeline to a list of raw lines.

        Steps:
            1. normalize_unicode    — NFC recomposition
            2. remove_artifacts     — strip \\x00, \\ufffd, \\xad
            3. normalize_quotes     — typographic -> ASCII punctuation
            4. normalize_whitespace — collapse spaces/tabs

        No lines are added or removed — only characters within each line
        are normalized.  The structure and length of the content is preserved
        exactly as extracted, so the LLM receives the full original context.

        Args:
            raw_lines (List[str]): Raw lines from the ``content`` column.

        Returns:
            List[str]: Cleaned lines preserving full document structure.
        """
        cleaned = []
        for line in raw_lines:
            line = self.normalize_unicode(line)  # Step 1
            line = self.remove_artifacts(line)  # Step 2
            line = self.normalize_quotes(line)  # Step 3
            line = self.normalize_whitespace(line)  # Step 4
            cleaned.append(line)
        return cleaned

    # ------------------------------------------------------------------
    # DataFrame-level entry point
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Title normalization helper
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_title(title: str) -> str:
        """
        Normalize a section title for use as a filter/query key.

        WHY: The raw ``title`` field contains accented characters, mixed
        case, and extra whitespace that make querying inconsistent.
        For example, "Aclaración de los documentos" and
        "aclaracion de los documentos" should match the same filter.
        A normalized form (lowercase, no accents, collapsed spaces) allows
        reliable grouping, deduplication, and title-based filtering across
        the 2.8M sections without worrying about encoding variations.

        Transformations applied:
            1. Strip leading/trailing whitespace
            2. NFC normalize (recompose accented characters)
            3. Remove accent marks (NFD decompose + strip combining chars)
            4. Lowercase
            5. Collapse multiple spaces into one

        Examples:
            "Aclaración de los documentos"  ->  "aclaracion de los documentos"
            "OBJETO DE LA CONTRATACION"     ->  "objeto de la contratacion"
            "  Precio  y  formulario  "     ->  "precio y formulario"

        Args:
            title (str): Raw title value from the ``title`` column.

        Returns:
            str: Normalized title suitable for filtering and grouping.
        """
        if not title or not isinstance(title, str):
            return ""
        t = title.strip()
        t = unicodedata.normalize("NFC", t)
        # NFD decompose then strip combining (accent) characters
        t = unicodedata.normalize("NFD", t)
        t = "".join(c for c in t if unicodedata.category(c) != "Mn")
        t = t.lower()
        t = re.sub(r"\s+", " ", t).strip()
        return t

    # ------------------------------------------------------------------
    # DataFrame-level entry point
    # ------------------------------------------------------------------

    @error_handling(default_return=None)
    def clean_content_field(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply the cleaning pipeline to every row of a DataFrame.

        Adds four new columns:
        - ``content_clean``       : cleaned list of lines (List[str]),
                                    preserving the original line structure.
        - ``content_text``        : lines joined with newline (str),
                                    ready to pass directly to an LLM prompt.
        - ``content_length_clean``: line count (same as original since no
                                    lines are dropped), for audit purposes.
        - ``title_normalized``    : lowercase, accent-free version of ``title``
                                    for reliable filtering and grouping across
                                    all licitaciones without encoding concerns.

        WHY keep ``content_clean`` as a list: the LLM pipeline can choose
        to present lines individually or joined, depending on the prompt
        strategy used for structured extraction.

        Args:
            df (pd.DataFrame): DataFrame with ``content`` and ``title`` columns.

        Returns:
            pd.DataFrame: Original DataFrame with four new columns added.
        """
        self.logger.info("Cleaning content field for %d rows...", len(df))
        df = df.copy()

        df["content_clean"] = df["content"].apply(
            lambda v: self.clean_content_list(self._to_line_list(v))
        )
        df["content_text"] = df["content_clean"].apply(
            lambda lines: "\n".join(lines) if lines else ""
        )
        df["content_length_clean"] = df["content_clean"].apply(
            lambda lines: len(lines) if lines else 0
        )
        # title_normalized al final — columna de apoyo para queries y filtros
        df["title_normalized"] = df["title"].apply(self.normalize_title)

        self.logger.info("Cleaning done. Processed %d rows.", len(df))
        return df
