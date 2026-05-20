"""Transformer for content cleaning pipeline."""

import re
import unicodedata
import logging
from typing import List
import pandas as pd
from src.utils.error_handler import error_handling


class ContentCleaningTransformer:
    _ARTIFACT_RE = re.compile(r"[\x00\ufffd\xad]+")
    _ELLIPSIS_RE = re.compile(r"\u2026")
    _WS_RE = re.compile(r"[ \t]+")

    _QUOTE_TABLE = str.maketrans({
        "\u201c": '"', "\u201d": '"', "\u201e": '"',
        "\u2018": "'", "\u2019": "'", "\u0060": "'",
        "\u2013": "-", "\u2014": "-",
    })

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _clean_line(line: str) -> str:
        line = unicodedata.normalize("NFC", line)
        line = ContentCleaningTransformer._ARTIFACT_RE.sub("", line)
        line = line.translate(ContentCleaningTransformer._QUOTE_TABLE)
        line = ContentCleaningTransformer._ELLIPSIS_RE.sub("...", line)
        line = ContentCleaningTransformer._WS_RE.sub(" ", line).strip()
        return line

    @staticmethod
    def _to_line_list(value) -> List[str]:
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, str):
            return value.split("\n")
        return []

    @staticmethod
    def normalize_title(title: str) -> str:
        if not title or not isinstance(title, str):
            return ""
        t = title.strip()
        t = unicodedata.normalize("NFC", t)
        t = unicodedata.normalize("NFD", t)
        t = "".join(c for c in t if unicodedata.category(c) != "Mn")
        t = t.lower()
        t = re.sub(r"\s+", " ", t).strip()
        return t

    @error_handling(default_return=None)
    def clean_content_field(self, df: pd.DataFrame) -> pd.DataFrame:
        self.logger.info("Cleaning content field for %d rows...", len(df))

        df["content_clean"] = df["content"].map(
            lambda v: [self._clean_line(l) for l in self._to_line_list(v)]
        )
        df["content_text"] = df["content_clean"].map(
            lambda lines: "\n".join(lines) if lines else ""
        )
        df["content_length_clean"] = df["content_clean"].map(len)
        df["title_normalized"] = df["title"].map(self.normalize_title)

        self.logger.info("Cleaning done. Processed %d rows.", len(df))
        return df
