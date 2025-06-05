"""Data models related to PDF processing."""

import re
import sys
from typing import List, Optional, Dict
from dataclasses import dataclass, asdict
from datetime import datetime

import pyarrow as pa


@dataclass
class PDFLine:
    """Data structure for a text line in a PDF."""

    document_id: str
    page_number: int
    line_number: int
    line_text: str
    processed_date: Optional[datetime] = None

    @staticmethod
    def get_schema() -> pa.Schema:
        """Get PyArrow schema for PDFLine."""
        return pa.schema(
            [
                ("document_id", pa.string()),
                ("page_number", pa.int32()),
                ("line_number", pa.int32()),
                ("line_text", pa.string()),
                ("processed_date", pa.timestamp("ns")),
            ]
        )

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class PDFOutline:
    """Data structure for a PDF outline."""

    document_id: str
    title: str
    page: int
    depth: int
    nro_licitacion: str
    category_id: str
    year: str
    line_number: Optional[int] = None

    @staticmethod
    def get_schema() -> pa.Schema:
        """Get PyArrow schema for PDFOutline."""
        return pa.schema(
            [
                ("document_id", pa.string()),
                ("year", pa.string()),
                ("category_id", pa.string()),
                ("nro_licitacion", pa.string()),
                ("title", pa.string()),
                ("page", pa.int32()),
                ("depth", pa.int32()),
                ("line_number", pa.int32()),
            ]
        )

    @staticmethod
    def get_outline_with_lines_schema() -> pa.Schema:
        """Get PyArrow schema for outlines with line positions."""
        return pa.schema(
            [
                ("document_id", pa.string()),
                ("year", pa.string()),
                ("category_id", pa.string()),
                ("nro_licitacion", pa.string()),
                ("title", pa.string()),
                ("page", pa.int32()),
                ("depth", pa.int32()),
                ("line_number", pa.int32()),
            ]
        )

    def to_dict(self) -> Dict:
        """Convert to dictionary."""
        return asdict(self)


@dataclass
class ContentSection:
    """Class to represent a content section extracted from a PDF."""

    title: str
    content: List[str]
    page: int
    line_start: int
    line_end: Optional[int]
    depth: int
    document_id: str
    nro_licitacion: str
    category_id: str
    year: str

    @staticmethod
    def get_schema() -> pa.Schema:
        """Get PyArrow schema for ContentSection."""
        return pa.schema(
            [
                ("document_id", pa.string()),
                ("nro_licitacion", pa.string()),
                ("category_id", pa.string()),
                ("year", pa.string()),
                ("title", pa.string()),
                ("content", pa.string()),
                ("page", pa.int32()),
                ("line_start", pa.int32()),
                ("line_end", pa.int32()),
                ("depth", pa.int32()),
                ("content_length", pa.int32()),
                ("estimated_tokens", pa.int32()),
                ("character_count", pa.int32()),
                ("size_bytes", pa.int32()),
                # ("word_count", pa.int32()),
            ]
        )

    @staticmethod
    def get_enhanced_schema() -> pa.Schema:
        """Get enhanced PyArrow schema with additional metrics."""
        return pa.schema(
            [
                ("document_id", pa.string()),
                ("nro_licitacion", pa.string()),
                ("category_id", pa.string()),
                ("year", pa.string()),
                ("title", pa.string()),
                ("content", pa.string()),
                ("page", pa.int32()),
                ("line_start", pa.int32()),
                ("line_end", pa.int32()),
                ("depth", pa.int32()),
                ("content_length", pa.int32()),
                ("estimated_tokens", pa.int32()),
                ("word_count", pa.int32()),
                ("size_bytes", pa.int32()),
            ]
        )

    def get_content_text(self) -> str:
        """Get the section content as a single string."""
        return "\n".join(self.content)

    def get_content_length(self) -> int:
        """Get the number of items in the content list."""
        return len(self.content)

    def get_line_count(self) -> int:
        """Get the number of lines in the content."""
        return len(self.content)

    def get_character_count(self) -> int:
        """Get the total number of characters in the content."""
        return sum(len(line) for line in self.content)

    def get_word_count(self) -> int:
        """Get the total number of words in the content."""
        text = self.get_content_text()
        words = re.findall(r"\b\w+\b", text)
        return len(words)

    def get_content_size_bytes(self) -> int:
        """Get the size of the content in bytes."""
        return sys.getsizeof(self.get_content_text())

    def estimate_tokens(self, chars_per_token: float = 4.0) -> int:
        """
        Estimate the number of tokens for an LLM.

        Args:
            chars_per_token: Average characters per token (default: 4.0 for most models)

        Returns:
            Estimated number of tokens
        """
        # Simple token estimation based on character count
        # Most LLMs use approximately 4 characters per token on average
        char_count = self.get_character_count()
        return int(char_count / chars_per_token)

    def estimate_tokens_gpt(self) -> int:
        """
        Estimate tokens specifically for GPT models.

        For GPT models, a common approximation is:
        - English text: ~4 chars per token
        - Code: ~3 chars per token

        Returns:
            Estimated number of tokens for GPT models
        """
        return self.estimate_tokens(chars_per_token=4.0)

    def get_section_metadata(self) -> dict:
        """
        Get a dictionary with all metadata about this section.

        Returns:
            Dictionary with metadata
        """
        return {
            "title": self.title,
            "document_id": self.document_id,
            "nro_licitacion": self.nro_licitacion,
            "category_id": self.category_id,
            "year": self.year,
            "page": self.page,
            "line_start": self.line_start,
            "line_end": self.line_end,
            "depth": self.depth,
            "line_count": self.get_line_count(),
            "character_count": self.get_character_count(),
            "word_count": self.get_word_count(),
            "size_bytes": self.get_content_size_bytes(),
            "estimated_tokens": self.estimate_tokens(),
        }

    def to_dict(self) -> Dict:
        """Convert to dictionary with computed fields."""
        result = asdict(self)
        # Add computed fields
        result["content"] = self.get_content_text()
        result["content_length"] = self.get_content_length()
        result["character_count"] = self.get_character_count()
        result["word_count"] = self.get_word_count()
        result["estimated_tokens"] = self.estimate_tokens()
        result["size_bytes"] = self.get_content_size_bytes()
        return result
