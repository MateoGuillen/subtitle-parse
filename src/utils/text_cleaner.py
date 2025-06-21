"""
Utilities for cleaning and preprocessing text content from pliego documents.

This module provides text cleaning and preprocessing functionality specifically
designed for pliego documents before being sent to LLM models for structured
extraction.
"""

import re
from typing import Dict
from unicodedata import normalize
import logging


class TextCleaner:
    """
    A utility class for cleaning and preprocessing text content from pliego documents.

    This class handles various text preprocessing tasks including:
    - Removing excessive whitespace and formatting artifacts
    - Normalizing Unicode characters
    - Cleaning HTML/XML tags if present
    - Removing or standardizing special characters
    - Handling encoding issues
    - Truncating text to fit within token limits
    """

    def __init__(self, max_content_length: int = 12000):
        """
        Initialize the TextCleaner.

        Args:
            max_content_length (int): Maximum length of content to process
        """
        self.max_content_length = max_content_length
        self.logger = logging.getLogger(__name__)

        # Common patterns found in pliego documents
        self.html_pattern = re.compile(r"<[^>]+>")
        self.excessive_whitespace = re.compile(r"\s+")
        self.line_breaks = re.compile(r"\n{3,}")
        self.bullet_points = re.compile(r"^\s*[-•·]\s*", re.MULTILINE)
        self.page_numbers = re.compile(r"^\s*\d+\s*$", re.MULTILINE)
        self.headers_footers = re.compile(
            r"^(página|page|pág\.?)\s*\d+.*$", re.MULTILINE | re.IGNORECASE
        )

        # Patterns for common pliego artifacts
        self.table_separators = re.compile(r"[-_=]{3,}")
        self.email_pattern = re.compile(
            r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
        )
        self.phone_pattern = re.compile(r"\b\d{3,4}[-.\s]?\d{3,4}[-.\s]?\d{3,4}\b")
        self.currency_pattern = re.compile(
            r"[Gg]s\.?\s*\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})?"
        )

    def clean(self, text: str) -> str:
        """
        Main cleaning method that applies all cleaning transformations.

        Args:
            text (str): Raw text content from pliego document

        Returns:
            str: Cleaned and preprocessed text
        """
        if not text or not isinstance(text, str):
            return ""

        try:
            # Step 1: Basic text normalization
            cleaned_text = self._normalize_unicode(text)

            # Step 2: Remove HTML/XML tags
            cleaned_text = self._remove_html_tags(cleaned_text)

            # Step 3: Clean whitespace and formatting
            cleaned_text = self._clean_whitespace(cleaned_text)

            # Step 4: Remove page artifacts
            cleaned_text = self._remove_page_artifacts(cleaned_text)

            # Step 5: Standardize formatting
            cleaned_text = self._standardize_formatting(cleaned_text)

            # Step 6: Truncate if too long
            cleaned_text = self._truncate_content(cleaned_text)

            # Step 7: Final cleanup
            cleaned_text = self._final_cleanup(cleaned_text)

            return cleaned_text.strip()

        except Exception as e:
            self.logger.error(f"Error cleaning text: {str(e)}")
            return (
                text[: self.max_content_length]
                if len(text) > self.max_content_length
                else text
            )

    def _normalize_unicode(self, text: str) -> str:
        """Normalize Unicode characters to standard forms."""
        # Normalize Unicode to NFD (decomposed) then to NFC (composed)
        text = normalize("NFD", text)
        text = normalize("NFC", text)

        # Replace common problematic characters
        replacements = {
            '"': '"',  # Left double quotation mark
            '"': '"',  # Right double quotation mark
            """: "'",  # Left single quotation mark
            """: "'",  # Right single quotation mark
            "–": "-",  # En dash
            "—": "-",  # Em dash
            "…": "...",  # Horizontal ellipsis
            "\u00a0": " ",  # Non-breaking space
            "\u2028": "\n",  # Line separator
            "\u2029": "\n\n",  # Paragraph separator
        }

        for old, new in replacements.items():
            text = text.replace(old, new)

        return text

    def _remove_html_tags(self, text: str) -> str:
        """Remove HTML/XML tags from text."""
        # Remove HTML tags but preserve content
        text = self.html_pattern.sub(" ", text)

        # Remove common HTML entities
        html_entities = {
            "&nbsp;": " ",
            "&amp;": "&",
            "&lt;": "<",
            "&gt;": ">",
            "&quot;": '"',
            "&#39;": "'",
        }

        for entity, replacement in html_entities.items():
            text = text.replace(entity, replacement)

        return text

    def _clean_whitespace(self, text: str) -> str:
        """Clean excessive whitespace and normalize line breaks."""
        # Replace multiple spaces with single space
        text = self.excessive_whitespace.sub(" ", text)

        # Replace multiple line breaks with maximum of 2
        text = self.line_breaks.sub("\n\n", text)

        # Clean up lines that are just whitespace
        lines = text.split("\n")
        cleaned_lines = []

        for line in lines:
            line = line.strip()
            if line:  # Only keep non-empty lines
                cleaned_lines.append(line)
            elif (
                cleaned_lines and cleaned_lines[-1]
            ):  # Keep one empty line after content
                cleaned_lines.append("")

        return "\n".join(cleaned_lines)

    def _remove_page_artifacts(self, text: str) -> str:
        """Remove page numbers, headers, and footers."""
        # Remove standalone page numbers
        text = self.page_numbers.sub("", text)

        # Remove page headers/footers
        text = self.headers_footers.sub("", text)

        # Remove table separators
        text = self.table_separators.sub("", text)

        return text

    def _standardize_formatting(self, text: str) -> str:
        """Standardize bullet points and formatting."""
        # Standardize bullet points
        text = self.bullet_points.sub("• ", text)

        # Standardize currency notation (preserve but clean)
        # This helps LLMs understand monetary values better
        def clean_currency(match):
            return match.group().replace(",", "").replace(".", "")

        # Keep currency patterns but clean them
        text = self.currency_pattern.sub(clean_currency, text)

        return text

    def _truncate_content(self, text: str) -> str:
        """Truncate content if it exceeds maximum length."""
        if len(text) <= self.max_content_length:
            return text

        # Truncate at sentence boundary if possible
        truncated = text[: self.max_content_length]

        # Try to find last sentence ending
        last_sentence = max(
            truncated.rfind("."), truncated.rfind("!"), truncated.rfind("?")
        )

        if (
            last_sentence > self.max_content_length * 0.8
        ):  # If we can keep 80% of content
            return truncated[: last_sentence + 1]

        # Otherwise, truncate at word boundary
        last_space = truncated.rfind(" ")
        if last_space > self.max_content_length * 0.9:
            return truncated[:last_space] + "..."

        return truncated + "..."

    def _final_cleanup(self, text: str) -> str:
        """Final cleanup pass."""
        # Remove any remaining multiple spaces
        text = re.sub(r" +", " ", text)

        # Clean up line breaks at start/end
        text = text.strip("\n")

        # Ensure we don't have empty content
        if not text.strip():
            return ""

        return text

    def extract_key_sections(self, text: str) -> Dict[str, str]:
        """
        Extract key sections from pliego text for better LLM processing.

        Args:
            text (str): Cleaned text content

        Returns:
            Dict[str, str]: Dictionary with identified sections
        """
        sections = {}

        # Common section patterns in pliegos
        section_patterns = {
            "objetivos": r"(objetivo[s]?|finalidad|propósito)[\s\S]*?(?=\n\n|\Z)",
            "requisitos": r"(requisito[s]?|requerimiento[s]?)[\s\S]*?(?=\n\n|\Z)",
            "criterios": r"(criterio[s]?|evaluación|puntaje)[\s\S]*?(?=\n\n|\Z)",
            "garantias": r"(garantía[s]?|seguro[s]?)[\s\S]*?(?=\n\n|\Z)",
            "plazos": r"(plazo[s]?|cronograma|tiempo)[\s\S]*?(?=\n\n|\Z)",
            "presupuesto": r"(presupuesto|precio|costo|monto)[\s\S]*?(?=\n\n|\Z)",
        }

        for section_name, pattern in section_patterns.items():
            matches = re.findall(pattern, text, re.IGNORECASE | re.MULTILINE)
            if matches:
                sections[section_name] = matches[0]

        return sections

    def preserve_structured_data(self, text: str) -> str:
        """
        Preserve structured data like tables, lists, and monetary values.

        Args:
            text (str): Text content

        Returns:
            str: Text with preserved structured elements
        """
        # Preserve email addresses
        emails = self.email_pattern.findall(text)

        # Preserve phone numbers
        phones = self.phone_pattern.findall(text)

        # Preserve currency amounts
        currencies = self.currency_pattern.findall(text)

        # Store these in a structured way that LLMs can better understand
        preserved_data = {"emails": emails, "phones": phones, "currencies": currencies}

        # Add structured data note if any found
        if any(preserved_data.values()):
            structured_note = "\n\n[DATOS ESTRUCTURADOS IDENTIFICADOS]\n"
            for data_type, values in preserved_data.items():
                if values:
                    structured_note += f"{data_type.upper()}: {', '.join(values)}\n"
            text += structured_note

        return text

    def get_content_stats(self, text: str) -> Dict[str, int]:
        """
        Get statistics about the content for monitoring purposes.

        Args:
            text (str): Text content

        Returns:
            Dict[str, int]: Content statistics
        """
        return {
            "character_count": len(text),
            "word_count": len(text.split()),
            "line_count": len(text.split("\n")),
            "paragraph_count": len([p for p in text.split("\n\n") if p.strip()]),
            "email_count": len(self.email_pattern.findall(text)),
            "phone_count": len(self.phone_pattern.findall(text)),
            "currency_count": len(self.currency_pattern.findall(text)),
        }
