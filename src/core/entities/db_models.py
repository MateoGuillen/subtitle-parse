"""Data models related to database operations."""

from typing import List, Optional, Dict, Any
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DocumentRecord:
    """Data structure for document information in the database."""

    document_id: str
    nro_licitacion: str
    category_id: str
    year: str
    title: str
    file_path: str
    file_size: int
    page_count: int
    processed_date: datetime
    status: str


@dataclass
class SectionRecord:
    """Data structure for section information in the database."""

    section_id: str  # Could be auto-generated
    document_id: str
    title: str
    content: str
    page: int
    line_start: int
    line_end: Optional[int]
    depth: int
    content_length: int
    vector_embedding: Optional[List[float]] = (
        None  # For future vector search capabilities
    )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for database operations."""
        return {
            "section_id": self.section_id,
            "document_id": self.document_id,
            "title": self.title,
            "content": self.content,
            "page": self.page,
            "line_start": self.line_start,
            "line_end": self.line_end if self.line_end is not None else -1,
            "depth": self.depth,
            "content_length": self.content_length,
            "vector_embedding": self.vector_embedding,
        }
