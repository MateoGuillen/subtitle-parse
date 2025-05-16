"""Transformer for PDF content data."""
import re
import logging
from typing import Tuple, List, Dict, Optional
from dataclasses import dataclass
from collections import defaultdict
import pandas as pd
from src.utils.error_handler import error_handling

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

class PdfContentTransformer:
    """
    Transformer for PDF content data.
    
    This class is responsible for transforming PDF lines and outlines data,
    matching outlines with lines, and extracting content sections.
    
    Attributes:
        logger (logging.Logger): Logger for this class.
        input_external_dir (str): Directory for external input data.
    """

    def __init__(self):
        """Initialize the PdfContentTransformer."""
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def clean_text(texto: str) -> str:
        """
        Clean duplicate text and replace hyphens.
        
        Args:
            texto (str): Text to clean.
            
        Returns:
            str: Cleaned text.
        """
        if pd.isna(texto):
            return texto
        return re.sub(r"\s+", " ", texto).strip()  # Clean extra spaces

    @error_handling(default_return=None)
    def match_titles_with_lines(
        self, outlines_df: pd.DataFrame, pdf_lines_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Match outline titles with their corresponding line numbers using DataFrame merge.
        
        Args:
            outlines_df (pd.DataFrame): DataFrame with outlines.
            pdf_lines_df (pd.DataFrame): DataFrame with PDF lines.
            
        Returns:
            pd.DataFrame: DataFrame with matched outlines and lines.
        """
        # Clean both title and line_text
        self.logger.info("Cleaning text fields...")
        outlines_df["clean_title"] = outlines_df["title"].apply(self.clean_text)
        pdf_lines_df["clean_line_text"] = pdf_lines_df["line_text"].apply(self.clean_text)

        # Perform the merge using cleaned fields
        self.logger.info("Merging datasets...")
        merged_df = pd.merge(
            outlines_df,
            pdf_lines_df[
                [
                    "document_id",
                    "page_number",
                    "line_text",
                    "clean_line_text",
                    "line_number",
                ]
            ],
            left_on=["document_id", "page", "clean_title"],
            right_on=["document_id", "page_number", "clean_line_text"],
            how="left",
        )

        # Drop temporary and duplicate columns
        merged_df = merged_df.drop(
            ["page_number", "line_text", "clean_title", "clean_line_text"], axis=1
        )

        # Print summary
        total_matches = merged_df["line_number"].notna().sum()
        match_rate = (total_matches/len(merged_df))*100 if len(merged_df) > 0 else 0
        # self.logger.info(f"Total outline entries: {len(merged_df)}")
        self.logger.info("Total outline entries: {}".format(len(merged_df)))
        # self.logger.info(f"Successfully matched: {total_matches}")
        self.logger.info("Successfully matched: {}".format(total_matches))
        # self.logger.info(f"Match rate: {match_rate:.2f}%")
        self.logger.info("Match rate: {:.2f}%".format(match_rate))

        return merged_df

    @error_handling(default_return=(None, None))
    def preprocess_dataframes(
        self, pdf_lines_df: pd.DataFrame, outlines_df: pd.DataFrame
    ) -> Tuple[Dict, pd.DataFrame]:
        """
        Preprocess dataframes to optimize content extraction.
        
        Args:
            pdf_lines_df (pd.DataFrame): DataFrame with PDF lines.
            outlines_df (pd.DataFrame): DataFrame with outlines.
            
        Returns:
            Tuple[Dict, pd.DataFrame]: Preprocessed data.
        """
        # Create a dictionary for quick access to PDF lines
        pdf_lines_dict = defaultdict(lambda: defaultdict(list))

        # Group PDF lines by document_id and page_number
        for _, row in pdf_lines_df.iterrows():
            pdf_lines_dict[row["document_id"]][row["page_number"]].append(
                {"line_number": row["line_number"], "line_text": row["line_text"]}
            )

        # Sort lines within each page
        for doc_id in pdf_lines_dict:
            for page in pdf_lines_dict[doc_id]:
                pdf_lines_dict[doc_id][page].sort(key=lambda x: x["line_number"])

        # Sort outlines
        outlines_df = outlines_df.sort_values(["document_id", "page", "line_number"])

        return pdf_lines_dict, outlines_df

    @error_handling(default_return=[])
    def get_section_content(
        self,
        pdf_lines_dict: Dict,
        doc_id: str,
        start_page: int,
        end_page: int,
        start_line: int,
        end_line: Optional[int],
    ) -> List[str]:
        """
        Extract content between two points in the PDF using preprocessed dictionary.
        
        Args:
            pdf_lines_dict (Dict): Dictionary with PDF lines.
            doc_id (str): Document ID.
            start_page (int): Start page.
            end_page (int): End page.
            start_line (int): Start line.
            end_line (Optional[int]): End line.
            
        Returns:
            List[str]: Extracted content.
        """
        content = []

        for page in range(start_page, end_page + 1):
            if page not in pdf_lines_dict[doc_id]:
                continue

            page_lines = pdf_lines_dict[doc_id][page]

            for line in page_lines:
                line_num = line["line_number"]

                if page == start_page and line_num < start_line:
                    continue
                if page == end_page and end_line is not None and line_num >= end_line:
                    break

                content.append(line["line_text"])

        return content

    @error_handling(default_return=[])
    def extract_content_sections(
        self,
        pdf_lines_dict: Dict,
        outlines_df: pd.DataFrame
    ) -> List[ContentSection]:
        """
        Extract content sections using preprocessed data.
        
        Args:
            pdf_lines_dict (Dict): Dictionary with PDF lines.
            outlines_df (pd.DataFrame): DataFrame with outlines.
            
        Returns:
            List[ContentSection]: Extracted content sections.
        """
        sections = []

        # Process each document's outlines
        for doc_id, doc_outlines in outlines_df.groupby("document_id"):
            doc_rows = doc_outlines.to_dict("records")

            for i, current in enumerate(doc_rows):
                # Skip entries with no line number
                if pd.isna(current["line_number"]):
                    continue

                # Determine section end
                if i < len(doc_rows) - 1:
                    next_outline = doc_rows[i + 1]
                    # If next outline is on the same page and has a valid line number
                    if next_outline["page"] == current["page"] and not pd.isna(next_outline["line_number"]):
                        end_page = next_outline["page"]
                        end_line = next_outline["line_number"]
                    else:
                        # If next outline is on different page or has no line number
                        end_page = current["page"] + 1
                        end_line = None
                else:
                    # For last section, use next page
                    end_page = current["page"] + 1
                    end_line = None

                content = self.get_section_content(
                    pdf_lines_dict,
                    doc_id,
                    current["page"],
                    end_page,
                    int(current["line_number"]),
                    end_line,
                )

                section = ContentSection(
                    title=current["title"],
                    content=content,
                    page=current["page"],
                    line_start=int(current["line_number"]),
                    line_end=end_line if end_line is not None else None,
                    depth=current["depth"],
                    document_id=doc_id,
                    nro_licitacion=current["nro_licitacion"],
                    category_id=current["category_id"],
                    year=current["year"],
                )
                sections.append(section)

        return sections

    @error_handling(default_return=None)
    def prepare_sections_dataframe(self, sections: List[ContentSection]) -> pd.DataFrame:
        """
        Prepare DataFrame from content sections.
        
        Args:
            sections (List[ContentSection]): Content sections.
            
        Returns:
            pd.DataFrame: DataFrame with content sections.
        """
        # Create DataFrame directly from a list of dictionaries for better performance
        sections_data = [
            {
                "document_id": section.document_id,
                "nro_licitacion": section.nro_licitacion,
                "category_id": section.category_id,
                "year": section.year,
                "title": section.title,
                "content": "\n".join(section.content),
                "page": section.page,
                "line_start": section.line_start,
                "line_end": section.line_end if section.line_end is not None else -1,
                "depth": section.depth,
                "content_length": len(section.content),
            }
            for section in sections
        ]

        return pd.DataFrame(sections_data)
