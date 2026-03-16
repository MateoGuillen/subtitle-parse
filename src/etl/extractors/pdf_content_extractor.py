"""Extractor for PDF outlines and lines."""

from typing import Tuple, Optional, List
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger
from src.core.entities.pdf_models import PDFLine, PDFOutline


class PdfContentExtractor:
    """
    Extractor for PDF content data.

    This class is responsible for loading PDF lines and outlines data from parquet files.

    Attributes:
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self):
        """Initialize the PdfContentExtractor."""
        self.logger = setup_logger(__name__)

    @error_handling(default_return=(None, None))
    def load_dataframes(
        self, pdf_lines_path: str, outlines_path: str
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Load and prepare the PDF lines and outlines DataFrames.

        Args:
            pdf_lines_path (str): Path to the PDF lines parquet file.
            outlines_path (str): Path to the outlines parquet file.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Loaded PDF lines and outlines DataFrames.
        """
        self.logger.info("Loading PDF lines from %s...", pdf_lines_path)
        pdf_lines_df = pd.read_parquet(pdf_lines_path)

        self.logger.info("Loading outlines from %s...", outlines_path)
        outlines_df = pd.read_parquet(outlines_path)

        self.logger.info(
            "Loaded %d outlines and %d PDF lines", len(outlines_df), len(pdf_lines_df)
        )

        return pdf_lines_df, outlines_df

    @error_handling(default_return=None)
    def load_single_dataframe(self, path: str) -> Optional[pd.DataFrame]:
        """
        Load a single DataFrame from a parquet file.

        Args:
            path (str): Path to the parquet file.

        Returns:
            Optional[pd.DataFrame]: Loaded DataFrame or None if there was an error.
        """
        self.logger.info("Loading data from %s...", path)
        df = pd.read_parquet(path)
        self.logger.info("Loaded %d rows from %s", len(df), path)
        return df

    @error_handling(default_return=[])
    def convert_df_to_pdflines(self, pdf_lines_df: pd.DataFrame) -> List[PDFLine]:
        """
        Convert DataFrame rows to PDFLine objects.

        Args:
            pdf_lines_df (pd.DataFrame): DataFrame with PDF lines.

        Returns:
            List[PDFLine]: List of PDFLine objects.
        """
        pdf_lines = []
        for _, row in pdf_lines_df.iterrows():
            pdf_line = PDFLine(
                document_id=row["document_id"],
                page_number=row["page_number"],
                line_number=row["line_number"],
                line_text=row["line_text"],
                processed_date=row.get("processed_date"),
            )
            pdf_lines.append(pdf_line)
        return pdf_lines

    @error_handling(default_return=[])
    def convert_df_to_pdfoutlines(self, outlines_df: pd.DataFrame) -> List[PDFOutline]:
        """
        Convert DataFrame rows to PDFOutline objects.

        Args:
            outlines_df (pd.DataFrame): DataFrame with outlines.

        Returns:
            List[PDFOutline]: List of PDFOutline objects.
        """
        pdf_outlines = []
        for _, row in outlines_df.iterrows():
            pdf_outline = PDFOutline(
                document_id=row["document_id"],
                title=row["title"],
                page=row["page"],
                depth=row["depth"],
                nro_licitacion=row["nro_licitacion"],
                category_id=row["category_id"],
                year=row["year"],
                line_number=row.get("line_number"),
            )
            pdf_outlines.append(pdf_outline)
        return pdf_outlines

    @error_handling(default_return=None)
    def save_as_parquet(
        self, df: pd.DataFrame, schema: pa.Schema, output_path: str
    ) -> None:
        """
        Save DataFrame to parquet with specified schema.

        Args:
            df (pd.DataFrame): DataFrame to save.
            schema (pa.Schema): PyArrow schema to use.
            output_path (str): Path to save the parquet file.
        """
        self.logger.info("Saving data to %s...", output_path)

        # Convert pandas DataFrame to PyArrow table using the schema
        table = pa.Table.from_pandas(df, schema=schema)

        # Write to parquet
        pq.write_table(table, output_path, compression="snappy", row_group_size=10000)

        self.logger.info("Saved %d rows to %s", len(df), output_path)

    @error_handling(default_return=None)
    def get_document_ids(self, pdf_lines_path: str) -> list:
        """Obtiene todos los document_ids únicos sin cargar el archivo completo."""

        parquet_file = pq.ParquetFile(pdf_lines_path)
        doc_ids = set()
        for batch in parquet_file.iter_batches(
            batch_size=200_000, columns=["document_id"]
        ):
            doc_ids.update(batch.column("document_id").to_pylist())
        self.logger.info("Found %d unique document_ids", len(doc_ids))
        return list(doc_ids)

    @error_handling(default_return=None)
    def load_lines_for_documents(
        self, pdf_lines_path: str, doc_ids: list
    ) -> pd.DataFrame:
        """Carga líneas solo para los document_ids especificados."""

        parquet_file = pq.ParquetFile(pdf_lines_path)
        doc_ids_set = set(doc_ids)  # lookup O(1)
        chunks = []

        for batch in parquet_file.iter_batches(
            batch_size=200_000,
            columns=["document_id", "page_number", "line_number", "line_text"],
        ):
            # Convertir a pandas y filtrar — evita problemas de tipos en PyArrow
            df = batch.to_pandas()
            filtered = df[df["document_id"].isin(doc_ids_set)]
            if not filtered.empty:
                chunks.append(filtered)

        if not chunks:
            return pd.DataFrame()

        result = pd.concat(chunks, ignore_index=True)
        # Reducir memoria
        result["page_number"] = result["page_number"].astype("int32")
        result["line_number"] = result["line_number"].astype("int32")
        return result
