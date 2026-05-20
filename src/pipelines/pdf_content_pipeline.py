"""Pipeline for extracting content from PDFs based on their outlines."""

from src.core.entities.pdf_models import PDFOutline
from src.etl.extractors.pdf_content_extractor import PdfContentExtractor
from src.etl.transformers.pdf_content_transformer import PdfContentTransformer
from src.etl.loaders.pdf_content_loader import PdfContentLoader
from src.utils.logging_utils import setup_logger
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os


class PdfContentPipeline:
    """
    Pipeline for extracting content from PDFs based on their outlines.

    This pipeline loads PDF lines and outlines data, matches outlines with lines,
    and extracts content for each section defined by the outlines.

    Attributes:
        config (dict): Configuration dictionary with file paths.
        extractor (PdfContentExtractor): Extractor component.
        transformer (PdfContentTransformer): Transformer component.
        loader (PdfContentLoader): Loader component.
        logger (logging.Logger): Logger for this class.
    """

    def __init__(self, config):
        """
        Initialize the PdfContentPipeline.

        Args:
            config (dict): Configuration dictionary with file paths.
        """
        self.config = config
        self.extractor = PdfContentExtractor()
        self.transformer = PdfContentTransformer()
        self.loader = PdfContentLoader()
        self.logger = setup_logger(__name__)

    def run(self):
        self.logger.info("Starting PDF content pipeline...")

        outlines_df = self.extractor.load_single_dataframe(self.config["outlines_path"])
        if outlines_df is None:
            return

        all_doc_ids = outlines_df["document_id"].unique().tolist()
        all_doc_ids_set = set(all_doc_ids)
        total_docs = len(all_doc_ids)
        self.logger.info("Total documents to process: %d", total_docs)

        outlines_schema = PDFOutline.get_outline_with_lines_schema()
        os.makedirs(
            os.path.dirname(self.config["outlines_with_position_in_content_path"]),
            exist_ok=True,
        )
        os.makedirs(self.config["content_sections_dir"], exist_ok=True)

        batch_size = 1000
        total_sections = 0
        line_buffers = {}
        buffer_order = []

        parquet_file = pq.ParquetFile(self.config["pdf_lines_path"])

        with pq.ParquetWriter(
            self.config["outlines_with_position_in_content_path"],
            outlines_schema,
            compression="snappy",
        ) as outlines_writer:

            for row_batch in parquet_file.iter_batches(
                batch_size=200_000,
                columns=["document_id", "page_number", "line_number", "line_text"],
            ):
                df = row_batch.to_pandas()
                mask = df["document_id"].isin(all_doc_ids_set)
                if not mask.any():
                    continue
                df = df[mask]

                for doc_id, group in df.groupby("document_id", sort=False):
                    group = group.reset_index(drop=True)
                    group["page_number"] = group["page_number"].astype("int32")
                    group["line_number"] = group["line_number"].astype("int32")

                    if doc_id in line_buffers:
                        line_buffers[doc_id] = pd.concat(
                            [line_buffers[doc_id], group], ignore_index=True
                        )
                    else:
                        line_buffers[doc_id] = group
                        buffer_order.append(doc_id)

                while len(buffer_order) >= batch_size:
                    batch_doc_ids = buffer_order[:batch_size]
                    total_sections = self._process_batch(
                        batch_doc_ids, line_buffers, outlines_df,
                        outlines_writer, total_sections
                    )
                    buffer_order = buffer_order[batch_size:]

            if buffer_order:
                total_sections = self._process_batch(
                    buffer_order, line_buffers, outlines_df,
                    outlines_writer, total_sections
                )

        self.logger.info("PDF content pipeline completed successfully.")
        self.logger.info("Total sections written: %d", total_sections)

    def _process_batch(self, batch_doc_ids, line_buffers, outlines_df, outlines_writer, total_sections):
        self.logger.info(
            "Processing batch of %d documents (docs in buffer: %d)...",
            len(batch_doc_ids), len(line_buffers),
        )

        chunks = [line_buffers[d] for d in batch_doc_ids if d in line_buffers]
        if not chunks:
            self.logger.warning("No lines found for batch, skipping...")
            for d in batch_doc_ids:
                line_buffers.pop(d, None)
            return total_sections

        pdf_lines_chunk = pd.concat(chunks, ignore_index=True)
        outlines_chunk = outlines_df[
            outlines_df["document_id"].isin(batch_doc_ids)
        ].copy()

        outlines_with_position = self.transformer.match_titles_with_lines(
            outlines_chunk, pdf_lines_chunk
        )
        if outlines_with_position is not None:
            matched = outlines_with_position["line_number"].notna().sum()
            self.logger.info(
                "Matched %d/%d outlines in batch", matched, len(outlines_with_position)
            )

            pdf_lines_dict, sorted_outlines_df = self.transformer.preprocess_dataframes(
                pdf_lines_chunk, outlines_with_position
            )

            outlines_table = pa.Table.from_pandas(
                outlines_with_position, schema=outlines_writer.schema
            )
            outlines_writer.write_table(outlines_table)

            if pdf_lines_dict is not None:
                sections = self.transformer.extract_content_sections(
                    pdf_lines_dict, sorted_outlines_df
                )

                if sections:
                    sections_df = self.transformer.prepare_sections_dataframe(sections)
                    if sections_df is not None:
                        self.loader.save_content_sections_partitioned(
                            sections_df, self.config["content_sections_dir"]
                        )
                        total_sections += len(sections_df)
                        self.logger.info(
                            "Total sections written so far: %d", total_sections
                        )

        for d in batch_doc_ids:
            line_buffers.pop(d, None)

        return total_sections
