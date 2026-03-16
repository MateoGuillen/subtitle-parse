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

    # def run(self):
    #     self.logger.info("Starting PDF content pipeline...")

    #     # Cargar outlines completo (es más pequeño)
    #     outlines_df = self.extractor.load_single_dataframe(self.config["outlines_path"])
    #     if outlines_df is None:
    #         self.logger.error("Failed to load outlines. Aborting pipeline.")
    #         return

    #     # Agrupar document_ids en lotes para procesar de a N documentos
    #     all_doc_ids = outlines_df["document_id"].unique().tolist()
    #     self.logger.info("Total documents to process: %d", len(all_doc_ids))

    #     BATCH_SIZE = 200  # Ajusta según tu RAM disponible
    #     all_sections = []
    #     all_outlines_with_position = []

    #     for i in range(0, len(all_doc_ids), BATCH_SIZE):
    #         batch_doc_ids = all_doc_ids[i : i + BATCH_SIZE]
    #         self.logger.info(
    #             "Processing batch %d/%d (docs %d-%d)...",
    #             i // BATCH_SIZE + 1,
    #             (len(all_doc_ids) + BATCH_SIZE - 1) // BATCH_SIZE,
    #             i,
    #             min(i + BATCH_SIZE, len(all_doc_ids)),
    #         )

    #         # Cargar solo las líneas de los documentos en este batch
    #         pdf_lines_chunk = self.extractor.load_lines_for_documents(
    #             self.config["pdf_lines_path"], batch_doc_ids
    #         )
    #         if pdf_lines_chunk.empty:
    #             self.logger.warning("No lines found for batch, skipping...")
    #             continue

    #         outlines_chunk = outlines_df[
    #             outlines_df["document_id"].isin(batch_doc_ids)
    #         ].copy()

    #         # Match outlines con líneas
    #         outlines_with_position = self.transformer.match_titles_with_lines(
    #             outlines_chunk, pdf_lines_chunk
    #         )
    #         if outlines_with_position is None:
    #             continue

    #         matched = outlines_with_position["line_number"].notna().sum()
    #         self.logger.info(
    #             "Matched %d/%d outlines in batch", matched, len(outlines_with_position)
    #         )

    #         all_outlines_with_position.append(outlines_with_position)

    #         # Preprocesar y extraer secciones
    #         pdf_lines_dict, sorted_outlines_df = self.transformer.preprocess_dataframes(
    #             pdf_lines_chunk, outlines_with_position
    #         )
    #         if pdf_lines_dict is None:
    #             continue

    #         sections = self.transformer.extract_content_sections(
    #             pdf_lines_dict, sorted_outlines_df
    #         )
    #         all_sections.extend(sections)
    #         self.logger.info("Total sections extracted so far: %d", len(all_sections))

    #     if not all_sections:
    #         self.logger.error(
    #             "No sections extracted. Check document_id types and column names."
    #         )
    #         return

    #     # Guardar resultados

    #     merged_outlines = pd.concat(all_outlines_with_position, ignore_index=True)
    #     self.loader.save_outlines_with_lines(
    #         merged_outlines, self.config["outlines_with_position_in_content_path"]
    #     )

    #     sections_df = self.transformer.prepare_sections_dataframe(all_sections)
    #     if sections_df is not None:
    #         self.loader.save_content_sections(
    #             sections_df, self.config["content_sections_path"]
    #         )

    #     self.logger.info("PDF content pipeline completed successfully.")

    def run(self):
        self.logger.info("Starting PDF content pipeline...")

        outlines_df = self.extractor.load_single_dataframe(self.config["outlines_path"])
        if outlines_df is None:
            return

        self.logger.info("Loading PDF lines into memory (once)...")
        pdf_lines_df = self.extractor.load_single_dataframe(
            self.config["pdf_lines_path"]
        )
        pdf_lines_df["page_number"] = pdf_lines_df["page_number"].astype("int32")
        pdf_lines_df["line_number"] = pdf_lines_df["line_number"].astype("int32")

        self.logger.info("Pre-grouping lines by document_id...")
        pdf_lines_by_doc = {
            doc_id: group.reset_index(drop=True)
            for doc_id, group in pdf_lines_df.groupby("document_id")
        }
        del pdf_lines_df

        all_doc_ids = outlines_df["document_id"].unique().tolist()
        self.logger.info("Total documents to process: %d", len(all_doc_ids))

        batch_size = 1000

        outlines_schema = PDFOutline.get_outline_with_lines_schema()
        os.makedirs(
            os.path.dirname(self.config["outlines_with_position_in_content_path"]),
            exist_ok=True,
        )
        os.makedirs(
            self.config["content_sections_dir"], exist_ok=True
        )  # ✅ directorio base para particiones

        total_sections = 0

        with pq.ParquetWriter(
            self.config["outlines_with_position_in_content_path"],
            outlines_schema,
            compression="snappy",
        ) as outlines_writer:

            for i in range(0, len(all_doc_ids), batch_size):
                batch_doc_ids = all_doc_ids[i : i + batch_size]
                self.logger.info(
                    "Processing batch %d/%d (docs %d-%d)...",
                    i // batch_size + 1,
                    (len(all_doc_ids) + batch_size - 1) // batch_size,
                    i,
                    min(i + batch_size, len(all_doc_ids)),
                )

                chunks = [
                    pdf_lines_by_doc[d] for d in batch_doc_ids if d in pdf_lines_by_doc
                ]
                if not chunks:
                    self.logger.warning("No lines found for batch, skipping...")
                    continue

                pdf_lines_chunk = pd.concat(chunks, ignore_index=True)
                outlines_chunk = outlines_df[
                    outlines_df["document_id"].isin(batch_doc_ids)
                ].copy()

                outlines_with_position = self.transformer.match_titles_with_lines(
                    outlines_chunk, pdf_lines_chunk
                )
                if outlines_with_position is None:
                    continue

                matched = outlines_with_position["line_number"].notna().sum()
                self.logger.info(
                    "Matched %d/%d outlines in batch",
                    matched,
                    len(outlines_with_position),
                )
                pdf_lines_dict, sorted_outlines_df = (
                    self.transformer.preprocess_dataframes(
                        pdf_lines_chunk, outlines_with_position
                    )
                )
                del pdf_lines_chunk

                outlines_table = pa.Table.from_pandas(
                    outlines_with_position, schema=outlines_schema
                )
                outlines_writer.write_table(outlines_table)
                del outlines_with_position, outlines_table

                if pdf_lines_dict is None:
                    continue

                sections = self.transformer.extract_content_sections(
                    pdf_lines_dict, sorted_outlines_df
                )
                del pdf_lines_dict, sorted_outlines_df

                if not sections:
                    continue

                sections_df = self.transformer.prepare_sections_dataframe(sections)
                del sections

                if sections_df is not None:
                    # ✅ Escribir particionado por año, batch a batch
                    self.loader.save_content_sections_partitioned(
                        sections_df, self.config["content_sections_dir"]
                    )
                    total_sections += len(sections_df)
                    del sections_df

                self.logger.info("Total sections written so far: %d", total_sections)

        self.logger.info("PDF content pipeline completed successfully.")
        self.logger.info("Total sections written: %d", total_sections)
