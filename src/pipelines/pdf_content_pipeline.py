"""Pipeline for extracting content from PDFs based on their outlines."""

from src.etl.extractors.pdf_content_extractor import PdfContentExtractor
from src.etl.transformers.pdf_content_transformer import PdfContentTransformer
from src.etl.loaders.pdf_content_loader import PdfContentLoader
from src.utils.logging_utils import setup_logger


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
        """
        Run the PDF content pipeline.

        This method orchestrates the entire process of extracting content
        from PDFs based on their outlines.
        """
        self.logger.info("Starting PDF content pipeline...")

        # Extract data
        pdf_lines_df, outlines_df = self.extractor.load_dataframes(
            self.config["pdf_lines_path"], self.config["outlines_path"]
        )

        if pdf_lines_df is None or outlines_df is None:
            self.logger.error("Failed to load data. Aborting pipeline.")
            return

        # Transform data - match outlines with lines
        self.logger.info("Matching outlines with lines...")
        outlines_with_position = self.transformer.match_titles_with_lines(
            outlines_df, pdf_lines_df
        )

        if outlines_with_position is None:
            self.logger.error("Failed to match outlines with lines. Aborting pipeline.")
            return

        # Save intermediate result
        self.logger.info("Saving outlines with position...")
        self.loader.save_outlines_with_lines(
            outlines_with_position,
            self.config["outlines_with_position_in_content_path"],
        )

        # Preprocess dataframes for content extraction
        self.logger.info("Preprocessing dataframes for content extraction...")
        pdf_lines_dict, sorted_outlines_df = self.transformer.preprocess_dataframes(
            pdf_lines_df, outlines_with_position
        )

        if pdf_lines_dict is None or sorted_outlines_df is None:
            self.logger.error("Failed to preprocess dataframes. Aborting pipeline.")
            return

        # Extract content sections
        self.logger.info("Extracting content sections...")
        content_sections = self.transformer.extract_content_sections(
            pdf_lines_dict, sorted_outlines_df
        )

        self.logger.info("Extracted %s content sections", len(content_sections))

        # Prepare sections dataframe
        self.logger.info("Preparing sections dataframe...")
        sections_df = self.transformer.prepare_sections_dataframe(content_sections)

        if sections_df is None:
            self.logger.error(
                "Failed to prepare sections dataframe. Aborting pipeline."
            )
            return

        # Save result
        self.logger.info("Saving content sections...")
        self.loader.save_content_sections(
            sections_df, self.config["content_sections_path"]
        )

        self.logger.info("PDF content pipeline completed successfully.")
