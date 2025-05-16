"""A pipeline for extracting and processing content from PDF documents."""
import pandas as pd
from src.etl.extractors.file_content_extractor import PdfContentExtractor
from src.etl.transformers.pdf_content_transformer import PdfContentTransformer
from src.etl.loaders.pdf_content_loader import PdfContentLoader
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from src.utils.file_checker import FileChecker

class PdfContentPipeline:
    """
    A pipeline for extracting content from PDF documents using outlines and line information.

    This pipeline processes PDF outlines, matches them with line numbers in the PDF,
    and extracts content for each section defined by the outlines.

    Attributes:
        config (dict): Configuration dictionary.
        pdf_lines_path (str): Path to the parquet file containing PDF lines.
        outlines_path (str): Path to the parquet file containing PDF outlines.
        outlines_with_lines_path (str): Path for the output file with matched outlines and lines.
        content_sections_path (str): Path for the output file with extracted content sections.
        logger (logging.Logger): Logger object for logging messages.
    """
    def __init__(self, config):
        """
        Initialize the PdfContentPipeline.
        
        Args:
            config (dict): Configuration dictionary containing file paths.
        """
        self.config = config
        self.pdf_lines_path = config.get('pdf_lines_path')
        self.outlines_path = config.get('outlines_path')
        self.outlines_with_lines_path = config.get('outlines_with_lines_path')
        self.content_sections_path = config.get('content_sections_path')
        self.logger = setup_logger(__name__)
        self._initialize_components()

    def _initialize_components(self):
        """Initialize the components of the pipeline."""
        # ETL components
        self.extractor = PdfContentExtractor()
        self.transformer = PdfContentTransformer()
        self.loader = PdfContentLoader()

        # Utility components
        self.file_checker = FileChecker()

    @error_handling(default_return=None)
    def match_titles_with_lines(self) -> pd.DataFrame:
        """
        Match outline titles with their corresponding line numbers.
        
        Returns:
            pd.DataFrame: DataFrame with matched outlines and lines.
        """
        # Check if output file already exists
        if self.file_checker.file_exists(self.outlines_with_lines_path):
            self.logger.info(
                "Output file %s already exists. Loading...", self.outlines_with_lines_path
            )
            return self.extractor.load_single_dataframe(
                self.outlines_with_lines_path
            )

        # Extract: Load DataFrames
        outlines_df, pdf_lines_df = self.extractor.load_dataframes(
            self.outlines_path, self.pdf_lines_path
        )
        if outlines_df is None or pdf_lines_df is None:
            self.logger.error("Failed to load DataFrames for matching titles with lines.")
            return None

        # Transform: Match titles with lines
        merged_df = self.transformer.match_titles_with_lines(outlines_df, pdf_lines_df)
        if merged_df is None:
            self.logger.error("Failed to match titles with lines.")
            return None

        # Load: Save matched outlines with lines
        result_df = self.loader.save_outlines_with_lines(merged_df, self.outlines_with_lines_path)

        return result_df

    @error_handling(default_return=None)
    def extract_content(self) -> pd.DataFrame:
        """
        Extract content for each outline section.
        
        Returns:
            pd.DataFrame: DataFrame with extracted content.
        """
        # Check if output file already exists
        if self.file_checker.file_exists(self.content_sections_path):
            self.logger.info(
                "Output file  %s already exists. Loading...", self.content_sections_path
            )
            return self.extractor.load_single_dataframe(self.content_sections_path)

        # Extract: Load DataFrames
        pdf_lines_df, outlines_with_lines_df = self.extractor.load_dataframes(
            self.pdf_lines_path, self.outlines_with_lines_path
        )
        if pdf_lines_df is None or outlines_with_lines_df is None:
            self.logger.error("Failed to load DataFrames for content extraction.")
            return None

        # Transform: Preprocess data and extract content sections
        pdf_lines_dict, outlines_with_lines_df = self.transformer.preprocess_dataframes(
            pdf_lines_df, outlines_with_lines_df
        )
        if pdf_lines_dict is None or outlines_with_lines_df is None:
            self.logger.error("Failed to preprocess DataFrames.")
            return None

        sections = self.transformer.extract_content_sections(pdf_lines_dict, outlines_with_lines_df)
        if not sections:
            self.logger.error("Failed to extract content sections.")
            return None

        sections_df = self.transformer.prepare_sections_dataframe(sections)
        if sections_df is None:
            self.logger.error("Failed to prepare sections DataFrame.")
            return None

        # Load: Save content sections
        result_df = self.loader.save_content_sections(sections_df, self.content_sections_path)

        return result_df

    def run(self):
        """Run the content extraction pipeline."""
        self.logger.info("Starting content extraction pipeline...")

        # Step 1: Match titles with lines
        self.logger.info("Step 1: Matching titles with lines...")
        outlines_with_lines_df = self.match_titles_with_lines()
        if outlines_with_lines_df is None:
            self.logger.error("Failed to match titles with lines. Pipeline terminated.")
            return False

        # Step 2: Extract content for each section
        self.logger.info("Step 2: Extracting content for each section...")
        content_sections_df = self.extract_content()
        if content_sections_df is None:
            self.logger.error("Failed to extract content. Pipeline terminated.")
            return False

        self.logger.info("Content extraction pipeline completed successfully.")
        return True
