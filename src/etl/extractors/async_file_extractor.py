""" Asynchronous file extractor for downloading files."""
import os
import pandas as pd
from src.utils.logging_utils import setup_logger
from src.utils.file_async_downloader import FileHandler
from src.utils.error_handler import error_handling

class AsyncFileExtractor:
    """
    Class responsible for asynchronously extracting files from URLs.
    This class creates and configures the appropriate file handlers
    for different file types.
    """
    def __init__(self):
        """Initialize the AsyncFileExtractor."""
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def create_file_handler(self, config):
        """
        Creates and returns a FileHandler instance with the provided configuration.
        
        Args:
            config (dict): Configuration dictionary with the following keys:
                - file_path: Path to the CSV file containing URLs
                - output_dir: Directory where files will be stored
                - low_memory: Whether to use low memory mode for pandas
                - batch_size: Number of files to download in each batch
                - pause_time: Time to pause between batches (in seconds)
        
        Returns:
            FileHandler: Configured FileHandler instance
        """
        file_path = config.get('file_path')
        output_dir = config.get('output_dir')
        low_memory = config.get('low_memory', False)
        batch_size = config.get('batch_size', 1000)
        pause_time = config.get('pause_time', 300)

        if not file_path or not output_dir:
            self.logger.error("Missing required configuration parameters.")
            return None

        if not os.path.exists(file_path):
            self.logger.error("Input file does not exist: %s", file_path)
            return None

        os.makedirs(output_dir, exist_ok=True)

        # Create and return the file handler
        handler = FileHandler(
            file_path=file_path,
            output_dir=output_dir,
            low_memory=low_memory,
            batch_size=batch_size,
            pause_time=pause_time
        )

        self.logger.info("FileHandler created successfully.")
        return handler

    @error_handling(default_return=(False, None))
    def validate_and_load_data(self, file_path, low_memory=False):
        """
        Validates and loads data from a CSV file.
        
        Args:
            file_path (str): Path to the CSV file
            low_memory (bool): Whether to use low memory mode for pandas
        
        Returns:
            tuple: (Success status (bool), loaded DataFrame or None)
        """

        if not os.path.exists(file_path):
            self.logger.error("File does not exist: %s", file_path)
            return False, None
        # Validate required columns
        required_columns = ['tender_documents_url', 'nro_licitacion', 'categoria_id', 'date']

        # Load data
        df = pd.read_csv(file_path, low_memory=low_memory)

        # Check if all required columns exist
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error("Missing required columns in CSV: %s", missing_columns)
            return False, None

        self.logger.info("Data loaded and validated successfully from %s", file_path)
        return True, df
