"""A module for extracting files from a zip archive."""
# src/etl/extractors/file_extractor.py

import os
import zipfile
from src.core.exceptions.exceptions import ExtractionError
from src.utils.error_handler import error_handling
from src.utils.logging_utils import setup_logger

class FileExtractor:
    """A class for extracting files from a zip archive."""

    def __init__(self):
        self.logger = setup_logger(__name__)

    @error_handling(default_return=None)
    def extract_file(self, zip_path, target_file, output_path):
        """Extracts a file from a zip archive."""
        self.logger.info("Extrayendo archivo %s de %s", target_file, zip_path)
        os.makedirs(output_path, exist_ok=True)

        if not zipfile.is_zipfile(zip_path):
            raise ExtractionError(zip_path, "El archivo no es un archivo ZIP válido o está corrupto.")

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            file_list = zip_ref.namelist()

            if target_file not in file_list:
                raise ExtractionError(zip_path, f"{target_file} no encontrado en {zip_path}")

            zip_ref.extract(target_file, output_path)
            self.logger.info("Archivo extraído a %s", output_path)

        return os.path.join(output_path, target_file)
