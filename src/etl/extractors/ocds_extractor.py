""" A module for extracting OCDS data."""
# src/etl/extractors/ocds_extractor.py
import os
import logging
from src.utils.file_checker import FileChecker
from src.utils.error_handler import error_handling

class OCDSExtractor:
    """A class for extracting OCDS data."""
    def __init__(self, base_url, output_dir):
        self.base_url = base_url
        self.output_dir = output_dir
        self.logger = logging.getLogger(__name__)
        self.downloader = None
        self.extractor = None
        self.file_checker = FileChecker()

    def set_dependencies(self, downloader, extractor):
        """Sets dependencies for the OCDSExtractor."""
        self.downloader = downloader
        self.extractor = extractor

    @error_handling(default_return=(None, None))
    def extract_year_data(self, year):
        """Extracts OCDS data for a specific year."""
        zip_name = f"masivo_{year}.zip"
        zip_path = os.path.join(self.output_dir, zip_name)

        csv_path = os.path.join(self.output_dir, f"ten_documents_{year}.csv")
        record_csv_path = os.path.join(self.output_dir, f"records_{year}.csv")


        if self.file_checker.all_files_exist([csv_path, record_csv_path]):
            self.logger.info("Los archivos CSV del año %s ya existen. Omitiendo extracción.", year)
            return csv_path, record_csv_path

        self.file_checker.ensure_directory(self.output_dir)

        if not self.file_checker.file_exists(zip_path):
            url = f"{self.base_url}/{year}/masivo.zip"
            self.logger.info("Descargando archivo desde %s", url)
            self.downloader.download_file(url, zip_path)

        else:
            self.logger.info("El archivo zip del año %s ya existe. Omitiendo descarga.", year)

        ten_docs_path = self.extractor.extract_file(zip_path, "ten_documents.csv", self.output_dir)
        records_path = self.extractor.extract_file(zip_path, "records.csv", self.output_dir)

        os.rename(ten_docs_path, csv_path)
        os.rename(records_path, record_csv_path)

        return csv_path, record_csv_path
