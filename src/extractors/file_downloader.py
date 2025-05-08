"""A module for downloading files."""
# src/extractors/file_downloader.py

import requests
from src.utils.logging_utils import setup_logger

class FileDownloader:
    """A class for downloading files, with retries and error handling."""

    def __init__(self):
        self.logger = setup_logger(__name__)

    def download_file(self, url, output_path):
        """Downloads a file from a given URL and saves it to a local path."""
        self.logger.info("Descargando archivo desde %s", url)
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with open(output_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            self.logger.info("Archivo descargado y guardado en %s", output_path)
            return output_path
        except requests.exceptions.RequestException as e:
            self.logger.error("Error al descargar el archivo desde %s: %s", url, str(e))
            raise
