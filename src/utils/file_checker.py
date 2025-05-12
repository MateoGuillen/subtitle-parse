"""FileChecker module"""

# src/utils/file_checker.py
import os
from src.utils.logging_utils import setup_logger
from typing import List, Dict


class FileChecker:
    """
    Clase utilitaria para verificar la existencia de archivos y evitar
    operaciones redundantes como descargas o extracciones.
    """

    def __init__(self):
        self.logger = setup_logger(__name__)

    def file_exists(self, file_path: str) -> bool:
        """
        Verifica si un archivo existe.

        Args:
            file_path: Ruta al archivo a verificar

        Returns:
            bool: True si el archivo existe, False en caso contrario
        """
        return os.path.isfile(file_path)

    def all_files_exist(self, file_paths: List[str]) -> bool:
        """
        Verifica si todos los archivos en una lista existen.

        Args:
            file_paths: Lista de rutas de archivos a verificar

        Returns:
            bool: True si todos los archivos existen, False si al menos uno no existe
        """
        return all(self.file_exists(file_path) for file_path in file_paths)

    def any_file_exists(self, file_paths: List[str]) -> bool:
        """
        Verifica si al menos uno de los archivos en una lista existe.

        Args:
            file_paths: Lista de rutas de archivos a verificar

        Returns:
            bool: True si al menos un archivo existe, False si ninguno existe
        """
        return any(self.file_exists(file_path) for file_path in file_paths)

    def check_files_status(self, file_paths: List[str]) -> Dict[str, bool]:
        """
        Verifica el estado de existencia de múltiples archivos.

        Args:
            file_paths: Lista de rutas de archivos a verificar

        Returns:
            Dict[str, bool]: Diccionario con la ruta como clave y un booleano indicando existencia
        """
        return {file_path: self.file_exists(file_path) for file_path in file_paths}

    def ensure_directory(self, directory_path: str) -> bool:
        """
        Asegura que un directorio exista, creándolo si es necesario.

        Args:
            directory_path: Ruta del directorio

        Returns:
            bool: True si el directorio existe o fue creado exitosamente
        """
        try:
            if not os.path.exists(directory_path):
                os.makedirs(directory_path, exist_ok=True)
                self.logger.info(f"Directorio creado: {directory_path}")
            return True
        except Exception as e:
            self.logger.error(f"Error al crear directorio {directory_path}: {str(e)}")
            return False
