import os
class FileUtility:
    @staticmethod
    def check_file_generation(file_path):
        if file_path and os.path.exists(file_path):
            print(f"Archivo combinado generado correctamente: {file_path}")
            return True
        else:
            print(f"Error: No se encontró el archivo combinado: {file_path}")
            return False

    @staticmethod
    def ensure_directory_exists(directory_path):
        """
        Crea el directorio si no existe.
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
