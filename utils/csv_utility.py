import os
import pandas as pd


class CSVUtility:
    """
    Clase que proporciona utilidades para trabajar con archivos CSV, como asegurar
    la existencia de directorios y combinar archivos CSV de categoría específica.
    """
    
    @staticmethod
    def ensure_directory_exists(directory_path):
        """
        Crea el directorio si no existe.
        
        Parámetros:
            directory_path (str): Ruta del directorio que se desea verificar o crear.
        """
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            print(f"Directorio creado: {directory_path}")
        else:
            print(f"El directorio ya existe: {directory_path}")

    @staticmethod
    def combine_csv_by_category(directory_path, output_file, category):
        """
        Combina archivos CSV de una categoría específica en un directorio y elimina duplicados.
        
        Parámetros:
            directory_path (str): Ruta al directorio que contiene los archivos CSV.
            output_file (str): Ruta para guardar el archivo CSV combinado.
            category (str): Categoría de los archivos CSV que se desea combinar 
                            (por ejemplo: '1' o '2').
        """
        combined_df = pd.DataFrame()  # DataFrame vacío para combinar los archivos

        # Iterar sobre los archivos en el directorio
        for file_name in os.listdir(directory_path):
            if file_name.endswith(f"_{category}.csv"):  
                file_path = os.path.join(directory_path, file_name)
                print(f"Procesando archivo: {file_name}")
                df = pd.read_csv(file_path)  
                combined_df = pd.concat([combined_df, df], ignore_index=True)  

        # Eliminar duplicados basándose en todas las columnas
        combined_df = combined_df.drop_duplicates()

        # Guardar el archivo combinado
        combined_df.to_csv(output_file, index=False)
        print(f"Archivos combinados y guardados en {output_file}")