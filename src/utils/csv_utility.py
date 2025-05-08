import os
import pandas as pd
import re
import json



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
    
    
    @staticmethod
    def combine_all_categories_excluding_duplicates(directory_path, output_file, total_categories):
        """
        Combina los archivos CSV de todas las categorías y elimina los duplicados.
        
        Parámetros:
            directory_path (str): Ruta al directorio que contiene los archivos CSV.
            output_file (str): Ruta para guardar el archivo CSV combinado de todas las categorías.
            total_categories (int): El número total de categorías para combinar.
        """
        combined_df = pd.DataFrame()  # DataFrame vacío para combinar todos los archivos

        # Iterar sobre las categorías
        for category in range(1, total_categories + 1):
            category_str = str(category)
            category_file = f"{directory_path}combined_category_{category_str}.csv"
            print(f"Procesando archivo combinado de categoría {category_str}...")
            
            # Leer el archivo combinado de cada categoría
            if os.path.exists(category_file):
                df = pd.read_csv(category_file)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            else:
                print(f"El archivo {category_file} no existe.")

        # Eliminar duplicados basándose en todas las columnas
        combined_df = combined_df.drop_duplicates()

        # Guardar el archivo combinado sin duplicados
        combined_df.to_csv(output_file, index=False)
        print(f"Archivos combinados de todas las categorías y guardados en {output_file}")
    
import os
import pandas as pd
import re


class CSVUtility:
    @staticmethod
    def combine_all_categories_excluding_duplicates(directory_path, output_file, total_categories):
        """
        Combina los archivos CSV de todas las categorías y elimina los duplicados.
        
        Args:
            directory_path (str): Ruta al directorio que contiene los archivos CSV.
            output_file (str): Ruta para guardar el archivo CSV combinado de todas las categorías.
            total_categories (int): El número total de categorías para combinar.
        """
        combined_df = pd.DataFrame()  # DataFrame vacío para combinar todos los archivos

        # Iterar sobre las categorías
        for category in range(1, total_categories + 1):
            category_file = os.path.join(directory_path, f"combined_category_{category}.csv")
            print(f"Procesando archivo combinado de categoría {category}...")
            
            # Leer el archivo combinado de cada categoría
            if os.path.exists(category_file):
                df = pd.read_csv(category_file)
                combined_df = pd.concat([combined_df, df], ignore_index=True)
            else:
                print(f"El archivo {category_file} no existe.")

        # Eliminar duplicados basándose en todas las columnas
        combined_df.drop_duplicates(inplace=True)

        # Guardar el archivo combinado sin duplicados
        combined_df.to_csv(output_file, index=False)
        print(f"Archivos combinados de todas las categorías y guardados en {output_file}")

    @staticmethod
    def rename_columns(csv_path, output_path, rename_strategy=None):
        """
        Renombra las columnas de un archivo CSV y guarda el archivo con los nuevos nombres.

        Args:
            csv_path (str): Ruta del archivo CSV de entrada.
            output_path (str): Ruta donde se guardará el archivo con columnas renombradas.
            rename_strategy (callable, opcional): Función que define cómo transformar los nombres de columnas. 
                                                Si no se proporciona, se usará una estrategia predeterminada.
        """
        df = pd.read_csv(csv_path)
        
        if rename_strategy is None:
            def rename_strategy(col_name):
                # Paso 1: Eliminar prefijos y patrones específicos
                col_name = col_name.replace("compiledRelease/", "")
                col_name = col_name.replace("/0/", "_")
                
                # Paso 2: Reemplazar espacios por guiones bajos antes de la conversión
                col_name = col_name.replace(" ", "_")
                
                # Paso 3: Convertir camelCase a snake_case, pero asegurando que no se dividan letras como 'ID'
                # Esto asegura que las letras mayúsculas de 'ID' o similares no se dividan
                col_name = re.sub(r'([a-z])([A-Z])', r'\1_\2', col_name)  # Agregar guión bajo entre minúsculas y mayúsculas
                col_name = col_name.lower()  # Convertir todo a minúsculas
                
                # Paso 4: Reemplazar caracteres no deseados y limpiar guiones bajos
                col_name = re.sub(r'\W+', '_', col_name)  # Reemplazar caracteres no alfanuméricos
                col_name = re.sub(r'_+', '_', col_name)  # Reducir múltiples "_"
                col_name = col_name.strip('_')  # Quitar guiones bajos iniciales y finales
                return col_name

        new_column_names = {col: rename_strategy(col) for col in df.columns}
        df.rename(columns=new_column_names, inplace=True)
        
        df.to_csv(output_path, index=False)
        print(f"Archivo con columnas renombradas guardado en: {output_path}")

    @staticmethod
    def rename_columns_v2(csv_path, output_path, json_map_path):
        """
        Renombra las columnas de un archivo CSV utilizando un mapeo proporcionado en un archivo JSON
        y guarda el archivo con los nuevos nombres.

        Args:
            csv_path (str): Ruta del archivo CSV de entrada.
            output_path (str): Ruta donde se guardará el archivo con columnas renombradas.
            json_map_path (str): Ruta de un archivo JSON que contiene un mapeo explícito de columnas.
                                El formato debe ser {"columna_original": "columna_nueva"}.
        """
        # Leer el archivo CSV
        df = pd.read_csv(csv_path)

        # Cargar el mapa JSON
        with open(json_map_path, 'r', encoding='utf-8') as f:
            column_map = json.load(f)

        # Validar las claves del JSON para asegurarse de que existen en el DataFrame
        column_map = {key: value for key, value in column_map.items() if key in df.columns}

        # Renombrar las columnas
        if column_map:
            df.rename(columns=column_map, inplace=True)
        else:
            print("No se encontraron columnas coincidentes para renombrar.")

        # Guardar el DataFrame modificado
        df.to_csv(output_path, index=False)
        print(f"Archivo con columnas renombradas guardado en: {output_path}")
    
    @staticmethod
    def filter_csv_by_column(csv_path, output_path, column_name, filter_method="unique"):
        """
        Filtra un archivo CSV basado en una columna específica y un método de filtrado.

        Args:
            csv_path (str): Ruta del archivo CSV de entrada.
            output_path (str): Ruta donde se guardará el archivo filtrado.
            column_name (str): Nombre de la columna para aplicar el filtro.
            filter_method (str | callable): Método de filtrado. Opciones:
                - "unique": Elimina duplicados.
                - "non_null": Filtra valores no nulos.
                - "positive": Filtra valores mayores a 0.
                - callable: Permite pasar una lógica personalizada con un DataFrame.

        Returns:
            None
        """
        # Leer archivo CSV
        df = pd.read_csv(csv_path)

        # Diccionario con los métodos de filtrado
        filter_methods = {
            "unique": lambda df: df.drop_duplicates(subset=[column_name]),
            "non_null": lambda df: df[df[column_name].notnull()],
            "positive": lambda df: df[df[column_name] > 0]
        }

        # Aplicar el método de filtrado
        if callable(filter_method):
            filtered_df = filter_method(df)  # Filtro personalizado
        elif filter_method in filter_methods:
            filtered_df = filter_methods[filter_method](df)
        else:
            raise ValueError(f"El método de filtrado '{filter_method}' no es válido.")

        # Guardar archivo filtrado
        filtered_df.to_csv(output_path, index=False)
        print(f"Archivo filtrado guardado en: {output_path}")
    
    @staticmethod
    def filter_by_column_and_limit(csv_path, output_path, column_name, value, limit=None):
        """
        Filtra un archivo CSV por un valor específico en una columna y limita la cantidad de registros.

        Args:
            csv_path (str): Ruta del archivo CSV de entrada.
            output_path (str): Ruta donde se guardará el archivo filtrado.
            column_name (str): Nombre de la columna por la que se va a filtrar.
            value (str): El valor específico para filtrar en la columna, siempre pasado como string.
            limit (int | None): Número máximo de registros a devolver. Si es None, no hay límite.

        Returns:
            None
        """
        # Leer archivo CSV
        df = pd.read_csv(csv_path)

        # Obtener el tipo de la columna
        column_type = df[column_name].dtype

        # Convertir el valor de str al tipo de la columna
        if column_type == 'int64':  # Si la columna es numérica
            value = int(value)  # Convertir a entero
        elif column_type == 'float64':  # Si la columna es decimal
            value = float(value)  # Convertir a float
        # Si la columna es de tipo texto, no se necesita hacer ninguna conversión

        # Filtrar por el valor de la columna específica
        filtered_df = df[df[column_name] == value]

        # Limitar el número de registros si se especifica el límite
        if limit is not None:
            filtered_df = filtered_df.head(limit)

        # Guardar archivo filtrado
        filtered_df.to_csv(output_path, index=False)
        print(f"Archivo filtrado guardado en: {output_path}")


    
    




