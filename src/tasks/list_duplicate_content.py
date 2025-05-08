import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from typing import Tuple, List, Dict, Optional
from dataclasses import dataclass
from collections import defaultdict

# Configuración de logging
def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('content_extraction.log'),
            logging.StreamHandler()
        ]
    )

# Función para cargar datos desde Parquet
def load_dataframes(pdf_lines_path: str, outlines_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and prepare the PDF lines and outlines DataFrames
    """
    logging.info("Loading PDF lines and outlines data...")
    pdf_lines_df = pd.read_parquet(pdf_lines_path)
    outlines_df = pd.read_parquet(outlines_path)
    return pdf_lines_df, outlines_df

# Función para limpiar caracteres duplicados
def clean_duplicate_characters(content: str) -> str:
    """
    Clean duplicate characters like --, ,, .., ;;, :: in the content.
    """
    if pd.isna(content):  # Manejar contenido nulo
        return content
    
    # Reemplazar caracteres duplicados
    replacements = {
        "--": "-",
        ",,": ",",
        "..": ".",
        ";;": ";",
        "::": ":",
        "°°": "°",
        "//": "/",
    }
    
    for old, new in replacements.items():
        content = content.replace(old, new)
    
    return content

# Función para identificar registros con líneas duplicadas consecutivas
def find_duplicate_lines(content: str) -> bool:
    """
    Check if the content has consecutive duplicate lines.
    """
    if pd.isna(content):  # Manejar contenido nulo
        return False
    lines = content.split('\n')
    for i in range(len(lines) - 1):
        if lines[i] == lines[i + 1]:
            return True
    return False

# Función para limpiar líneas duplicadas consecutivas
def remove_consecutive_duplicates(content: str) -> str:
    """
    Remove consecutive duplicate lines from the content.
    """
    if pd.isna(content):  # Manejar contenido nulo
        return content
    lines = content.split('\n')
    cleaned_lines = []
    previous_line = None
    for line in lines:
        if line != previous_line:
            cleaned_lines.append(line)
            previous_line = line
    return '\n'.join(cleaned_lines)

# Función principal
def main():
    """Main execution function"""
    setup_logging()
    
    # Ruta del archivo Parquet
    input_path = "./outputs/processed_pdf/sections/todos_2021_to_2024/content_sections_2021_to_2024.parquet"
    output_csv_path = "./outputs/processed_pdf/sections/todos_2021_to_2024/duplicate_lines_report.csv"
    output_cleaned_path = "./outputs/processed_pdf/sections/todos_2021_to_2024/content_sections_cleaned_2021_to_2024.parquet"
    
    try:
        # Cargar el archivo Parquet
        logging.info(f"Loading data from {input_path}...")
        df = pd.read_parquet(input_path)
        
        # Filtrar registros donde document_id no sea nulo
        logging.info("Filtering records with non-null document_id...")
        df = df[df['document_id'].notna()]
        
        # Limpiar caracteres duplicados en la columna 'content'
        logging.info("Cleaning duplicate characters in 'content'...")
        df['content'] = df['content'].apply(clean_duplicate_characters)
        
        # Identificar registros con líneas duplicadas consecutivas
        logging.info("Identifying records with consecutive duplicate lines...")
        df['has_duplicates'] = df['content'].apply(find_duplicate_lines)
        
        # Filtrar registros con líneas duplicadas
        duplicate_records = df[df['has_duplicates']]
        
        # Guardar los registros con líneas duplicadas en un CSV (solo columnas originales)
        logging.info(f"Saving duplicate records to {output_csv_path}...")
        original_columns = df.columns.tolist()  # Obtener las columnas originales
        original_columns.remove('has_duplicates')  # Eliminar la columna temporal
        duplicate_records[original_columns].to_csv(output_csv_path, index=False)
        
        # Limpiar los registros con líneas duplicadas
        logging.info("Cleaning duplicate lines...")
        df['cleaned_content'] = df['content'].apply(remove_consecutive_duplicates)
        
        # Filtrar registros con content_length > 0
        logging.info("Filtering records with content_length > 0...")
        df = df[df['content_length'] > 0]
        
        # Guardar el DataFrame limpio en un nuevo archivo Parquet
        logging.info(f"Saving cleaned data to {output_cleaned_path}...")
        df.to_parquet(output_cleaned_path, index=False)
        
        # Resumen
        print("\nDuplicate Lines Report:")
        print(f"Total records: {len(df)}")
        print(f"Records with consecutive duplicate lines: {len(duplicate_records)}")
        print(f"Duplicate records saved to: {output_csv_path}")
        print(f"Cleaned data saved to: {output_cleaned_path}")
        
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()