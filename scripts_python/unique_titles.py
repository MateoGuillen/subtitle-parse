import pandas as pd
import os
from pathlib import Path
from tqdm import tqdm
import asyncio
import aiofiles
import csv
from utils.csv_utility import CSVUtility

async def process_csv_file(file_path):
    """
    Procesa un archivo CSV y extrae los títulos únicos junto con el nombre del archivo.
    """
    try:
        # Usamos pandas para leer solo la columna 'titulo'
        df = pd.read_csv(file_path, usecols=['titulo'])
        # Agregamos el nombre del archivo para cada título
        titles_with_source = [(title, file_path.name) for title in df['titulo'].dropna().unique()]
        return titles_with_source
    except Exception as e:
        print(f"Error procesando {file_path}: {e}")
        return []

async def process_batch(files, pbar):
    """
    Procesa un lote de archivos CSV.
    """
    tasks = [process_csv_file(file) for file in files]
    results = await asyncio.gather(*tasks)
    pbar.update(len(files))
    # Aplanar la lista de resultados
    return [item for sublist in results for item in sublist]

def is_uppercase(text):
    """
    Verifica si un texto está completamente en mayúsculas.
    Ignora espacios, números y caracteres especiales.
    """
    # Filtrar solo letras
    letters = ''.join(c for c in text if c.isalpha())
    # Verificar si hay letras y si todas están en mayúsculas
    return len(letters) > 0 and letters.isupper()

async def main():
    # Directorio base
    base_dir = "./outputs/processed_json"
    years = ["2021", "2022", "2023", "2024", "2025"]
    
    # Recolectar todos los archivos CSV
    all_files = []
    for year in years:
        year_dir = Path(base_dir) / year
        if year_dir.exists():
            all_files.extend(list(year_dir.glob("*.csv")))
    
    if not all_files:
        print("No se encontraron archivos CSV")
        return
    
    print(f"Total de archivos encontrados: {len(all_files)}")
    
    # Configuración de procesamiento por lotes
    batch_size = 100
    all_titles = []
    
    # Procesar archivos en lotes con barra de progreso
    with tqdm(total=len(all_files), desc="Procesando archivos") as pbar:
        for i in range(0, len(all_files), batch_size):
            batch = all_files[i:i + batch_size]
            batch_titles = await process_batch(batch, pbar)
            all_titles.extend(batch_titles)
    
    # Convertir resultados a DataFrame
    df_result = pd.DataFrame(all_titles, columns=['titulo', 'nombre_archivo'])
    
    # Guardar resultados sin filtrar
    output_file = "titulos_con_fuente.csv"
    df_result.to_csv(output_file, index=False, quoting=csv.QUOTE_ALL)
    print(f"\nTotal de títulos encontrados: {len(df_result)}")
    print(f"Resultados guardados en: {output_file}")
    
    # Aplicar filtro usando CSVUtility
    output_file_filtered = "titulos_con_fuente_filtered.csv"
    CSVUtility.filter_csv_by_column(
        output_file,
        output_file_filtered,
        "titulo",
        filter_method="unique"
    )
    print(f"Resultados filtrados guardados en: {output_file_filtered}")
    
    # Filtrar títulos en mayúsculas
    df_filtered = pd.read_csv(output_file_filtered)
    df_uppercase = df_filtered[df_filtered['titulo'].apply(is_uppercase)]
    df_lowercase = df_filtered[~df_filtered['titulo'].apply(is_uppercase)]  # Títulos que NO están en mayúsculas
    
    # Guardar resultados de mayúsculas
    output_file_uppercase = "titulos_mayusculas.csv"
    df_uppercase.to_csv(output_file_uppercase, index=False, quoting=csv.QUOTE_ALL)
    print(f"\nTítulos en mayúsculas encontrados: {len(df_uppercase)}")
    print(f"Resultados de mayúsculas guardados en: {output_file_uppercase}")
    
    # Guardar resultados de títulos que NO están en mayúsculas
    output_file_lowercase = "titulos_sin_mayusculas.csv"
    df_lowercase.to_csv(output_file_lowercase, index=False, quoting=csv.QUOTE_ALL)
    print(f"\nTítulos sin mayúsculas encontrados: {len(df_lowercase)}")
    print(f"Resultados sin mayúsculas guardados en: {output_file_lowercase}")

if __name__ == "__main__":
    asyncio.run(main())