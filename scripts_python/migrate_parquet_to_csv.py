import pandas as pd
import os
from datetime import datetime

def convert_parquet_to_csv(parquet_path, output_dir='./outputs/csv'):
    """
    Convierte un archivo Parquet a CSV.
    
    Args:
        parquet_path (str): Ruta al archivo Parquet
        output_dir (str): Directorio donde se guardará el archivo CSV
    """
    try:
        # Crear el directorio de salida si no existe
        os.makedirs(output_dir, exist_ok=True)
        
        # Leer el archivo Parquet
        print(f"Leyendo archivo Parquet: {parquet_path}")
        df = pd.read_parquet(parquet_path)
        
        # Generar nombre del archivo CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(parquet_path))[0]
        csv_filename = f"{base_name}_{timestamp}.csv"
        csv_path = os.path.join(output_dir, csv_filename)
        
        # Convertir a CSV
        print("Convirtiendo a CSV...")
        df.to_csv(csv_path, index=False, encoding='utf-8')
        
        print("✔️ Conversión completada exitosamente.")
        print(f"✔️ Archivo guardado en: {csv_path}")
        print(f"✔️ Número de filas convertidas: {len(df)}")
        
    except Exception as e:
        print(f"❌ Error durante la conversión: {str(e)}")
        raise

def main():
    # Usar la misma ruta de tu archivo Parquet
    parquet_path = './outputs/processed_pdf/sections/content_sections_2021_to_2024.parquet'
    
    # Ejecutar la conversión
    convert_parquet_to_csv(parquet_path)

if __name__ == "__main__":
    main()