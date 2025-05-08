import os
import pandas as pd

def filter_not_downloaded(dataset_path_filtered_limit, downloads_folder):
    """
    Filtra el archivo CSV para obtener los nro_licitacion que no tienen un archivo PDF
    correspondiente en la carpeta de descargas y guarda un archivo con los resultados.

    Args:
        dataset_path_filtered_limit (str): Ruta del archivo CSV con los nro_licitacion.
        downloads_folder (str): Ruta de la carpeta de descargas donde se encuentran los archivos PDF.

    Returns:
        None
    """
    # Leer el archivo CSV
    df = pd.read_csv(dataset_path_filtered_limit)

    # Obtener los nro_licitacion del CSV
    licitaciones_csv = set(df['nro_licitacion'].astype(str))  # Convertir a set para facilitar la comparación

    # Listar los archivos PDF en la carpeta de descargas
    archivos_descargados = os.listdir(downloads_folder)
    
    # Filtrar solo los archivos con el formato {year}_{categoria}_{nro_licitacion}.pdf
    licitaciones_descargadas = set()
    for archivo in archivos_descargados:
        if archivo.endswith('.pdf'):
            # Extraer nro_licitacion del nombre del archivo
            parts = archivo.split('_')
            if len(parts) >= 3:  # Asegurarse de que tiene el formato esperado
                nro_licitacion = parts[2].replace('.pdf', '')
                licitaciones_descargadas.add(nro_licitacion)

    # Filtrar el DataFrame para obtener solo las licitaciones no descargadas
    licitaciones_no_descargadas = licitaciones_csv - licitaciones_descargadas
    df_no_descargadas = df[df['nro_licitacion'].astype(str).isin(licitaciones_no_descargadas)]

    # Crear un nombre de archivo personalizado para los no descargados
    output_file = f"./outputs/{dataset_path_filtered_limit.split('/')[-1].replace('.csv', '')}_no_descargados.csv"
    
    # Guardar el archivo filtrado
    df_no_descargadas.to_csv(output_file, index=False)
    print(f"Archivo con las licitaciones no descargadas guardado como: {output_file}")

def main():
    try:
        # Año y rutas de los archivos
        year = '2024'
        dataset_path_filtered_limit = f'./outputs/ten_documents_pliego_pdf_every_year_filtered_limit_{year}.csv'
        downloads_folder = f'./down/{year}/'
        
        # Llamar a la función de filtrado
        filter_not_downloaded(dataset_path_filtered_limit, downloads_folder)
    
    except Exception as e:
        print(f"Ha ocurrido un error: {str(e)}")

if __name__ == "__main__":
    main()
