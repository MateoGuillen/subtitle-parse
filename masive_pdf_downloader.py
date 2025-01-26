import os
from utils.licitaciones_downloader import LicitacionesDownloader
from utils.pdf_proccesor_v2 import ProcesadorPDF

def ensure_directory_exists(directory_path):
    """
    Crea el directorio si no existe.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def main():
    try:
        # Configuración inicial
        input_root_dir = "./downloads/"
        output_root_dir = "./outputs/"
        outline_pdf_root_dir = "./outline_pdfs/"


        ensure_directory_exists(input_root_dir)
        ensure_directory_exists(output_root_dir)

        # Descargar licitaciones
        downloader = LicitacionesDownloader()
        dataset_path = "./outputs/ten_documents_pliego_pdf_every_year.csv"
        all_downloaded_files = []  # Para almacenar todos los nombres de archivos descargados
        downloaded_files = downloader.download_files_from_urls(dataset_path)
        all_downloaded_files.extend(downloaded_files)  # Agregar los nombres a la lista global

        # Procesar y extraer subtítulos de cada archivo descargado
        for file_name_without_ext in all_downloaded_files:
            input_pdf_path = os.path.join(input_root_dir, f"{file_name_without_ext}.pdf")
            procesadorpdf = ProcesadorPDF(input_pdf_path)
            input_csv_path = f'{outline_pdf_root_dir}{file_name_without_ext}.csv'
            output_csv_path = os.path.join(output_root_dir, f"{file_name_without_ext}.csv")


            print(f"Procesando PDF: {input_pdf_path} con outline en {input_csv_path}")
            procesadorpdf.extraer_outline_con_posiciones(input_csv_path, output_csv_path)
            print(f"Subtítulos extraídos y guardados en {output_csv_path}")
    except Exception as e:
        print(f"Ha ocurrido un error: {e}")

if __name__ == "__main__":
    main()
