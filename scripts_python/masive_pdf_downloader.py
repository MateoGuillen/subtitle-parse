from utils.licitaciones_downloader import LicitacionesDownloader
from utils.file_utility import FileUtility
from utils.csv_utility import CSVUtility




def main():
    try:
        # Configuración inicial
        input_root_dir = "./downloads/"
        output_root_dir = "./outputs/"
        outline_pdf_root_dir = "./pdfs/"
        dataset_path = "./outputs/ten_documents_pliego_pdf_every_year.csv"
        dataset_path_filtered = "./outputs/ten_documents_pliego_pdf_every_year_filtered.csv"
        
        FileUtility.ensure_directory_exists(input_root_dir)
        FileUtility.ensure_directory_exists(output_root_dir)
        FileUtility.ensure_directory_exists(outline_pdf_root_dir)
        
        # Descargar licitaciones
        downloader = LicitacionesDownloader(file_path=dataset_path, output_dir=outline_pdf_root_dir)
        filtered_csv = dataset_path
        CSVUtility.filter_csv_by_column(filtered_csv, dataset_path_filtered, "nro_licitacion", filter_method="unique")
        # filtered_csv = downloader.filter_csv(year=2025)
        all_downloaded_files = []  # Para almacenar todos los nombres de archivos descargados
        downloaded_files = downloader.download_files_from_urls(dataset_path_filtered)
        all_downloaded_files.extend(downloaded_files)  # Agregar los nombres a la lista global

    except Exception as e:
        print(f"Ha ocurrido un error: {e}")

if __name__ == "__main__":
    main()
