import os
from utils.subtitle_extractor import SubtitleExtractor
from utils.pdf_proccesor import PDFProcessor
from utils.licitaciones_downloader import LicitacionesDownloader

def ensure_directory_exists(directory_path):
    """
    Crea el directorio si no existe.
    """
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)

def main():
    try:
        # Configuración inicial
        input_root_dir = "./inputs/"
        output_root_dir = "./outputs/"
        input_file_name = "pliego"  

        input_pdf_path = os.path.join(input_root_dir, f"{input_file_name}.pdf")
        output_html_path = os.path.join(output_root_dir, f"{input_file_name}.html")
        output_csv_path = os.path.join(output_root_dir, f"{input_file_name}.csv")

        exclusion_list = ["Etapas y Plazos", "Adjudicación y Contrato", "Datos del Contacto"]

       
        ensure_directory_exists(input_root_dir)
        ensure_directory_exists(output_root_dir)

        # Configuración para el conversor de PDF 
        config = {
            "output_format": "html",
        }

        # Descargar licitaciones
        downloader = LicitacionesDownloader()
        downloader.download_files_from_urls(downloader.filter_csv(categoria=1, cantidad=2))

        # # Procesar el archivo PDF y generar el archivo HTML de salida
        # pdf_processor = PDFProcessor(config)
        # pdf_processor.process_pdf(input_pdf_path, output_html_path)

        # # Extraer subtítulos del archivo HTML y guardarlos en CSV
        # extractor = SubtitleExtractor(exclusion_list)
        # subtitles = extractor.extract_subtitles(output_html_path, output_csv_path)

        # print(f"Subtítulos extraídos y guardados en {output_csv_path}")
        # print("Subtítulos encontrados:", subtitles)


    except Exception as e:
        print(f"Ha ocurrido un error: {e}")

if __name__ == "__main__":
    main()
