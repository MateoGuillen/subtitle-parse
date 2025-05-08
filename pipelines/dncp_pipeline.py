# pipelines/dncp_pipeline.py
import os
import pandas as pd
import logging
from src.extractors.file_downloader import FileDownloader
from src.extractors.file_extractor import FileExtractor
from src.transformers.csv_processor import CSVProcessor
from src.transformers.data_enricher import DataEnricher
from src.loaders.csv_loader import CSVLoader
from src.utils.csv_utility import CSVUtility
from src.utils.logging_utils import setup_logger
from config.settings import DNCP_BASE_URL, BASE_OUTPUT_DIR

def process_year(year, prefix_name, base_url, output_dir):
    """Procesa los datos para un año específico."""
    logger = logging.getLogger(__name__)
    
    # Instanciar clases
    downloader = FileDownloader()
    extractor = FileExtractor()
    processor = CSVProcessor()
    enricher = DataEnricher()
    loader = CSVLoader(output_dir)
    
    # Definir nombres de archivos
    zip_name = f"masivo_{year}.zip"
    csv_name = f"ten_documents_{year}.csv"
    record_csv_name = f"records_{year}.csv"
    
    zip_path = os.path.join(output_dir, zip_name)
    csv_path = os.path.join(output_dir, csv_name)
    record_csv_path = os.path.join(output_dir, record_csv_name)
    
    try:
        # Descargar archivo zip
        url = f"{base_url}/{year}/masivo.zip"
        downloader.download_file(url, zip_path)
        
        # Extraer archivos CSV
        extracted_ten_documents_path = extractor.extract_file(zip_path, "ten_documents.csv", output_dir)
        extracted_record_path = extractor.extract_file(zip_path, "records.csv", output_dir)
        
        # Renombrar archivos
        os.rename(extracted_ten_documents_path, csv_path)
        os.rename(extracted_record_path, record_csv_path)
        
        # Renombrar columnas
        CSVUtility.rename_columns(csv_path, csv_path)
        CSVUtility.rename_columns(record_csv_path, record_csv_path)
        
        # Filtrar para PDFs
        pdf_output = os.path.join(output_dir, f"ten_documents_pliego_pdf_{year}.csv")
        processor.filter_and_process_csv(
            csv_path, 
            pdf_output, 
            {
                "tender_documents_document_type_details": "Pliego Electrónico de bases y Condiciones",
                "tender_documents_format": "application/pdf",
            }, 
            "tender_documents_date_published"
        )
        
        # Filtrar para JSONs
        json_output = os.path.join(output_dir, f"ten_documents_pliego_json_{year}.csv")
        processor.filter_and_process_csv(
            csv_path, 
            json_output, 
            {
                "tender_documents_document_type_details": "Pliego Electrónico de bases y Condiciones",
                "tender_documents_format": "application/json",
            }, 
            "tender_documents_date_published"
        )
        
        # Realizar merge con records y enriquecimiento
        logger.info(f"Realizando left join para PDFs del año {year}...")
        pdf_df = pd.read_csv(pdf_output)
        record_df = pd.read_csv(record_csv_path)
        pdf_merged_df = pdf_df.merge(record_df, on="tender_id", how="left")
        
        # Enriquecer datos
        pdf_merged_df["nro_licitacion"] = pdf_merged_df["open_contracting_id"].apply(enricher.extract_nro_licitacion)
        pdf_merged_df["version_pliego"] = pdf_merged_df["tender_documents_url"].apply(enricher.extract_version_pliego)
        pdf_merged_df["categoria"] = pdf_merged_df["tender_main_procurement_category_details"].apply(enricher.extract_categoria)
        pdf_merged_df["year"] = pdf_merged_df["date"].apply(enricher.extract_year)
        
        # Proceso similar para JSON
        logger.info(f"Realizando left join para JSONs del año {year}...")
        json_df = pd.read_csv(json_output)
        json_merged_df = json_df.merge(record_df, on="tender_id", how="left")
        
        json_merged_df["nro_licitacion"] = json_merged_df["open_contracting_id"].apply(enricher.extract_nro_licitacion)
        json_merged_df["version_pliego"] = json_merged_df["tender_documents_url"].apply(enricher.extract_version_pliego)
        json_merged_df["categoria"] = json_merged_df["tender_main_procurement_category_details"].apply(enricher.extract_categoria)
        json_merged_df["year"] = json_merged_df["date"].apply(enricher.extract_year)
        
        # Cargar categorías
        categories_df = pd.read_csv("./inputs/unique_categories_sorted.csv")
        categories_df.rename(columns={
            "compiledRelease/parties/0/details/categories/0/id": "categoria_id",
            "compiledRelease/parties/0/details/categories/0/name": "categoria"
        }, inplace=True)
        
        # Merge con categorías
        pdf_merged_df = pdf_merged_df.merge(categories_df, on="categoria", how="left")
        json_merged_df = json_merged_df.merge(categories_df, on="categoria", how="left")
        
        # Guardar resultados
        pdf_merged_output_path = os.path.join(output_dir, f"{prefix_name}_pdf_{year}.csv")
        json_merged_output_path = os.path.join(output_dir, f"{prefix_name}_json_{year}.csv")
        
        loader.save_csv(pdf_merged_df, f"{prefix_name}_pdf_{year}.csv")
        loader.save_csv(json_merged_df, f"{prefix_name}_json_{year}.csv")
        
        logger.info(f"Datos finales guardados en {pdf_merged_output_path} y {json_merged_output_path}")
        return pdf_merged_output_path, json_merged_output_path
    except Exception as e:
        logger.error(f"Error al procesar datos del año {year}: {e}")
        return None, None

def merge_yearly_outputs(years, output_pdf, output_json, prefix_name, output_dir):
    """Combina los resultados de varios años."""
    logger = logging.getLogger(__name__)
    processor = CSVProcessor()
    
    try:
        pdf_frames = []
        json_frames = []

        for year in years:
            pdf_file = os.path.join(output_dir, f"{prefix_name}_pdf_{year}.csv")
            json_file = os.path.join(output_dir, f"{prefix_name}_json_{year}.csv")

            if os.path.exists(pdf_file):
                pdf_frames.append(pd.read_csv(pdf_file))
            if os.path.exists(json_file):
                json_frames.append(pd.read_csv(json_file))

        if pdf_frames:
            processor.merge_dataframes(pdf_frames, output_pdf)

        if json_frames:
            processor.merge_dataframes(json_frames, output_json)

        logger.info(f"Archivos combinados guardados en {output_pdf} y {output_json}")
        return output_pdf, output_json
    except Exception as e:
        logger.error(f"Error al combinar archivos: {e}")
        return None, None

def main():
    # Configurar logger
    logger = setup_logger("dncp_pipeline")
    logger.info("Iniciando el pipeline de extracción de documentos de licitaciones...")
    logger.info("Directorio de trabajo actual: %s", os.getcwd())
    
    output_dir_name = f'{BASE_OUTPUT_DIR}/csv/datasets/'
    output_pdf_file_name = "ten_documents_pliego_pdf_every_year.csv"
    output_json_file_name = "ten_documents_pliego_json_every_year.csv"
    output_pdf_filtered_file_name = "ten_documents_pliego_pdf_every_year_filtered.csv"
    
    url_ocds_dataset = f'{DNCP_BASE_URL}/images/opendata-v3/final/ocds'
     
    output_path = os.path.abspath(output_dir_name)
    os.makedirs(output_path, exist_ok=True)
    
    output_pdf = os.path.join(output_path, output_pdf_file_name)
    output_json = os.path.join(output_path, output_json_file_name)
    output_pdf_filtered = os.path.join(output_path, output_pdf_filtered_file_name)
     
    prefix_name = "merged_tender_data"
    years = [2021, 2022, 2023, 2024, 2025]
    
    # Procesar datos por año
    for year in years:
        process_year(year, prefix_name, url_ocds_dataset, output_path)
    
    # Combinar resultados de todos los años
    merge_yearly_outputs(years, output_pdf, output_json, prefix_name, output_path)
    
    # Filtrar para obtener licitaciones únicas
    CSVUtility.filter_csv_by_column(output_pdf, output_pdf_filtered, column_name="nro_licitacion", filter_method="unique")

if __name__ == "__main__":
    main()