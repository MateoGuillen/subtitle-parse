import os
from src.utils.ocds_procesor import OCDSProcessor
from src.utils.csv_utility import CSVUtility
from src.utils.logging_utils import setup_logger
from config.settings import DNCP_BASE_URL
from config.settings import BASE_OUTPUT_DIR

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
    years = [2021,2022,2023,2024,2025]

    processor = OCDSProcessor(url_ocds_dataset, output_path)
    for year in years:
        processor.process_year(year, prefix_name)

    processor.merge_yearly_outputs(years, output_pdf, output_json, prefix_name)
    
    CSVUtility.filter_csv_by_column(output_pdf, output_pdf_filtered, column_name="nro_licitacion", filter_method="unique")
    
    
    
    

if __name__ == "__main__":
    main()
