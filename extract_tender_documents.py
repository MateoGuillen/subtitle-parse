import os
from utils.ocds_procesor import OCDSProcessor
from utils.csv_utility import CSVUtility
from utils.file_utility import FileUtility

def main():
    # Verificar el directorio actual
    print(f"Directorio de trabajo actual: {os.getcwd()}")

    base_url = "https://www.contrataciones.gov.py"
    url_buscador_licitacion = f'{base_url}/buscador/licitaciones.csv?nro_nombre_licitacion='
    url_ocds_dataset = f'{base_url}/images/opendata-v3/final/ocds'
    
    # Usar rutas absolutas para evitar problemas
    output_path = os.path.abspath("./outputs")
    os.makedirs(output_path, exist_ok=True)

    output_pdf = os.path.join(output_path, "ten_documents_pliego_pdf_every_year.csv")
    output_json = os.path.join(output_path, "ten_documents_pliego_json_every_year.csv")

    prefix_name = "merged_tender_data"
    years = [2025]

    processor = OCDSProcessor(url_ocds_dataset, output_path)
    # CSVUtility.rename_columns(output_pdf_merged, output_pdf_merged)
    for year in years:
        processor.process_year(year, prefix_name)

    processor.merge_yearly_outputs(years, output_pdf, output_json, prefix_name)
    
    
    

if __name__ == "__main__":
    main()
