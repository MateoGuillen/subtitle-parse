import os
from utils.ocds_procesor import OCDSProcessor

def main():
    base_url = "https://www.contrataciones.gov.py"
    url_buscador_licitacion = f'{base_url}/buscador/licitaciones.csv?nro_nombre_licitacion='
    url_ocds_dataset = f'{base_url}/images/opendata-v3/final/ocds'
    
    output_path = "./outputs"
    prefix_name = "merged_tender_data"
    os.makedirs(output_path, exist_ok=True)

    years = [2025]
    output_pdf = os.path.join(output_path, "ten_documents_pliego_pdf_every_year.csv")
    output_json = os.path.join(output_path, "ten_documents_pliego_json_every_year.csv")

    processor = OCDSProcessor(url_ocds_dataset, output_path)

    for year in years:
        processor.process_year(year, prefix_name)

    processor.merge_yearly_outputs(years, output_pdf, output_json, prefix_name )
    # processor.process_and_enrich_data(output_pdf, output_json, url_buscador_licitacion)  

if __name__ == "__main__":
    main()
