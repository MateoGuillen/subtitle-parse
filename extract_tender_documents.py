import os
from utils.ocds_procesor import OCDSProcessor

def main():
    base_url = "https://www.contrataciones.gov.py/images/opendata-v3/final/ocds"
    output_path = "./outputs"
    os.makedirs(output_path, exist_ok=True)
    
    years = [2021, 2022, 2023, 2024, 2025]
    output_pdf = os.path.join(output_path, "ten_documents_pliego_pdf_every_year.csv")
    output_json = os.path.join(output_path, "ten_documents_pliego_json_every_year.csv")
    

    processor = OCDSProcessor(base_url, output_path)
    processor.merge_yearly_outputs(years, output_pdf, output_json)
    

    # for year in range(2021, 2026):
    #     processor.process_year(year)

if __name__ == "__main__":
    main()
