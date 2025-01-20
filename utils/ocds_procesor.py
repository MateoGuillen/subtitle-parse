import os
import requests
import zipfile
import pandas as pd

class FileDownloader:
    @staticmethod
    def download_file(url, output_path):
        response = requests.get(url, stream=True)
        if response.status_code == 200:
            with open(output_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
        else:
            raise Exception(f"Failed to download file from {url}, status code: {response.status_code}")

class FileExtractor:
    @staticmethod
    def extract_file(zip_path, target_file, output_path):
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            if target_file in file_list:
                zip_ref.extract(target_file, output_path)
                return os.path.join(output_path, target_file)
            else:
                raise FileNotFoundError(f"{target_file} not found in {zip_path}")

class CSVProcessor:
    @staticmethod
    def filter_and_process_csv(input_csv, output_csv, column_filters, date_column):
        df = pd.read_csv(input_csv)
        
        # Apply the column filters
        for column, value in column_filters.items():
            df = df[df[column] == value]
        
        # Keep only the most recent record for each unique tender ID
        if not df.empty:
            df[date_column] = pd.to_datetime(df[date_column])
            df = df.sort_values(by=date_column, ascending=False).drop_duplicates(
                subset="compiledRelease/tender/id", keep="first"
            )
        
        df.to_csv(output_csv, index=False)

class OCDSProcessor:
    def __init__(self, base_url, output_dir):
        self.base_url = base_url
        self.output_dir = output_dir

    def process_year(self, year):
        zip_name = f"masivo_{year}.zip"
        csv_name = f"ten_documents_{year}.csv"
        zip_path = os.path.join(self.output_dir, zip_name)
        csv_path = os.path.join(self.output_dir, csv_name)
        
        # Download the ZIP file
        url = f"{self.base_url}/{year}/masivo.zip"
        FileDownloader.download_file(url, zip_path)
        
        # Extract the target CSV
        extracted_csv_path = FileExtractor.extract_file(zip_path, "ten_documents.csv", self.output_dir)
        os.rename(extracted_csv_path, csv_path)

        # Process for PDFs
        pdf_output = os.path.join(self.output_dir, f"ten_documents_pliego_pdf_{year}.csv")
        CSVProcessor.filter_and_process_csv(csv_path, pdf_output, {
            "compiledRelease/tender/documents/0/documentTypeDetails": "Pliego Electrónico de bases y Condiciones",
            "compiledRelease/tender/documents/0/format": "application/pdf",
        }, "compiledRelease/tender/documents/0/datePublished")

        # Process for JSONs
        json_output = os.path.join(self.output_dir, f"ten_documents_pliego_json_{year}.csv")
        CSVProcessor.filter_and_process_csv(csv_path, json_output, {
            "compiledRelease/tender/documents/0/documentTypeDetails": "Pliego Electrónico de bases y Condiciones",
            "compiledRelease/tender/documents/0/format": "application/json",
        }, "compiledRelease/tender/documents/0/datePublished")

        # Remove ZIP file
        os.remove(zip_path)
        
    def merge_yearly_outputs(self, years, output_pdf, output_json):
        pdf_frames = []
        json_frames = []
        
        for year in years:
            pdf_file = os.path.join(self.output_dir, f"ten_documents_pliego_pdf_{year}.csv")
            json_file = os.path.join(self.output_dir, f"ten_documents_pliego_json_{year}.csv")
            
            if os.path.exists(pdf_file):
                pdf_frames.append(pd.read_csv(pdf_file))
            if os.path.exists(json_file):
                json_frames.append(pd.read_csv(json_file))
        
        # Concatenate and save merged PDF files
        if pdf_frames:
            pd.concat(pdf_frames).to_csv(output_pdf, index=False)
        
        # Concatenate and save merged JSON files
        if json_frames:
            pd.concat(json_frames).to_csv(output_json, index=False)

   

