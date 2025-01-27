import os
import requests
import zipfile
import pandas as pd
from io import StringIO
from utils.csv_utility import CSVUtility


class FileDownloader:
    @staticmethod
    def download_file(url, output_path):
        print(f"Descargando archivo desde {url}")
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()  
            with open(output_path, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            print(f"Archivo descargado y guardado en {output_path}")
        except requests.exceptions.RequestException as e:
            print(f"Error al descargar el archivo desde {url}: {e}")
            raise

class FileExtractor:
    @staticmethod
    def extract_file(zip_path, target_file, output_path):
        print(f"Extrayendo archivo {target_file} de {zip_path}")
        try:
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                file_list = zip_ref.namelist()
                if target_file in file_list:
                    zip_ref.extract(target_file, output_path)
                    print(f"Archivo extraído a {output_path}")
                    return os.path.join(output_path, target_file)
                else:
                    raise FileNotFoundError(f"{target_file} no encontrado en {zip_path}")
        except (zipfile.BadZipFile, FileNotFoundError) as e:
            print(f"Error al extraer el archivo: {e}")
            raise


class CSVProcessor:
    @staticmethod
    def filter_and_process_csv(input_csv, output_csv, column_filters, date_column):
        try:
            print(f"Procesando CSV: {input_csv}")
            df = pd.read_csv(input_csv)

            for column, value in column_filters.items():
                df = df[df[column] == value]

            if not df.empty:
                df[date_column] = pd.to_datetime(df[date_column])
                df = df.sort_values(by=date_column, ascending=False).drop_duplicates(
                    subset="tender_id", keep="first"
                )

            df.to_csv(output_csv, index=False)
            print(f"CSV procesado guardado en {output_csv}")
        except Exception as e:
            print(f"Error al procesar el archivo CSV: {e}")
            raise


class OCDSProcessor:
    def __init__(self, base_url, output_dir):
        self.base_url = base_url
        self.output_dir = output_dir
    def extract_nro_licitacion(self, open_contracting_id):
        if pd.isna(open_contracting_id):
            return None
        try:
            return open_contracting_id.split('-')[2]
        except IndexError:
            return None

    def extract_version_pliego(self, url):
        if pd.isna(url):
            return None
        try:
            return url.split('/')[-1]
        except IndexError:
            return None

    def extract_categoria(self, category_details):
        if pd.isna(category_details):
            return None
        try:
            return category_details.split(' - ', 1)[1] if ' - ' in category_details else category_details
        except AttributeError:
            return None
    def extract_year(self,date):
        if pd.isna(date):
            return None
        try:
            return str(date)[:4]
        except (TypeError, AttributeError):
            return None

    def process_year(self, year, prefix_name):
        zip_name = f"masivo_{year}.zip"
        csv_name = f"ten_documents_{year}.csv"
        zip_path = os.path.join(self.output_dir, zip_name)
        csv_path = os.path.join(self.output_dir, csv_name)
        record_csv_name = f"records_{year}.csv"
        record_csv_path = os.path.join(self.output_dir, record_csv_name)

        try:
            # Descargar masivo.zip
            url = f"{self.base_url}/{year}/masivo.zip"
            FileDownloader.download_file(url, zip_path)

            # Extraer ten_documents.csv y records.csv
            extracted_ten_documents_path = FileExtractor.extract_file(zip_path, "ten_documents.csv", self.output_dir)
            extracted_record_path = FileExtractor.extract_file(zip_path, "records.csv", self.output_dir)

            # Renombrar los archivos extraídos
            os.rename(extracted_ten_documents_path, csv_path)
            os.rename(extracted_record_path, record_csv_path)
            
            # Renombrar las columnas
            CSVUtility.rename_columns(csv_path, csv_path)
            CSVUtility.rename_columns(record_csv_path, record_csv_path)

            # Filtrar datos y generar archivos para PDFs
            pdf_output = os.path.join(self.output_dir, f"ten_documents_pliego_pdf_{year}.csv")
            CSVProcessor.filter_and_process_csv(csv_path, pdf_output, {
                "tender_documents_document_type_details": "Pliego Electrónico de bases y Condiciones",
                "tender_documents_format": "application/pdf",
            }, "tender_documents_date_published")

            # Filtrar datos y generar archivos para JSONs
            json_output = os.path.join(self.output_dir, f"ten_documents_pliego_json_{year}.csv")
            CSVProcessor.filter_and_process_csv(csv_path, json_output, {
                "tender_documents_document_type_details": "Pliego Electrónico de bases y Condiciones",
                "tender_documents_format": "application/json",
            }, "tender_documents_date_published")

            # Realizar un left join entre PDFs y record.csv
            print(f"Realizando left join para PDFs del año {year}...")
            pdf_df = pd.read_csv(pdf_output)
            record_df = pd.read_csv(record_csv_path)
            pdf_merged_df = pdf_df.merge(record_df, on="tender_id", how="left")

            # Agregar columnas nro_licitacion, version_pliego y categoria
            pdf_merged_df["nro_licitacion"] = pdf_merged_df["open_contracting_id"].apply(self.extract_nro_licitacion)
            pdf_merged_df["version_pliego"] = pdf_merged_df["tender_documents_url"].apply(self.extract_version_pliego)
            pdf_merged_df["categoria"] = pdf_merged_df["tender_main_procurement_category_details"].apply(self.extract_categoria)
            pdf_merged_df["year"] = pdf_merged_df["date"].apply(self.extract_year)
            

            # Realizar un left join entre JSONs y record.csv
            print(f"Realizando left join para JSONs del año {year}...")
            json_df = pd.read_csv(json_output)
            json_merged_df = json_df.merge(record_df, on="tender_id", how="left")

            # Agregar columnas nro_licitacion, version_pliego y categoria
            json_merged_df["nro_licitacion"] = json_merged_df["open_contracting_id"].apply(self.extract_nro_licitacion)
            json_merged_df["version_pliego"] = json_merged_df["tender_documents_url"].apply(self.extract_version_pliego)
            json_merged_df["categoria"] = json_merged_df["tender_main_procurement_category_details"].apply(self.extract_categoria)
            json_merged_df["year"] = json_merged_df["date"].apply(self.extract_year)
            

            # Cargar archivo de categorías y renombrar columnas
            categories_df = pd.read_csv("./inputs/unique_categories_sorted.csv")
            categories_df.rename(columns={
                "compiledRelease/parties/0/details/categories/0/id": "categoria_id",
                "compiledRelease/parties/0/details/categories/0/name": "categoria"
            }, inplace=True)

            # Realizar un left join con las categorías
            pdf_merged_df = pdf_merged_df.merge(categories_df, on="categoria", how="left")
            json_merged_df = json_merged_df.merge(categories_df, on="categoria", how="left")

            # Guardar los resultados finales
            pdf_merged_output_path = os.path.join(self.output_dir, f"{prefix_name}_pdf_{year}.csv")
            pdf_merged_df.to_csv(pdf_merged_output_path, index=False)

            json_merged_output_path = os.path.join(self.output_dir, f"{prefix_name}_json_{year}.csv")
            json_merged_df.to_csv(json_merged_output_path, index=False)

            print(f"Datos finales guardados en {pdf_merged_output_path} y {json_merged_output_path}")
            return pdf_merged_output_path, json_merged_output_path
        except Exception as e:
            print(f"Error al procesar datos del año {year}: {e}")
            return None, None
    def merge_yearly_outputs(self, years, output_pdf, output_json,prefix_name):
        try:
            pdf_frames = []
            json_frames = []

            for year in years:
                pdf_file = os.path.join(self.output_dir, f"{prefix_name}_pdf_{year}.csv")
                json_file = os.path.join(self.output_dir, f"{prefix_name}_json_{year}.csv")

                if os.path.exists(pdf_file):
                    pdf_frames.append(pd.read_csv(pdf_file))
                if os.path.exists(json_file):
                    json_frames.append(pd.read_csv(json_file))

            if pdf_frames:
                pd.concat(pdf_frames).to_csv(output_pdf, index=False)

            if json_frames:
                pd.concat(json_frames).to_csv(output_json, index=False)

            print(f"Archivos combinados guardados en {output_pdf} y {output_json}")
            return output_pdf, output_json
        except Exception as e:
            print(f"Error al combinar archivos: {e}")
            return None, None

    def process_and_enrich_data(self, input_csv, output_csv, base_url):
        try:
            df = pd.read_csv(input_csv)

            # Columnas adicionales que queremos incluir
            additional_columns = [
                "planificacion_slug", "convocatoria_slug", "adjudicacion_slug",
                "precalificacion_slug", "convenio_slug","nombre_licitacion", "tipo_procedimiento", "categoria", "convocante",
                "_etapa_licitacion", "etapa_licitacion", "fecha_entrega_oferta",
                "tipo_licitacion", "fecha_estimada", "fecha_publicacion_convocatoria", "geo"
            ]
            # Asegurar que todas las columnas adicionales estén presentes
            for col in additional_columns:
                if col not in df.columns:
                    df[col] = None

            for idx, row in df.iterrows():
                nro_licitacion = row.get("nro_licitacion")
                print(f"Enriqueciendo datos para licitación {nro_licitacion}")
                if pd.notna(nro_licitacion):
                    url = f"{base_url}{nro_licitacion}"
                    try:
                        response = requests.get(url, timeout=10)
                        response.raise_for_status()
                        csv_data = StringIO(response.content.decode("utf-8"))
                        temp_df = pd.read_csv(csv_data)
                        if not temp_df.empty:
                            for col in additional_columns:
                                if col in temp_df.columns:
                                    df.at[idx, col] = temp_df.iloc[0].get(col, None)
                    except requests.exceptions.RequestException as e:
                        print(f"Error al enriquecer datos para licitación {nro_licitacion}: {e}")

            # Guardar el resultado enriquecido en el archivo de salida
            df.to_csv(output_csv, index=False)
            print(f"Datos enriquecidos guardados en {output_csv}")
        except Exception as e:
            print(f"Error al enriquecer datos: {e}")


            
    def process_ocds_data(self, input_csv, output_csv, api_key):
        try:
            print(f"Procesando datos OCDS desde {input_csv}")
            df = pd.read_csv(input_csv)

            ocds_data = []

            for _, row in df.iterrows():
                tender_id = row.get("compiledRelease/tender/id")
                if pd.notna(tender_id):
                    url = f"https://www.contrataciones.gov.py/datos/api/v3/doc/tender/{tender_id}"
                    headers = {
                        "accept": "application/json",
                        "Authorization": api_key
                    }

                    try:
                        response = requests.get(url, headers=headers, timeout=10)
                        response.raise_for_status()
                        tender_data = response.json().get("tender", {})
                        ocds_data.append({
                            "mainProcurementCategoryDetails": tender_data.get("mainProcurementCategoryDetails"),
                            "mainProcurementCategory": tender_data.get("mainProcurementCategory"),
                            "title": tender_data.get("title"),
                            "procurementMethodDetails": tender_data.get("procurementMethodDetails"),
                            "procurementMethod": tender_data.get("procurementMethod"),
                            "id": tender_data.get("id"),
                            "statusDetails": tender_data.get("statusDetails"),
                            "awardCriteriaDetails": tender_data.get("awardCriteriaDetails"),
                        })
                    except requests.exceptions.RequestException as e:
                        print(f"Error al obtener datos para ID {tender_id}: {e}")
                        ocds_data.append({
                            "mainProcurementCategoryDetails": None,
                            "mainProcurementCategory": None,
                            "title": None,
                            "procurementMethodDetails": None,
                            "procurementMethod": None,
                            "id": tender_id,
                            "statusDetails": None,
                            "awardCriteriaDetails": None,
                        })

            # Convertir a DataFrame y combinar con el CSV original
            ocds_df = pd.DataFrame(ocds_data)
            enriched_df = pd.concat([df, ocds_df], axis=1)

            enriched_df.to_csv(output_csv, index=False)
            print(f"Datos procesados y guardados en {output_csv}")
        except Exception as e:
            print(f"Error al procesar datos OCDS: {e}")

