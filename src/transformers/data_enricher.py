# src/transformers/data_enricher.py
import pandas as pd
import requests
from io import StringIO
from src.utils.logging_utils import setup_logger

class DataEnricher:
    def __init__(self):
        self.logger = setup_logger(__name__)
    
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
            
    def extract_year(self, date):
        if pd.isna(date):
            return None
        try:
            return str(date)[:4]
        except (TypeError, AttributeError):
            return None
    
    def enrich_with_additional_data(self, df, base_url, additional_columns):
        """Enriquece un DataFrame con datos adicionales de una API."""
        self.logger.info("Iniciando proceso de enriquecimiento de datos")
        
        # Asegurar que todas las columnas adicionales estén presentes
        for col in additional_columns:
            if col not in df.columns:
                df[col] = None

        for idx, row in df.iterrows():
            nro_licitacion = row.get("nro_licitacion")
            self.logger.debug(f"Enriqueciendo datos para licitación {nro_licitacion}")
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
                    self.logger.error(f"Error al enriquecer datos para licitación {nro_licitacion}: {e}")
        
        self.logger.info("Proceso de enriquecimiento completado")
        return df
    
    def enrich_with_ocds_api(self, df, api_url, api_key):
        """Enriquece un DataFrame con datos de la API OCDS."""
        self.logger.info("Iniciando enriquecimiento con datos OCDS")
        
        ocds_data = []

        for _, row in df.iterrows():
            tender_id = row.get("compiledRelease/tender/id")
            if pd.notna(tender_id):
                url = f"{api_url}/{tender_id}"
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
                    self.logger.error(f"Error al obtener datos para ID {tender_id}: {e}")
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

        # Convertir a DataFrame y combinar con el DataFrame original
        ocds_df = pd.DataFrame(ocds_data)
        enriched_df = pd.concat([df, ocds_df], axis=1)
        
        self.logger.info("Enriquecimiento con datos OCDS completado")
        return enriched_df