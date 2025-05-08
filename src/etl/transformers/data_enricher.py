""" A module for enriching data."""
# src/etl/transformers/data_enricher.py
import pandas as pd
from src.utils.logging_utils import setup_logger

class DataEnricher:
    """ A class for enriching data."""
    def __init__(self):
        self.logger = setup_logger(__name__)

    def extract_nro_licitacion(self, open_contracting_id):
        """ Extracts the nro_licitacion from the open_contracting_id."""
        if pd.isna(open_contracting_id):
            return None
        try:
            return open_contracting_id.split('-')[2]
        except IndexError:
            return None

    def extract_version_pliego(self, url):
        """ Extracts the version_pliego from the url."""
        if pd.isna(url):
            return None
        try:
            return url.split('/')[-1]
        except IndexError:
            return None

    def extract_categoria(self, category_details):
        """ Extracts the categoria from the category_details."""
        if pd.isna(category_details):
            return None
        try:
            return category_details.split(' - ', 1)[1] if ' - ' in category_details else category_details
        except AttributeError:
            return None

    def extract_year(self, date):
        """ Extracts the year from the date."""
        if pd.isna(date):
            return None
        try:
            return str(date)[:4]
        except (TypeError, AttributeError):
            return None
