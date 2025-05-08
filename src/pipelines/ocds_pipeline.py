""" A pipeline for processing OCDS data."""
import os
import pandas as pd
from src.etl.extractors.file_downloader import FileDownloader
from src.etl.extractors.file_extractor import FileExtractor
from src.etl.extractors.ocds_extractor import OCDSExtractor
from src.etl.transformers.csv_processor import CSVProcessor
from src.etl.transformers.data_enricher import DataEnricher
from src.etl.transformers.ocds_transformer import OCDSTransformer
from src.etl.loaders.csv_loader import CSVLoader
from src.utils.csv_utility import CSVUtility
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from src.utils.file_checker import FileChecker

class OCDSPipeline:
    """
    A pipeline for processing OCDS data.

    Attributes:
        config (dict): Configuration dictionary.
        base_url (str): Base URL for OCDS data.
        output_dir (str): Directory for output files.
        output_processed_dir (str): Directory for processed output files.
        input_external_dir (str): Directory for external data.
        logger (logging.Logger): Logger object for logging messages.
    """
    def __init__(self,config):
        self.config = config
        self.base_url = config.get('base_url')
        self.output_dir = config.get('output_dir')
        self.output_processed_dir = config.get('output_processed_dir')
        self.input_external_dir = config.get('input_external_dir')
        self.logger = setup_logger(__name__)
        self._initialize_components()
    def _initialize_components(self):
        """Initialize the components of the pipeline."""
        # Base components
        self.downloader = FileDownloader()
        self.extractor = FileExtractor()
        self.csv_processor = CSVProcessor()
        self.data_enricher = DataEnricher()
        self.csv_utility = CSVUtility()
        self.file_checker = FileChecker()

        # ETL components
        self.ocds_extractor = OCDSExtractor(self.base_url, self.output_dir)
        self.ocds_transformer = OCDSTransformer(self.output_dir, self.input_external_dir, self.output_processed_dir)
        self.csv_loader = CSVLoader(self.output_dir, self.output_processed_dir)

        # Set dependencies
        self.ocds_extractor.set_dependencies(self.downloader, self.extractor)
        self.ocds_transformer.set_dependencies(self.csv_processor, self.csv_utility, self.data_enricher)
    @error_handling(default_return=False)
    def process_year(self, year, prefix_name):
        """Process data for a specific year."""
        self.logger.info("Procesando datos del año %s...", year)

        # Extract
        csv_path, record_csv_path = self.ocds_extractor.extract_year_data(year)
        if not csv_path or not record_csv_path:
            self.logger.error("Error en la extracción del año %s. Saltando...", year)
            return False

        pdf_output = os.path.join(self.output_dir, f"{prefix_name}_pdf_{year}.csv")
        json_output = os.path.join(self.output_dir, f"{prefix_name}_json_{year}.csv")
        if self.file_checker.all_files_exist([pdf_output, json_output]):
            self.logger.info("Los archivos transformados del año %s ya existen. Omitiendo transformación.", year)
            return True

        # Transform
        pdf_df, json_df = self.ocds_transformer.transform_year_data(
            csv_path, record_csv_path, year, prefix_name
        )
        if pdf_df is None and json_df is None:
            self.logger.error("Error en la transformación del año %s. Saltando...", year)
            return False

        # Load
        _, _ = self.csv_loader.save_yearly_data(pdf_df, json_df, year, prefix_name)

        self.logger.info("Datos del año %s procesados correctamente.", year)
        return True

    @error_handling(default_return=(None, None))
    def merge_yearly_outputs(self, years, output_pdf_name, output_json_name, prefix_name):
        """Combine yearly outputs into a single file for all years."""
        self.logger.info("Combinando resultados de todos los años...")
        output_pdf_path = os.path.join(self.output_dir, output_pdf_name)
        output_json_path = os.path.join(self.output_dir, output_json_name)

        # Verify if the combined files already exist
        if self.file_checker.all_files_exist([output_pdf_path, output_json_path]):
            self.logger.info("Los archivos combinados ya existen. Omitiendo combinación.")
            # Read the combined files
            pdf_merged = pd.read_csv(output_pdf_path) if os.path.exists(output_pdf_path) else None
            json_merged = pd.read_csv(output_json_path) if os.path.exists(output_json_path) else None
            return pdf_merged, json_merged

        # Get the combined files
        self.logger.info("merge_yearly_outputs")
        pdf_merged, json_merged = self.ocds_transformer.merge_yearly_outputs(years, prefix_name)

        # Save the combined files
        self.logger.info("save_merged_data")
        _, _ = self.csv_loader.save_merged_data(
            pdf_merged, json_merged, output_pdf_name, output_json_name
        )

        self.logger.info("Combinación de datos completada correctamente.")
        return pdf_merged, json_merged

    @error_handling(default_return=None)
    def filter_unique_tenders(self, pdf_merged, output_filtered_name):
        """Filter for unique tenders"""
        self.logger.info("Filtrando licitaciones únicas...")
        output_filtered_path = os.path.join(self.output_dir, output_filtered_name)

        # Verify if the filtered file already exists
        if self.file_checker.file_exists(output_filtered_path):
            self.logger.info("El archivo filtrado ya existe. Omitiendo filtrado.")
            filtered_df = pd.read_csv(output_filtered_path)
            return filtered_df

        # Filter
        filtered_df = self.ocds_transformer.filter_unique_tenders(pdf_merged)

        # Save the filtered file
        if filtered_df is not None:
            self.csv_loader.save_csv(filtered_df, output_filtered_name, self.output_processed_dir)
            self.logger.info("Filtrado de licitaciones únicas completado.")
            return filtered_df
        return None

    def run(self):
        """Run the OCDS data processing pipeline using the config dictionary."""
        self.logger.info("Iniciando pipeline de procesamiento de datos OCDS...")

        # Process data for each year
        for year in self.config['years']:
            self.process_year(year, self.config['prefix_name'])

        # Merge yearly outputs
        pdf_merged, _ = self.merge_yearly_outputs(
            self.config['years'],
            self.config['output_pdf_name'],
            self.config['output_json_name'],
            self.config['prefix_name']
        )

        # Filter unique tenders
        if pdf_merged is not None:
            self.filter_unique_tenders(pdf_merged, self.config['output_filtered_name'])

        self.logger.info("Pipeline de procesamiento completado.")
