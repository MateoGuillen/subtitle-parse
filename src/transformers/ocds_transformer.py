""" A module for transforming OCDS data."""
# src/transformers/ocds_transformer.py
import os
import pandas as pd
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling

class OCDSTransformer:
    """A class for transforming OCDS data."""
    def __init__(self, output_dir, input_external_dir, output_processed_dir):
        self.output_dir = output_dir
        self.output_processed_dir = output_processed_dir
        self.logger = setup_logger(__name__)
        self.csv_processor = None
        self.csv_utility = None
        self.data_enricher = None
        self.input_external_dir = input_external_dir

    def set_dependencies(self, csv_processor, csv_utility, data_enricher):
        """Inject dependencies into the transformer."""
        self.csv_processor = csv_processor
        self.csv_utility = csv_utility
        self.data_enricher = data_enricher

    @error_handling(default_return=(None, None))
    def transform_year_data(self, csv_path, record_csv_path, year, prefix_name):
        """Transform OCDS data for a specific year."""
        # Rename columns
        self.csv_utility.rename_columns(csv_path, csv_path)
        self.csv_utility.rename_columns(record_csv_path, record_csv_path)

        # Filter for PDFs
        pdf_output = os.path.join(self.output_dir, f"ten_documents_pliego_pdf_{year}.csv")
        self.csv_processor.filter_and_process_csv(
            csv_path,
            pdf_output,
            {
                "tender_documents_document_type_details": "Pliego Electrónico de bases y Condiciones",
                "tender_documents_format": "application/pdf",
            },
            "tender_documents_date_published"
        )

        # Filter for Json
        json_output = os.path.join(self.output_dir, f"ten_documents_pliego_json_{year}.csv")
        self.csv_processor.filter_and_process_csv(
            csv_path,
            json_output,
            {
                "tender_documents_document_type_details": "Pliego Electrónico de bases y Condiciones",
                "tender_documents_format": "application/json",
            },
            "tender_documents_date_published"
        )

        # Process and enrich data
        pdf_merged_df = self._process_and_enrich_data(pdf_output, record_csv_path, "PDF")
        json_merged_df = self._process_and_enrich_data(json_output, record_csv_path, "JSON")

        # Load categories
        pdf_merged_df, json_merged_df = self._apply_categories(pdf_merged_df, json_merged_df)

        return pdf_merged_df, json_merged_df

    def _process_and_enrich_data(self, source_path, record_path, file_type):
        """Process and enrich data"""
        self.logger.info("Realizando left join para %s...", file_type)
        source_df = pd.read_csv(source_path)
        record_df = pd.read_csv(record_path)
        merged_df = source_df.merge(record_df, on="tender_id", how="left")

        # Enrich data
        merged_df["nro_licitacion"] = merged_df["open_contracting_id"].apply(self.data_enricher.extract_nro_licitacion)
        merged_df["version_pliego"] = merged_df["tender_documents_url"].apply(self.data_enricher.extract_version_pliego)
        merged_df["categoria"] = merged_df["tender_main_procurement_category_details"].apply(self.data_enricher.extract_categoria)
        merged_df["year"] = merged_df["date"].apply(self.data_enricher.extract_year)

        return merged_df

    def _apply_categories(self, pdf_df, json_df):
        """Apply categories"""
        categories_df = pd.read_csv(self.input_external_dir)
        categories_df.rename(columns={
            "compiledRelease/parties/0/details/categories/0/id": "categoria_id",
            "compiledRelease/parties/0/details/categories/0/name": "categoria"
        }, inplace=True)

        # Merge with categories
        pdf_df = pdf_df.merge(categories_df, on="categoria", how="left")
        json_df = json_df.merge(categories_df, on="categoria", how="left")

        return pdf_df, json_df

    @error_handling(default_return=(None, None))
    def merge_yearly_outputs(self, years, prefix_name):
        """Combines the results of multiple years"""
        pdf_frames = []
        json_frames = []

        for year in years:
            pdf_file = os.path.join(self.output_processed_dir, f"{prefix_name}_pdf_{year}.csv")
            json_file = os.path.join(self.output_processed_dir, f"{prefix_name}_json_{year}.csv")

            if os.path.exists(pdf_file):
                pdf_frames.append(pd.read_csv(pdf_file))
            if os.path.exists(json_file):
                json_frames.append(pd.read_csv(json_file))

        pdf_merged = pd.concat(pdf_frames, ignore_index=True) if pdf_frames else None
        json_merged = pd.concat(json_frames, ignore_index=True) if json_frames else None

        return pdf_merged, json_merged

    @error_handling(default_return=None)
    def filter_unique_tenders(self, df, column_name="nro_licitacion"):
        """Filter unique tenders"""
        if df is not None:
            return df.drop_duplicates(subset=[column_name])
        return None
