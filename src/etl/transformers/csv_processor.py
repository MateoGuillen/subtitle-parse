"""A module for processing CSV files."""
# src/etl/transformers/csv_processor.py

import pandas as pd
from src.utils.logging_utils import setup_logger

class CSVProcessor:
    """A class for processing CSV files."""

    def __init__(self):
        self.logger = setup_logger(__name__)

    def filter_and_process_csv(self, input_csv, output_csv, column_filters, date_column):
        """Filter and process a CSV file."""
        try:
            self.logger.info("Procesando CSV: %s", input_csv)
            df = pd.read_csv(input_csv)

            for column, value in column_filters.items():
                df = df[df[column] == value]

            if not df.empty:
                df[date_column] = pd.to_datetime(df[date_column])
                df = df.sort_values(by=date_column, ascending=False).drop_duplicates(
                    subset="tender_id", keep="first"
                )

            df.to_csv(output_csv, index=False)
            self.logger.info("CSV procesado guardado en %s", output_csv)
            return output_csv
        except Exception as e:
            self.logger.error("Error al procesar el archivo CSV: %s", str(e))
            raise

    def merge_dataframes(self, frames, output_path):
        """Merge a list of DataFrames and save the result to a CSV file."""
        try:
            if frames:
                result = pd.concat(frames)
                result.to_csv(output_path, index=False)
                self.logger.info("DataFrames combinados y guardados en %s", output_path)
                return output_path
            self.logger.warning("No hay DataFrames para combinar")
            return None
        except Exception as e:
            self.logger.error("Error al combinar DataFrames: %s", str(e))
            raise
