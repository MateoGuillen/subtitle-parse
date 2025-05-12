""" CSV Outline Loader module """
import os
import pandas as pd
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling

class CSVOutlineLoader:
    """
    Loads processed outline data into CSV files.
    
    Attributes:
        output_dir (str): Base output directory.
        logger (logging.Logger): Logger object for logging messages.
    """
    def __init__(self, output_dir):
        self.output_dir = output_dir
        self.logger = setup_logger(__name__)

    @error_handling(default_return='')
    def save_outlines(self, df: pd.DataFrame, year: str) -> str:
        """
        Save outlines to a CSV file.
        
        Args:
            df (pd.DataFrame): DataFrame containing outline data.
            year (str): Year for which the outlines are being saved.
            
        Returns:
            str: Path to the saved CSV file, or None if an error occurred.
        """
        if df is None or df.empty:
            self.logger.warning("No data to save for year %s", year)
            return ''

        # Ensure output directory exists
        output_year_dir = os.path.join(self.output_dir, 'outlines', year)
        os.makedirs(output_year_dir, exist_ok=True)

        # Save to CSV
        output_path = os.path.join(output_year_dir, f'outlines_{year}.csv')
        df.to_csv(output_path, index=False, encoding='utf-8')
        self.logger.info("Outlines saved to %s", output_path)

        return output_path

    @error_handling(default_return=None)
    def merge_yearly_outlines(self, years: list) -> pd.DataFrame:
        """
        Merge outline data from multiple years into a single DataFrame.
        
        Args:
            years (list): List of years to merge.
            
        Returns:
            pd.DataFrame: Merged DataFrame containing outline data from all years.
        """
        all_dfs = []

        for year in years:
            year_str = str(year)
            csv_path = os.path.join(self.output_dir, 'outlines', year_str, f'outlines_{year_str}.csv')

            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path)
                    all_dfs.append(df)
                    self.logger.info("Loaded outlines for year %s", year)
                except Exception as e:
                    self.logger.error("Error loading outlines for year %s: %s", year, e)
            else:
                self.logger.warning("No outline file found for year %s", year)

        if not all_dfs:
            self.logger.warning("No outline data found for any year")
            return None

        merged_df = pd.concat(all_dfs, ignore_index=True)
        self.logger.info("Merged %d outlines from %d years", len(merged_df), len(all_dfs))

        return merged_df
