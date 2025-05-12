""" Merge outline CSV files from multiple years into a single CSV and Parquet file """
from pathlib import Path
from typing import List, Optional
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling
from config.settings import BASE_OUTPUT_PROCESSED_DIR

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('merge_outlines.log'),
            logging.StreamHandler()
        ]
    )

def merge_outline_files(
    years: List[int],
    base_path: str = f'{BASE_OUTPUT_PROCESSED_DIR}/outlines',
    output_dir: str = f"{BASE_OUTPUT_PROCESSED_DIR}/csv",
    output_dir_parquet: str = f"{BASE_OUTPUT_PROCESSED_DIR}/parquet"
) -> Optional[pd.DataFrame]:
    """
    Merge outline CSV files from multiple years into a single CSV and Parquet file

    Args:
        years: List of years to process
        base_path: Base directory containing year folders
        output_dir: Directory to save merged files

    Returns:
        Optional[pd.DataFrame]: Merged DataFrame if successful, None otherwise
    """
    all_dataframes = []
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_dir_parquet = Path(output_dir_parquet)
    output_dir_parquet.mkdir(parents=True, exist_ok=True)

    # Process each year
    for year in tqdm(years, desc="Processing years"):
        year_path = Path(base_path) / str(year) / f"outlines_{year}.csv"

        if year_path.exists():
            try:
                # Read CSV with proper data types
                df = pd.read_csv(year_path, dtype={
                    'document_id': str,
                    'year': str,
                    'category_id': str,
                    'nro_licitacion': str,
                    'title': str,
                    'page': int,
                    'depth': int
                })

                logging.info("Successfully read %s with %d rows", year_path, len(df))
                all_dataframes.append(df)
            except Exception as e:
                logging.error("Error reading %s: %s", year_path, e)
        else:
            logging.warning("File not found: %s", year_path)

    if not all_dataframes:
        logging.error("No data frames to merge")
        return None

    # Merge all dataframes
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    logging.info("Total rows in merged dataset: %d", len(merged_df))

    try:
        # Save as CSV
        csv_path = output_dir / "merged_outlines.csv"
        merged_df.to_csv(csv_path, index=False, encoding='utf-8')
        logging.info("Saved merged CSV to %s", csv_path)

        # Save as Parquet
        parquet_path = output_dir_parquet / "merged_outlines.parquet"

        # Define schema for Parquet
        schema = pa.schema([
            ('document_id', pa.string()),
            ('year', pa.string()),
            ('category_id', pa.string()),
            ('nro_licitacion', pa.string()),
            ('title', pa.string()),
            ('page', pa.int32()),
            ('depth', pa.int32())
        ])

        # Convert to Parquet with compression
        table = pa.Table.from_pandas(merged_df, schema=schema)
        pq.write_table(
            table,
            parquet_path,
            compression='snappy',
            row_group_size=10000
        )
        logging.info("Saved merged Parquet to %s", parquet_path)

        # Log summary statistics
        logging.info("Merge Summary:")
        logging.info("Total rows: %d", len(merged_df))

        rows_per_year = merged_df['year'].value_counts().sort_index()
        logging.info("Rows per year:\n%s", rows_per_year.to_string())

        rows_per_category = merged_df['category_id'].value_counts().sort_index()
        logging.info("Rows per category:\n%s", rows_per_category.to_string())

        return merged_df

    except Exception as e:
        logging.error("Error saving merged files: %s", e)
        return None

def main():
    """Main execution function"""
    # Setup logging
    setup_logger(__name__)

    # Define years to process
    years = [2021, 2022, 2023, 2024, 2025]

    # Execute merge
    result_df = merge_outline_files(years)

    if result_df is not None:
        logging.info("Merge completed successfully")
    else:
        logging.error("Merge failed")

if __name__ == "__main__":
    main()
