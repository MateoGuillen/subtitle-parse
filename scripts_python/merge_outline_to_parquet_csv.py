import pandas as pd
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import logging
from typing import List, Optional

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
    base_path: str = "./outputs/processed_pdf/outlines",
    output_dir: str = "./outputs/processed_pdf/merged"
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
                
                logging.info(f"Successfully read {year_path} with {len(df)} rows")
                all_dataframes.append(df)
            except Exception as e:
                logging.error(f"Error reading {year_path}: {e}")
        else:
            logging.warning(f"File not found: {year_path}")

    if not all_dataframes:
        logging.error("No data frames to merge")
        return None

    # Merge all dataframes
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    logging.info(f"Total rows in merged dataset: {len(merged_df)}")

    try:
        # Save as CSV
        csv_path = output_dir / "merged_outlines.csv"
        merged_df.to_csv(csv_path, index=False, encoding='utf-8')
        logging.info(f"Saved merged CSV to {csv_path}")

        # Save as Parquet
        parquet_path = output_dir / "merged_outlines.parquet"
        
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
        logging.info(f"Saved merged Parquet to {parquet_path}")

        # Print summary statistics
        print("\nMerge Summary:")
        print(f"Total rows: {len(merged_df)}")
        print("\nRows per year:")
        print(merged_df['year'].value_counts().sort_index())
        print("\nRows per category:")
        print(merged_df['category_id'].value_counts().sort_index())

        return merged_df

    except Exception as e:
        logging.error(f"Error saving merged files: {e}")
        return None

def main():
    """Main execution function"""
    # Setup logging
    setup_logging()
    
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