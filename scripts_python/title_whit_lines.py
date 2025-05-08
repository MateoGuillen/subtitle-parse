import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import re

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('match_titles_lines.log'),
            logging.StreamHandler()
        ]
    )

def clean_text(texto: str) -> str:
    """
    Function to clean duplicate text and replace hyphens
    """
    if pd.isna(texto):
        return texto
    texto = re.sub(r'(\w)\1+', r'\1', texto)  # Remove duplicate letters
    texto = texto.replace("--", "-")  # Replace double hyphens with single hyphen
    texto = texto.replace("::", ":")  # Replace multiple consecutive colons
    texto = texto.replace("..", ".")  # Replace consecutive periods
    texto = re.sub(r',+', ',', texto)  # Replace multiple commas with a single comma
    return re.sub(r'\s+', ' ', texto).strip()  # Clean extra spaces

def match_titles_with_lines(
    outlines_path: str = "./outputs/processed_pdf/merged/merged_outlines.parquet",
    pdf_lines_path: str = "./outputs/processed_pdf/parquet/merged_pdf_lines.parquet",
    output_path: str = "./outputs/processed_pdf/merged/merged_outlines_with_lines.parquet"
):
    """
    Match outline titles with their corresponding line numbers using DataFrame merge
    """
    try:
        # Read DataFrames
        logging.info("Loading data...")
        outlines_df = pd.read_parquet(outlines_path)
        pdf_lines_df = pd.read_parquet(pdf_lines_path)
        
        logging.info(f"Loaded {len(outlines_df)} outline entries and {len(pdf_lines_df)} PDF lines")

        # Clean both title and line_text
        logging.info("Cleaning text fields...")
        outlines_df['clean_title'] = outlines_df['title'].apply(clean_text)
        pdf_lines_df['clean_line_text'] = pdf_lines_df['line_text'].apply(clean_text)

        # Perform the merge using cleaned fields
        logging.info("Merging datasets...")
        merged_df = pd.merge(
            outlines_df,
            pdf_lines_df[['document_id', 'page_number', 'line_text', 'clean_line_text', 'line_number']],
            left_on=['document_id', 'page', 'clean_title'],
            right_on=['document_id', 'page_number', 'clean_line_text'],
            how='left'
        )

        # Drop temporary and duplicate columns
        merged_df = merged_df.drop(['page_number', 'line_text', 'clean_title', 'clean_line_text'], axis=1)

        # Save updated outlines
        logging.info("Saving results...")
        schema = pa.schema([
            ('document_id', pa.string()),
            ('year', pa.string()),
            ('category_id', pa.string()),
            ('nro_licitacion', pa.string()),
            ('title', pa.string()),
            ('page', pa.int32()),
            ('depth', pa.int32()),
            ('line_number', pa.int32())
        ])

        table = pa.Table.from_pandas(merged_df, schema=schema)
        pq.write_table(
            table,
            output_path,
            compression='snappy',
            row_group_size=10000
        )

        # Print summary
        total_matches = merged_df['line_number'].notna().sum()
        print("\nMatching Summary:")
        print(f"Total outline entries: {len(merged_df)}")
        print(f"Successfully matched: {total_matches}")
        print(f"Match rate: {(total_matches/len(merged_df))*100:.2f}%")

        return merged_df

    except Exception as e:
        logging.error(f"Error in main process: {e}")
        raise

def main():
    """Main execution function"""
    setup_logging()
    
    try:
        result_df = match_titles_with_lines()
        logging.info("Title-line matching completed successfully")
    except Exception as e:
        logging.error(f"Title-line matching failed: {e}")

if __name__ == "__main__":
    main()