import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from typing import Tuple, List, Dict
from dataclasses import dataclass
from collections import defaultdict

@dataclass
class ContentSection:
    title: str
    content: List[str]
    page: int
    line_start: int
    line_end: int | None
    depth: int
    document_id: str
    nro_licitacion: str
    category_id: str
    year: str

def setup_logging():
    """Configure logging settings"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('content_extraction.log'),
            logging.StreamHandler()
        ]
    )

def load_dataframes(pdf_lines_path: str, outlines_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and prepare the PDF lines and outlines DataFrames
    """
    logging.info("Loading PDF lines and outlines data...")
    pdf_lines_df = pd.read_parquet(pdf_lines_path)
    outlines_df = pd.read_parquet(outlines_path)
    
    return pdf_lines_df, outlines_df

def preprocess_dataframes(pdf_lines_df: pd.DataFrame, outlines_df: pd.DataFrame) -> Tuple[Dict, pd.DataFrame]:
    """
    Preprocess dataframes to optimize content extraction
    """
    # Create a dictionary for quick access to PDF lines
    pdf_lines_dict = defaultdict(lambda: defaultdict(list))
    
    # Group PDF lines by document_id and page_number
    for _, row in pdf_lines_df.iterrows():
        pdf_lines_dict[row['document_id']][row['page_number']].append({
            'line_number': row['line_number'],
            'line_text': row['line_text']
        })
    
    # Sort lines within each page
    for doc_id in pdf_lines_dict:
        for page in pdf_lines_dict[doc_id]:
            pdf_lines_dict[doc_id][page].sort(key=lambda x: x['line_number'])
    
    # Sort outlines
    outlines_df = outlines_df.sort_values(['document_id', 'page', 'line_number'])
    
    return pdf_lines_dict, outlines_df

def get_section_content(
    pdf_lines_dict: Dict,
    doc_id: str,
    start_page: int,
    end_page: int,
    start_line: int,
    end_line: int | None
) -> List[str]:
    """
    Extract content between two points in the PDF using preprocessed dictionary
    """
    content = []
    
    for page in range(start_page, end_page + 1):
        if page not in pdf_lines_dict[doc_id]:
            continue
            
        page_lines = pdf_lines_dict[doc_id][page]
        
        for line in page_lines:
            line_num = line['line_number']
            
            if page == start_page and line_num < start_line:
                continue
            if page == end_page and end_line is not None and line_num >= end_line:
                break
                
            content.append(line['line_text'])
    
    return content

def extract_content_sections(
    pdf_lines_dict: Dict,
    outlines_df: pd.DataFrame
) -> List[ContentSection]:
    """
    Extract content sections using preprocessed data
    """
    sections = []
    
    # Process each document's outlines
    for doc_id, doc_outlines in outlines_df.groupby('document_id'):
        doc_rows = doc_outlines.to_dict('records')
        
        for i, current in enumerate(doc_rows):
            # Determine section end
            if i < len(doc_rows) - 1:
                next_outline = doc_rows[i + 1]
                end_page = next_outline['page']
                end_line = next_outline['line_number']
            else:
                # For last section, use next page
                end_page = current['page'] + 1
                end_line = None
            
            content = get_section_content(
                pdf_lines_dict,
                doc_id,
                current['page'],
                end_page,
                current['line_number'],
                end_line
            )
            
            section = ContentSection(
                title=current['title'],
                content=content,
                page=current['page'],
                line_start=current['line_number'],
                line_end=end_line,
                depth=current['depth'],
                document_id=doc_id,
                nro_licitacion=current['nro_licitacion'],
                category_id=current['category_id'],
                year=current['year']
            )
            sections.append(section)
    
    return sections

def save_sections_to_parquet(
    sections: List[ContentSection],
    output_path: str = "./outputs/processed_pdf/sections/content_sections.parquet"
):
    """
    Save the extracted sections to a parquet file
    """
    # Create DataFrame directly from a list of dictionaries for better performance
    sections_data = [{
        'document_id': section.document_id,
        'nro_licitacion': section.nro_licitacion,
        'category_id': section.category_id,
        'year': section.year,
        'title': section.title,
        'content': '\n'.join(section.content),
        'page': section.page,
        'line_start': section.line_start,
        'line_end': section.line_end,
        'depth': section.depth,
        'content_length': len(section.content)
    } for section in sections]
    
    df = pd.DataFrame(sections_data)
    
    schema = pa.schema([
        ('document_id', pa.string()),
        ('nro_licitacion', pa.string()),
        ('category_id', pa.string()),
        ('year', pa.string()),
        ('title', pa.string()),
        ('content', pa.string()),
        ('page', pa.int32()),
        ('line_start', pa.int32()),
        ('line_end', pa.int32()),
        ('depth', pa.int32()),
        ('content_length', pa.int32())
    ])
    
    table = pa.Table.from_pandas(df, schema=schema)
    pq.write_table(
        table,
        output_path,
        compression='snappy',
        row_group_size=10000
    )
    
    logging.info(f"Saved {len(sections)} sections to {output_path}")
    return df

def main():
    """Main execution function"""
    setup_logging()
    
    # Define paths
    pdf_lines_path = "./outputs/processed_pdf/parquet/merged_pdf_lines.parquet"
    outlines_path = "./outputs/processed_pdf/merged/merged_outlines_with_lines.parquet"
    output_path = "./outputs/processed_pdf/sections/content_sections.parquet"
    
    try:
        # Load data
        pdf_lines_df, outlines_df = load_dataframes(pdf_lines_path, outlines_path)
        logging.info(f"Loaded {len(outlines_df)} outlines and {len(pdf_lines_df)} PDF lines")
        
        # Preprocess data for faster access
        logging.info("Preprocessing data...")
        pdf_lines_dict, outlines_df = preprocess_dataframes(pdf_lines_df, outlines_df)
        
        # Extract content sections
        logging.info("Extracting content sections...")
        sections = extract_content_sections(pdf_lines_dict, outlines_df)
        logging.info(f"Extracted {len(sections)} content sections")
        
        # Save results
        result_df = save_sections_to_parquet(sections, output_path)
        
        # Print summary
        print("\nExtraction Summary:")
        print(f"Total sections processed: {len(sections)}")
        print(f"Total documents processed: {result_df['document_id'].nunique()}")
        print(f"Total licitaciones processed: {result_df['nro_licitacion'].nunique()}")
        print(f"Average content length: {result_df['content_length'].mean():.2f} lines")
        print(f"Output saved to: {output_path}")
        
    except Exception as e:
        logging.error(f"Error in main process: {e}")
        raise

if __name__ == "__main__":
    main()