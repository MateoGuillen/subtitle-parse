import asyncio
from utils.file_async_downloader import FileHandler
from utils.file_utility import FileUtility
from utils.csv_utility import CSVUtility

async def main():
    try:
        # Initial configuration
        year = "2025"
        file_type = "json"
        input_root_dir = "./downloads/"
        output_root_dir = "./outputs/"
        json_download_dir = f'./downloads/{file_type}/{year}/'  # Changed name to reflect JSON files
        dataset_path = f'./outputs/ten_documents_pliego_{file_type}_every_year.csv'
        dataset_path_filtered = f'./outputs/ten_documents_pliego_{file_type}_every_year_filtered.csv'
        dataset_path_filtered_limit = f'./outputs/ten_documents_pliego_{file_type}_every_year_filtered_limit_{year}.csv'
        
        # Create necessary directories
        FileUtility.ensure_directory_exists(input_root_dir)
        FileUtility.ensure_directory_exists(output_root_dir)
        FileUtility.ensure_directory_exists(json_download_dir)
        
        # Filter CSV - remains the same as it's independent of file type
        CSVUtility.filter_csv_by_column(
            dataset_path, 
            dataset_path_filtered,
            "nro_licitacion",
            filter_method="unique"
        )
        CSVUtility.filter_by_column_and_limit(
            dataset_path_filtered, 
            dataset_path_filtered_limit,
            "year",
            year 
        )

        # Initialize downloader with JSON-specific directory
        downloader = FileHandler(
            file_path=dataset_path_filtered_limit,
            output_dir=json_download_dir,
            low_memory=False,
            batch_size=1000,
            pause_time=15
        )
        
        # Download JSON files in batches
        total_downloaded = 0
        batch_number = 1
        
        while True:
            # Use the new JSON download method instead of the original batch download
            downloaded_files = await downloader.download_json_files_batch()
            if not downloaded_files:
                break
                
            batch_count = len(downloaded_files)
            total_downloaded += batch_count
            print(f"Batch {batch_number}: Successfully downloaded {batch_count} JSON files")
            print(f"Total files downloaded so far: {total_downloaded}")
            
            if batch_count == downloader.batch_size:
                print(f"Pausing for {downloader.pause_time} seconds before next batch...")
                await asyncio.sleep(downloader.pause_time)
            
            batch_number += 1
        
        print(f"Download complete. Total JSON files downloaded: {total_downloaded}")
                
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())