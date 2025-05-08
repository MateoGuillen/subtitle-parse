""" File loader for saving and organizing downloaded files."""
import os
import shutil
import pandas as pd
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling

class FileLoader:
    """
    Class responsible for saving and organizing downloaded files.
    This class provides methods for moving files, creating summaries,
    and organizing the directory structure.
    """
    def __init__(self, output_dir):
        """
        Initialize the FileLoader.
        
        Args:
            output_dir (str): Base directory for output files
        """
        self.output_dir = output_dir
        self.logger = setup_logger(__name__)

    @error_handling(default_return=False)
    def organize_files_by_category(self, category_column='categoria_id'):
        """
        Organizes downloaded files into subdirectories based on their category.
        
        Args:
            category_column (str): Column name for categorization
        
        Returns:
            bool: True if successful, False otherwise
        """
        # Get a list of all files in the output directory
        all_files = [f for f in os.listdir(self.output_dir)
                        if os.path.isfile(os.path.join(self.output_dir, f))]

        moved_count = 0
        for file_name in all_files:
            # Parse category from filename (format: date_category_id_licitacion.ext)
            parts = file_name.split('_')
            if len(parts) < 3:
                continue

            try:
                category_id = parts[1]
                # Create category directory if it doesn't exist
                category_dir = os.path.join(self.output_dir, f"category_{category_id}")
                os.makedirs(category_dir, exist_ok=True)

                # Move file to category directory
                src_path = os.path.join(self.output_dir, file_name)
                dst_path = os.path.join(category_dir, file_name)

                if not os.path.exists(dst_path):
                    shutil.move(src_path, dst_path)
                    moved_count += 1
            except (IndexError, ValueError):
                self.logger.warning("Could not parse category from filename: %s", file_name)

        self.logger.info("Organized %s files into category directories", moved_count)
        return True

    @error_handling(default_return=None)
    def create_download_summary(self, summary_name="download_summary.csv"):
        """
        Creates a summary CSV of downloaded files.
        
        Args:
            summary_name (str): Name of the summary CSV file
        
        Returns:
            str: Path to the summary file if successful, None otherwise
        """
            # Find all files in the output directory and subdirectories
        file_data = []

        for root, _, files in os.walk(self.output_dir):
            for file_name in files:
                if file_name.endswith(('.pdf', '.json', '.zip', '.rar')):
                    full_path = os.path.join(root, file_name)
                    relative_path = os.path.relpath(full_path, self.output_dir)

                    # Parse file information from name
                    parts = os.path.splitext(file_name)[0].split('_')

                    file_info = {
                        'file_name': file_name,
                        'path': relative_path,
                        'size_kb': round(os.path.getsize(full_path) / 1024, 2),
                        'date_modified': pd.Timestamp(os.path.getmtime(full_path), unit='s')
                    }

                    # Add parsed information if available
                    if len(parts) >= 3:
                        try:
                            file_info['year'] = parts[0]
                            file_info['category_id'] = parts[1]
                            file_info['nro_licitacion'] = parts[2]
                        except (IndexError, ValueError):
                            pass

                    file_data.append(file_info)

        # Create DataFrame and save summary
        if file_data:
            summary_df = pd.DataFrame(file_data)
            summary_path = os.path.join(self.output_dir, summary_name)
            summary_df.to_csv(summary_path, index=False)
            self.logger.info("Download summary created: %s", summary_path)
            return summary_path
        else:
            self.logger.warning("No files found to include in summary")
            return None

    @error_handling(default_return=False)
    def clean_temporary_files(self, extensions=('.tmp', '.part')):
        """
        Cleans temporary files from the output directory.
        
        Args:
            extensions (tuple): File extensions to consider as temporary
        
        Returns:
            bool: True if successful, False otherwise
        """

        removed_count = 0
        for root, _, files in os.walk(self.output_dir):
            for file_name in files:
                if file_name.endswith(extensions):
                    file_path = os.path.join(root, file_name)
                    os.remove(file_path)
                    removed_count += 1

        self.logger.info("Cleaned up %s temporary files", removed_count)
        return True
