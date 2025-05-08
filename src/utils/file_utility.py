""" Utility class for file operations."""
import os
import shutil
import re
import hashlib
from datetime import datetime
from src.utils.logging_utils import setup_logger
from src.utils.error_handler import error_handling

class FileUtility:
    """
    Utility class for various file operations.
    """
    def __init__(self):
        self.logger = setup_logger(__name__)
    
    @staticmethod
    def ensure_directory_exists(directory_path):
        """
        Creates a directory if it doesn't exist.
        
        Args:
            directory_path (str): Path of the directory to create
        
        Returns:
            bool: True if the directory exists or was created successfully
        """
        try:
            os.makedirs(directory_path, exist_ok=True)
            return True
        except Exception as e:
            logger = setup_logger(__name__)
            logger.error("Error creating directory %s: %s", directory_path, str(e))
            return False
    
    @staticmethod
    @error_handling(default_return=None)
    def get_file_checksum(file_path, algorithm='md5', buffer_size=8192):
        """
        Calculates the checksum of a file.
        
        Args:
            file_path (str): Path to the file
            algorithm (str): Hash algorithm to use ('md5', 'sha1', 'sha256')
            buffer_size (int): Size of the buffer for reading the file
        
        Returns:
            str: Hexadecimal checksum or None if error
        """
        if not os.path.exists(file_path) or not os.path.isfile(file_path):
            return None
        
        hash_algorithms = {
            'md5': hashlib.md5(),
            'sha1': hashlib.sha1(),
            'sha256': hashlib.sha256()
        }
        
        if algorithm not in hash_algorithms:
            return None
        
        hasher = hash_algorithms[algorithm]
        
        with open(file_path, 'rb') as file:
            while True:
                data = file.read(buffer_size)
                if not data:
                    break
                hasher.update(data)
        
        return hasher.hexdigest()
    
    @staticmethod
    @error_handling(default_return=0)
    def count_files_in_directory(directory_path, extensions=None, recursive=True):
        """
        Counts files in a directory, optionally filtering by extension.
        
        Args:
            directory_path (str): Directory to count files in
            extensions (list): List of file extensions to include (e.g., ['.pdf', '.txt'])
            recursive (bool): Whether to include subdirectories
        
        Returns:
            int: Number of files
        """
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            return 0
        
        count = 0
        
        if recursive:
            for root, _, files in os.walk(directory_path):
                if extensions:
                    count += sum(1 for f in files if os.path.splitext(f)[1].lower() in extensions)
                else:
                    count += len(files)
        else:
            files = [f for f in os.listdir(directory_path) if os.path.isfile(os.path.join(directory_path, f))]
            if extensions:
                count = sum(1 for f in files if os.path.splitext(f)[1].lower() in extensions)
            else:
                count = len(files)
        
        return count
    
    @staticmethod
    @error_handling(default_return=[])
    def find_files_by_pattern(directory_path, pattern, recursive=True):
        """
        Finds files matching a regex pattern.
        
        Args:
            directory_path (str): Directory to search in
            pattern (str): Regular expression pattern to match filenames
            recursive (bool): Whether to search in subdirectories
        
        Returns:
            list: List of matching file paths
        """
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            return []
        
        pattern_compiled = re.compile(pattern)
        matching_files = []
        
        if recursive:
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if pattern_compiled.search(file):
                        matching_files.append(os.path.join(root, file))
        else:
            for file in os.listdir(directory_path):
                file_path = os.path.join(directory_path, file)
                if os.path.isfile(file_path) and pattern_compiled.search(file):
                    matching_files.append(file_path)
        
        return matching_files
    
    @staticmethod
    @error_handling(default_return=False)
    def backup_directory(source_dir, backup_dir, include_timestamp=True):
        """
        Creates a backup of a directory.
        
        Args:
            source_dir (str): Source directory to backup
            backup_dir (str): Destination directory for the backup
            include_timestamp (bool): Whether to include a timestamp in the backup directory name
        
        Returns:
            bool: True if successful, False otherwise
        """
        if not os.path.exists(source_dir) or not os.path.isdir(source_dir):
            return False
        
        if include_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_dir = f"{backup_dir}_{timestamp}"
        
        try:
            # Create backup directory if it doesn't exist
            os.makedirs(os.path.dirname(backup_dir), exist_ok=True)
            
            # Copy the directory
            shutil.copytree(source_dir, backup_dir)
            return True
        except Exception as e:
            logger = setup_logger(__name__)
            logger.error("Error backing up directory %s to %s: %s", source_dir, backup_dir, str(e))
            return False
    
    @staticmethod
    @error_handling(default_return=(0, 0))
    def analyze_file_sizes(directory_path, extensions=None, recursive=True):
        """
        Analyzes file sizes in a directory.
        
        Args:
            directory_path (str): Directory to analyze
            extensions (list): List of file extensions to include
            recursive (bool): Whether to include subdirectories
        
        Returns:
            tuple: (Total size in bytes, Average size in bytes)
        """
        if not os.path.exists(directory_path) or not os.path.isdir(directory_path):
            return (0, 0)
        
        total_size = 0
        file_count = 0
        
        if recursive:
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if extensions and not any(file.lower().endswith(ext) for ext in extensions):
                        continue
                    
                    file_path = os.path.join(root, file)
                    if os.path.isfile(file_path):
                        file_size = os.path.getsize(file_path)
                        total_size += file_size
                        file_count += 1
        else:
            for file in os.listdir(directory_path):
                if extensions and not any(file.lower().endswith(ext) for ext in extensions):
                    continue
                
                file_path = os.path.join(directory_path, file)
                if os.path.isfile(file_path):
                    file_size = os.path.getsize(file_path)
                    total_size += file_size
                    file_count += 1
        
        avg_size = total_size // file_count if file_count > 0 else 0
        
        return (total_size, avg_size)