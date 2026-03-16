import os
import glob
from config.cleanup_config import CLEANUP_DIRS, CLEANUP_FILES_PATTERN
def run_cleanup():
    for directory in CLEANUP_DIRS:
        if not os.path.exists(directory):
            print(f"Directory {directory} does not exist. Skipping.")
            continue
        # Remove temporary files
        temp_files = glob.glob(os.path.join(directory, CLEANUP_FILES_PATTERN))
        for file_path in temp_files:
            try:
                os.remove(file_path)
                print(f'Removed temporary file: {file_path}')
            except Exception as e:
                print(f'Error removing temporary file {file_path}: {e}')
    # Additional cleanup tasks can be added here
if __name__ == '__main__':
    run_cleanup()
