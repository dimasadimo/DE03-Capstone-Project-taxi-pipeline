import pandas as pd
import logging  
from pathlib import Path

# Loader class
class Loader:
    
    # Initialize Loader with the folder path for saving the file in csv.
    def __init__(self, folder_path, file_name):
        self.folder_path = Path(folder_path)
        self.file_name = Path(file_name)

    # Save data in the specified folder.
    def load(self, data: pd.DataFrame):
        try:
            logging.info(f"Saving {self.file_name} to {self.folder_path}...")
            self.folder_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
            file_path = self.folder_path / self.file_name
            data.to_csv(file_path, index=False)
            logging.info(f"Data successfully saved to {file_path}")
        except Exception as e:
            logging.error(f"Error load data: {e}", exc_info=True)
            raise