import os
import logging
import pandas as pd  
from abc import ABC, abstractmethod
from pathlib import Path

# Base Extractor class (Abstract)
class Extractor(ABC):
    @abstractmethod
    def extract(self):
        """Abstract method that must be implemented by subclasses"""
        pass

# CSV Extractor class
class CSVExtractor(Extractor):

    # Initialize CSVExtractor with the folder path containing CSV files.
    def __init__(self, folder_path):
        self.folder_path = folder_path

    # Extract data from all CSV files in the specified folder and Merge them.
    def extract(self):
        try:
            logging.info(f"Extracting CSV files from {self.folder_path}...")
            csv_files = [f for f in os.listdir(self.folder_path) if f.endswith('.csv')]

            if not csv_files: 
                logging.warning("No CSV files found.")
                return None
            
            logging.info(f"Found {len(csv_files)} CSV files. Reading data...")
            csv_data = [pd.read_csv(os.path.join(self.folder_path, f)) for f in csv_files]
            csv_merge = pd.concat(csv_data, ignore_index=True)
            logging.info("CSV data extraction completed successfully.")
            return csv_merge
        except Exception as e:
            logging.error(f"Error extracting CSV data: {e}", exc_info=True)
            raise

# JSON Extractor class
class JSONExtractor(Extractor):

    # Initialize JSONExtractor with the folder path containing JSON files.
    def __init__(self, folder_path):
        self.folder_path = folder_path

    # Extract data from all JSON files in the specified folder and Merge them.
    def extract(self):
        try:
            logging.info(f"Extracting JSON files from {self.folder_path}...")
            json_files = [f for f in os.listdir(self.folder_path) if f.endswith('.json')]
            if not json_files: 
                logging.warning("No JSON files found.")
                return None
            
            logging.info(f"Found {len(json_files)} JSON files. Reading data...")
            json_data = [pd.read_json(os.path.join(self.folder_path, f)) for f in json_files]
            json_merge = pd.concat(json_data, ignore_index=True)
            logging.info("JSON data extraction completed successfully.")
            return json_merge
        except Exception as e:
            logging.error(f"Error extracting JSON data: {e}", exc_info=True)
            raise

# Output Extractor class
class OutputExtractor:
    
    # Initialize OutputExtractor with the folder path for saving the file in csv.
    def __init__(self, folder_path, file_name):
        self.folder_path = Path(folder_path)
        self.file_name = Path(file_name)

    # Save merge data from all CSV & JSON files in the specified folder.
    def save(self, data: pd.DataFrame):
        try:
            logging.info(f"Saving {self.file_name} to {self.folder_path}...")
            self.folder_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
            file_path = self.folder_path / self.file_name
            data.to_csv(file_path, index=False)
            logging.info("Data successfully saved.")
        except Exception as e:
            logging.error(f"Error saving data: {e}", exc_info=True)
            raise