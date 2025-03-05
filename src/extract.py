import os
import logging
import pandas as pd  
from abc import ABC, abstractmethod
from pathlib import Path
import threading
from queue import Queue

# Base Extractor class (Abstract)
class Extractor(ABC):
    @abstractmethod
    def extract(self):
        """Abstract method that must be implemented by subclasses"""
        pass

    @abstractmethod
    def load(self):
        """Abstract method that must be implemented by subclasses"""
        pass
    
    @abstractmethod
    def worker(self):
        """Abstract method that must be implemented by subclasses"""
        pass

# CSV Extractor class
class CSVExtractor(Extractor):

    # Initialize CSVExtractor with the folder path containing CSV files.
    def __init__(self, folder_path):
        self.folder_path = folder_path

    # Extract data from all CSV files in the specified folder and Merge them.
    def extract(self, num_threads=4):
        try:
            queue = Queue()
            csv_data = []
            lock = threading.Lock()

            # Add CSV files to the queue
            logging.info(f"Extracting CSV files from {self.folder_path}...")
            
            for file in os.listdir(self.folder_path):
                if file.endswith('.csv'):
                    queue.put(os.path.join(self.folder_path, file))

            if not list(queue.queue): 
                logging.warning("No CSV files found.")
                return None

            logging.info(f"Found {len(list(queue.queue))} CSV files. Reading data...")

            # Start threads
            threads = [threading.Thread(target=self.worker, args=(queue, csv_data, lock)) for _ in range(num_threads)]
            for thread in threads: thread.start()
            for thread in threads: thread.join()  # Wait for all threads

            csv_merge = pd.concat(csv_data, ignore_index=True)
            logging.info("CSV data extraction completed successfully.")
            return csv_merge
        except Exception as e:
            logging.error(f"Error extracting CSV data: {e}", exc_info=True)
            raise

    def load(self, file_path=None, csv_data=None, lock=None):
        try:
            df = pd.read_csv(file_path)
            with lock:
                csv_data.append(df)
        except Exception as e: 
            logging.error(f"Error extracting CSV data: {e}", exc_info=True)
            raise
    
    def worker(self, queue=None, csv_data=None, lock=None):
        try:
            while not queue.empty():
                file_path = queue.get()
                self.load(file_path, csv_data, lock)
                queue.task_done()
        except Exception as e: 
            logging.error(f"Error extracting CSV data: {e}", exc_info=True)
            raise

# JSON Extractor class
class JSONExtractor(Extractor):

    # Initialize JSONExtractor with the folder path containing JSON files.
    def __init__(self, folder_path):
        self.folder_path = folder_path

    # Extract data from all JSON files in the specified folder and Merge them.
    def extract(self, num_threads=4):
        try:
            queue = Queue()
            json_data = []
            lock = threading.Lock()

            # Add CSV files to the queue
            logging.info(f"Extracting JSON files from {self.folder_path}...")
            
            for file in os.listdir(self.folder_path):
                if file.endswith('.json'):
                    queue.put(os.path.join(self.folder_path, file))

            if not list(queue.queue): 
                logging.warning("No JSON files found.")
                return None

            logging.info(f"Found {len(list(queue.queue))} JSON files. Reading data...")

            # Start threads
            threads = [threading.Thread(target=self.worker, args=(queue, json_data, lock)) for _ in range(num_threads)]
            for thread in threads: thread.start()
            for thread in threads: thread.join()  # Wait for all threads

            json_merge = pd.concat(json_data, ignore_index=True)
            logging.info("JSON data extraction completed successfully.")
            return json_merge
        except Exception as e:
            logging.error(f"Error extracting JSON data: {e}", exc_info=True)
            raise
    
    def load(self, file_path=None, json_data=None, lock=None):
        try:
            df = pd.read_json(file_path)
            with lock:
                json_data.append(df)
        except Exception as e: 
            logging.error(f"Error extracting JSON data: {e}", exc_info=True)
            raise
    
    def worker(self, queue=None, json_data=None, lock=None):
        try:
            while not queue.empty():
                file_path = queue.get()
                self.load(file_path, json_data, lock)
                queue.task_done()
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