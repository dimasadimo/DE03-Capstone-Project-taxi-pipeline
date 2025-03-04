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

        # User input to choose format result file
    
        while True:
            try:
                user_input = input("\nChoose format for result file:\n1. CSV\n2. Excel\nEnter choice (1 or 2): ").strip().lower()
                
                if user_input == "1":
                    logging.info(f"Saving result_data.csv to {self.folder_path} as csv file...")
                    self.folder_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
                    file_path = self.folder_path / 'result_data.csv'
                    data.to_csv(file_path, index=False)
                    logging.info(f"Data successfully saved to {file_path}")
                    break
                elif user_input == "2":
                    logging.info(f"Saving result_data.xlsx to {self.folder_path} as excel file...")
                    self.folder_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
                    file_path = self.folder_path / 'result_data.xlsx'
                    data.to_excel(file_path, index=False, engine='openpyxl')
                    logging.info(f"Data successfully saved to {file_path}")
                    break
                else:
                    print("\nInvalid input. Please type '1' to export the data in CSV or '2' in Excel format.")
            except Exception as e:
                logging.error(f"Error load data: {e}", exc_info=True)
                raise