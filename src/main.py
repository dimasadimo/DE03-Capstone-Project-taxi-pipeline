import logging
from extract import CSVExtractor, JSONExtractor
from transform import Transformer
from load import Loader
from pipeline import ETLPipeline
        
def main():
    logging.info("Starting ETL program...")
    
    # Initialize Extractor, Transformer & Loader
    csv_extractor = CSVExtractor(folder_path='../data/csv/')
    json_extractor = JSONExtractor(folder_path='../data/json/')

    transformer = Transformer('../staging/staging_data.csv')
    loader = Loader(folder_path='../result', file_name='result_data.csv')

    # Run ETL Function
    etl_pipeline = ETLPipeline(extractors= [csv_extractor, json_extractor], transformer= transformer, loader=loader)
    etl_pipeline.run()
    
    logging.info("Program completed successfully.")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Configure logging to save in app.log
    root_logger = logging.getLogger()

    file_handler = logging.FileHandler('./logs/app.log')
    file_handler.setLevel(logging.DEBUG)  # Log all levels to the file
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)

    root_logger.addHandler(file_handler)
    
    # User input to start ETL
    while True:
        user_input = input("Start the ETL process(y/n): ").strip().lower()
        
        if user_input == "y":
            main()
            break
        elif user_input == "n":
            print("Exiting program.")
            break
        else:
            print("Invalid input. Please type 'y' to start the ETL process or 'n' to quit.")
