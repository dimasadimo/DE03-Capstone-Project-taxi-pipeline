import logging
from extract import CSVExtractor, JSONExtractor
from transform import Transformer
from load import Loader
from pipeline import ETLPipeline
        
def main():
    logging.info("Starting the program...")
    
    csv_extractor = CSVExtractor(folder_path='../data/csv/')
    json_extractor = JSONExtractor(folder_path='../data/json/')

    transformer = Transformer('../staging/staging_data.csv')
    loader = Loader(folder_path='../result', file_name='result_data.csv')

    # Your main program logic here
    etl_pipeline = ETLPipeline(extractors= [csv_extractor, json_extractor], transformer= transformer, loader=loader)
    etl_pipeline.run()
    logging.info("Program completed successfully.")

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    
    main()