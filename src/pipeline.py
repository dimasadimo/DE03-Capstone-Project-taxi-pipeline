import logging
import pandas as pd
from extract import OutputExtractor

# ETL Pipeline class
class ETLPipeline:

    # Initialize Extractor, Transformer & Loader
    def __init__(self, extractors, transformer, loader):
        self.extractors = extractors
        self.transformer = transformer
        self.loader = loader

    # Run Func to run all Extractor, Transformer & Loader
    def run(self):
        try:
            logging.info("Pipeline execution started.")
            result = []
            for item in self.extractors:
                extracted_data = item.extract()
                result.append(extracted_data)

            logging.info("Merging extracted data...")
            data_staging = pd.concat(result, ignore_index=True)

            output_extractor = OutputExtractor(folder_path='../staging', file_name='staging_data.csv')
            output_extractor.save(data_staging)
            final_result = self.transformer.transform()
            self.loader.load(final_result)
            logging.info("ETL pipeline executed successfully.")
        except Exception as e: 
            logging.error(f"Error in pipeline execution: {e}", exc_info=True)
            raise