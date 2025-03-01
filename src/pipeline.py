import pandas as pd
from extract import OutputExtractor

# ETL Pipeline
class ETLPipeline:
    def __init__(self, extractors, transformer, loader):
        self.extractors = extractors
        self.transformer = transformer
        self.loader = loader

    def run(self):
        try:
            result = []
            for item in self.extractors:
                extracted_data = item.extract()
                result.append(extracted_data)
            data_staging = pd.concat(result, ignore_index=True)
            output_extractor = OutputExtractor(folder_path='../staging', file_name='staging_data.csv')
            output_extractor.save(data_staging)
            # self.transformer.transform(result)
            # self.loader.load(result)
            print('Run Pipeline')
        except: 
            print('Error pipeline')
            raise