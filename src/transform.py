import pandas as pd
import logging

# Transformer class
class Transformer:

    # Initialize Transformer with the folder path containing Staging CSV file.
    def __init__(self, folder_path):
        self.folder_path = folder_path

    # Transform Func to Run all The Transformation Requirements
    def transform(self):
        try:
            logging.info(f"Starting transformation process on file: {self.folder_path}")
            data = pd.read_csv(self.folder_path)
            logging.info("File read successfully.")

            df_trip_duration = self.calculate_trip_duration(data)
            df_normalize_column = self.normalize_column_names(df_trip_duration)
            df_payment_type = self.change_payment_type(df_normalize_column)
            df_convert_distance = self.convert_trip_distance(df_payment_type)

            logging.info("Transformation process completed successfully.")
            return df_convert_distance
        except Exception as e: 
            logging.error(f"Error transforming data: {e}", exc_info=True)
            raise

    # Calculate Trip Duration Func to add duration time from pickup dan dropoff time
    def calculate_trip_duration(self, data):
        try:
            logging.info("Transforming data: Adding trip duration...")
            data["lpep_pickup_datetime"] = pd.to_datetime(data["lpep_pickup_datetime"])
            data["lpep_dropoff_datetime"] = pd.to_datetime(data["lpep_dropoff_datetime"])
            data["trip_duration"] = (data["lpep_dropoff_datetime"] - data["lpep_pickup_datetime"])
            logging.info("Transforming data: Adding trip duration completed.")
            return data
        except Exception as e: 
            logging.error(f"Error Transforming data in Adding trip duration: {e}", exc_info=True)
            raise
    
    # Normalize Column Func to change column name to snake case format
    def normalize_column_names(self, data):
        try:
            logging.info("Transforming data: Normalizing column names to Snake Case...")
            data.columns = data.columns.str.replace(r'(?<=[a-z0-9])([A-Z])', r'_\1', regex=True).str.replace(r'(?<!^)(?=[A-Z][a-z])', '_', regex=True).str.lower()
            logging.info("Transforming data: Normalizing column names to Snake Case completed.")
            return data
        except Exception as e: 
            logging.error(f"Error Transforming data in Normalizing column names to Snake Case: {e}", exc_info=True)
            raise
    
    # Change Payment Type to change payment type to be more readable
    def change_payment_type(self, data):
        try:
            logging.info("Transforming data: Changing payment type value...")
            payment_master = pd.read_csv('../data/payment_type.csv')
            df_new = data.merge(payment_master, on="payment_type", how="left", validate="many_to_one")
            df_new.rename(columns={"description": "payment_description"}, inplace=True)  # Rename new column
            cols = df_new.columns.tolist()
            payment_type_index = cols.index('payment_type')
            payment_description_index = cols.index('payment_description')
            payment_description_col = cols.pop(payment_description_index)
            cols.insert(payment_type_index, payment_description_col)
            df_new = df_new[cols]
            df_new.drop(columns=['payment_type'], inplace=True)
            df_new.rename(columns={"payment_description": 'payment_type'}, inplace=True)
            logging.info("Transforming data: Changing payment type value completed.")
            return df_new
        except Exception as e: 
            logging.error(f"Error Transforming data in Changing payment type value: {e}", exc_info=True)
            raise

    # Convert Trip Distance for miles to km
    def convert_trip_distance(self, data):
        try:
            logging.info("Transforming data: Converting trip distance from miles to kilometers...")
            data['trip_distance'] = (data['trip_distance'] * 1.60934).round(2)
            logging.info("Transforming data: Converting trip distance from miles to kilometers completed.")
            return data
        except Exception as e:
            logging.error(f"Error Transforming data in Converting trip distance from miles to kilometers: {e}", exc_info=True)
            raise
