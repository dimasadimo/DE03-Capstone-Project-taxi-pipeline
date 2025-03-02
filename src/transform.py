import pandas as pd 

# Transformer class
class Transformer:

    # Initialize Transformer with the folder path containing Staging CSV file.
    def __init__(self, folder_path):
        self.folder_path = folder_path

    def transform(self):
        try:
            data = pd.read_csv(self.folder_path)
            df_trip_duration = self.calculate_trip_duration(data)
            df_normalize_column = self.normalize_column_names(df_trip_duration)
            df_payment_type = self.change_payment_type(df_normalize_column)
            df_convert_distance = self.convert_trip_distance(df_payment_type)
            return df_convert_distance
        except: 
            print('Error transforming')
            raise

    def calculate_trip_duration(self, data):
        try:
            data["lpep_pickup_datetime"] = pd.to_datetime(data["lpep_pickup_datetime"])
            data["lpep_dropoff_datetime"] = pd.to_datetime(data["lpep_dropoff_datetime"])
            data["trip_duration"] = (data["lpep_dropoff_datetime"] - data["lpep_pickup_datetime"])
            return data
        except: 
            print('Error calculate_trip_duration')
            raise
    
    def normalize_column_names(self, data):
        try:
            data.columns = data.columns.str.replace(r'(?<=[a-z0-9])([A-Z])', r'_\1', regex=True).str.replace(r'(?<!^)(?=[A-Z][a-z])', '_', regex=True).str.lower()
            return data
        except: 
            print('Error normalize_column_names')
            raise
    
    def change_payment_type(self, data):
        try:
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
            return df_new
        except: 
            print('Error  map_payment_type')
            raise

    def convert_trip_distance(self, data):
        try:
            data['trip_distance'] = (data['trip_distance'] * 1.60934).round(2)
            return data
        except: 
            print('Error convert_trip_distance')
            raise
