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
                    self.show(df=data)
                    break
                elif user_input == "2":
                    logging.info(f"Saving result_data.xlsx to {self.folder_path} as excel file...")
                    self.folder_path.parent.mkdir(parents=True, exist_ok=True)  # Create directory if it doesn't exist
                    file_path = self.folder_path / 'result_data.xlsx'
                    data.to_excel(file_path, index=False, engine='openpyxl')
                    logging.info(f"Data successfully saved to {file_path}")
                    self.show(df=data)
                    break
                else:
                    print("\nInvalid input. Please type '1' to export the data in CSV or '2' in Excel format.")
            except Exception as e:
                logging.error(f"Error load data: {e}", exc_info=True)
                raise

    def show(self, df: pd.DataFrame):
        try:
            print(df.head(5))
            print("\n**Dataset Taxi Trip Overview**")
            print("=" * 50)
            print(f"Total Data: {df.shape[0]}")
            print(f"Total Columns: {df.shape[1]}")
            print("\n")

            print("**General Summary**")
            print("=" * 50)

            most_common_pu = df['pu_location_id'].value_counts().idxmax()
            most_common_do = df['do_location_id'].value_counts().idxmax()

            zone_master = pd.read_csv('../data/taxi_zone_lookup.csv')

            print(f"\nPickup Time Range: {df['lpep_pickup_datetime'].min()} - {df['lpep_pickup_datetime'].max()}")
            print(f"\nDropoff Range: {df['lpep_dropoff_datetime'].min()} - {df['lpep_dropoff_datetime'].max()}")
            print(f"\nPickup Most Common Zone: {zone_master.loc[zone_master['LocationID'] == most_common_pu, 'Zone'].values[0]}")
            print(f"\nDropoff Most Common Zone: {zone_master.loc[zone_master['LocationID'] == most_common_do, 'Zone'].values[0]}")

            print("\nTrip Duration: ")
            print(f"   - Min Value: {df['trip_duration'].min()}")
            print(f"   - Max Value: {df['trip_duration'].max()}")
            print(f"   - Average: {df['trip_duration'].mean()}")

            print("\nTotal Passagers: ")
            print(f"   - Min Value: {df['passenger_count'].min()}")
            print(f"   - Max Value: {df['passenger_count'].max()}")
            print(f"   - Average Value: {round(df['passenger_count'].mean())}")

            print("\nFare Amount: ")
            print(f"   - Min Value: USD {df['fare_amount'].min()}")
            print(f"   - Max Value: USD {df['fare_amount'].max()}")
            print(f"   - Average Value: USD {df['fare_amount'].mean():.2f}")
            print(f"   - Total Value: USD {df['fare_amount'].sum():.2f}")

            print("\nTotal Amount: ")
            print(f"   - Min Value: USD {df['total_amount'].min()}")
            print(f"   - Max Value: USD {df['total_amount'].max()}")
            print(f"   - Average Value: USD {df['total_amount'].mean():.2f}")
            print(f"   - Total Value: USD {df['total_amount'].sum():.2f}")

            avg_tip_percentage = df['tip_amount'].mean() / df['fare_amount'].mean() * 100

            print(f"\nAverage Tip Percentage: {avg_tip_percentage:.2f}%")

            tolls_applied = (df['tolls_amount'] > 0).sum()
            mta_tax_applied = (df['mta_tax'] > 0).sum()
            congestion_applied = (df['congestion_surcharge'] > 0).sum()
            improvement_applied = (df['improvement_surcharge'] > 0).sum()

            # Contribution to total fare (percentage)
            tolls_contribution = (df['tolls_amount'].sum() / df['total_amount'].sum()) * 100
            mta_tax_contribution = (df['mta_tax'].sum() / df['total_amount'].sum()) * 100
            congestion_contribution = (df['congestion_surcharge'].sum() / df['total_amount'].sum()) * 100
            improvement_contribution = (df['improvement_surcharge'].sum() / df['total_amount'].sum()) * 100

            
            print(f"\nTolls applied in {tolls_applied} trips ({tolls_contribution:.2f}% of total revenue)")
            print(f"\nMTA Tax applied in {mta_tax_applied} trips ({mta_tax_contribution:.2f}% of total revenue)")
            print(f"\nCongestion Surcharge applied in {congestion_applied} trips ({congestion_contribution:.2f}% of total revenue)")
            print(f"\nImprovement Surcharge applied in {improvement_applied} trips ({improvement_contribution:.2f}% of total revenue)")

            print("\nTrip Distance: ")
            print(f"   - Min Value: {df['trip_distance'].min()} km")
            print(f"   - Max Value: {df['trip_distance'].max()} km")
            print(f"   - Average Value: {df['trip_distance'].mean():.2f} km")
            print(f"   - Total Value: {df['trip_distance'].sum():.2f} km")

            payment_counts = df['payment_type'].value_counts()
            total = payment_counts.sum()
            payment_percent = (payment_counts / total) * 100

            print("\nPayment Method Usage:")
            print(f"   - Cash: {payment_percent.get('Cash', 0):.0f}%")
            print(f"   - Card: {payment_percent.get('Credit Card', 0):.0f}%")

            print("\n✅ **Summary Completed**")
        except Exception as e:
                logging.error(f"Error show summary data: {e}", exc_info=True)
                raise   