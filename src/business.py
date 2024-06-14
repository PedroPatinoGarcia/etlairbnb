from staging import HandlerBranchStaging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, lit
import os
import shutil
from datetime import datetime

spark = SparkSession.builder.appName("BusinessAirBnB").getOrCreate()

class HandlerBranchBusiness:
    @staticmethod
    def get_latest_parquet_file(directory):
        try:
            files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.parquet')]
            if not files:
                raise ValueError(f"No Parquet files found in {directory}.")
            latest_file = max(files, key=os.path.getctime)
            return latest_file
        except Exception as e:
            print(f"Error getting the latest Parquet file: {e}")
            return None

    @staticmethod
    def process_data(df):
        df = df.withColumn('processed_date', lit(current_date()))
        return df

    @staticmethod
    def partition_folder(base_path):
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day

        path = os.path.join(base_path, str(year), str(month), str(day))
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def export_to_csv(df, output_path):
        temp_output_path = os.path.join(output_path, "temp_csv_output")
        df.coalesce(1).write.mode("overwrite").csv(temp_output_path, header=True)

        temp_file = [f for f in os.listdir(temp_output_path) if f.endswith('.csv')][0]
        final_csv_path = os.path.join(output_path, f"data-{datetime.now().strftime('%Y-%m-%d')}.csv")
        os.rename(os.path.join(temp_output_path, temp_file), final_csv_path)

        shutil.rmtree(temp_output_path)
        
        print(f"Data exported to CSV at {final_csv_path}")

    @staticmethod
    def process_latest_staging():
        staging_path = HandlerBranchStaging.partition_folder('staging')
        latest_file = HandlerBranchBusiness.get_latest_parquet_file(staging_path)

        if latest_file:
            print(f"Processing file: {latest_file}")
            df = spark.read.parquet(latest_file)
            business_df = HandlerBranchBusiness.process_data(df)
            
            if business_df.count() > 0:
                business_path = HandlerBranchBusiness.partition_folder('business')
                output_path = os.path.join(business_path, f"data-{datetime.now().strftime('%Y-%m-%d')}.parquet")

                temp_output_path = os.path.join(business_path, "temp_output")
                business_df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

                temp_file = [f for f in os.listdir(temp_output_path) if f.endswith('.parquet')][0]
                os.rename(os.path.join(temp_output_path, temp_file), output_path)

                shutil.rmtree(temp_output_path)

                print(f"Data processed and saved to {output_path}")

                HandlerBranchBusiness.export_to_csv(business_df, business_path)
            else:
                print("DataFrame is empty after business processing. No file was saved.")
        else:
            print("No staging data file found to process.")

if __name__ == "__main__":
    HandlerBranchBusiness.process_latest_staging()