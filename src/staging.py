import os
import shutil
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, exp, length, format_string, split, regexp_replace, concat_ws, lit
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType

spark = SparkSession.builder.appName("StagingAirBnB").getOrCreate()

class HandlerBranchStaging:
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
    def partition_folder(base_path):
        current_date = datetime.now()
        year = current_date.year
        month = current_date.month
        day = current_date.day

        path = os.path.join(base_path, str(year), str(month), str(day))
        os.makedirs(path, exist_ok=True)
        return path

    @staticmethod
    def clean_data(df):
        df = df.dropna(subset=['zipcode'])
        df = df.filter((length(col('zipcode')) == 5) | col('zipcode').endswith('.0'))
        df = df.withColumn('zipcode', format_string('%05d', col('zipcode').cast('double').cast('int')))
        df = df.withColumn('price', exp(col('log_price')))

        columns_to_remove = ["description", "name", "thumbnail_url", "processed_date"]
        for column in columns_to_remove:
            if column in df.columns:
                df = df.drop(column)

        df = df.withColumn('price', col('price').cast('int'))
        df = df.withColumn('latitude', format_string('%.6f', col('latitude')))
        df = df.withColumn('longitude', format_string('%.6f', col('longitude')))
        df = df.withColumn("amenities", split(regexp_replace(col("amenities"), '[\\[\\]\"]', ''), ',\s*').cast(ArrayType(StringType())))
        df = df.withColumn("amenities", concat_ws(", ", col("amenities")))
        return df

    @staticmethod
    def process_cleaned_data(df):
        amenities_df = df.select(F.explode(F.split(F.col("amenities"), ",")).alias("amenity"))
        amenities_list = [row['amenity'] for row in amenities_df.select(F.trim(F.col("amenity")).alias("amenity")).distinct().collect()]

        
        for amenity in amenities_list:
            cleaned_amenity = amenity.strip()
            df = df.withColumn(cleaned_amenity, F.col("amenities").contains(cleaned_amenity).cast("int"))
        
        if "Firm mattress" in df.columns and "Firm matress" in df.columns:
            df = df.withColumn("Firm mattress", F.col("Firm mattress") + F.col("Firm matress"))
            df = df.drop("Firm matress")

        if "Smart lock" in df.columns and "Smartlock" in df.columns:
            df = df.withColumn("Smart lock", F.col("Smart lock") + F.col("Smartlock"))
            df = df.drop("Smartlock")

        if "Wide clearance to shower and toilet" in df.columns and "Wide clearance to shower & toilet" in df.columns:
            df = df.withColumn("Wide clearance to shower and toilet", F.col("Wide clearance to shower and toilet") + F.col("Wide clearance to shower & toilet"))
            df = df.drop("Wide clearance to shower & toilet")

        df = df.withColumn("Firm mattress", (F.col("Firm mattress") > 0).cast("int"))
        df = df.withColumn("Smart lock", (F.col("Smart lock") > 0).cast("int"))
        df = df.withColumn("Wide clearance to shower and toilet", (F.col("Wide clearance to shower and toilet") > 0).cast("int"))

        df = df.drop("amenities")

        return df

    @staticmethod
    def process_latest_raw():
        raw_path = HandlerBranchStaging.partition_folder('C:/Users/ppatinog/OneDrive - NTT DATA EMEAL/Escritorio/ProyectoFinal/raw')
        latest_file = HandlerBranchStaging.get_latest_parquet_file(raw_path)

        if latest_file:
            print(f"Processing file: {latest_file}")
            df = spark.read.parquet(latest_file)
            cleaned_df = HandlerBranchStaging.clean_data(df)
            
            if cleaned_df.count() > 0:
                staging_path = HandlerBranchStaging.partition_folder('C:/Users/ppatinog/OneDrive - NTT DATA EMEAL/Escritorio/ProyectoFinal/staging')
                output_path = os.path.join(staging_path, "cleaned_data.parquet")

                temp_output_path = os.path.join(staging_path, "temp_output")
                cleaned_df.coalesce(1).write.mode("overwrite").parquet(temp_output_path)

                temp_file = [f for f in os.listdir(temp_output_path) if f.endswith('.parquet')][0]
                os.rename(os.path.join(temp_output_path, temp_file), output_path)

                shutil.rmtree(temp_output_path)

                print(f"Data cleaned and saved to {output_path}")

                # Process the cleaned data further
                processed_cleaned_df = HandlerBranchStaging.process_cleaned_data(cleaned_df)
                output_processed_path = os.path.join(staging_path, "processed_cleaned_data.parquet")

                temp_output_processed_path = os.path.join(staging_path, "temp_output_processed")
                processed_cleaned_df.coalesce(1).write.mode("overwrite").parquet(temp_output_processed_path)

                temp_file_processed = [f for f in os.listdir(temp_output_processed_path) if f.endswith('.parquet')][0]
                os.rename(os.path.join(temp_output_processed_path, temp_file_processed), output_processed_path)

                shutil.rmtree(temp_output_processed_path)

                print(f"Processed cleaned data saved to {output_processed_path}")

            else:
                print("DataFrame is empty after cleaning. No file was saved.")
        else:
            print("No raw data file found to process.")

if __name__ == "__main__":
    HandlerBranchStaging.process_latest_raw()