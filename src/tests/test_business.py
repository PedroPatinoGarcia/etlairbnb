# test_business.py
import sys
import os
import shutil
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from src.business import HandlerBranchBusiness
from src.spark_session import spark

@pytest.fixture(scope="module")
def setup_directories():
    os.makedirs('test_staging', exist_ok=True)
    os.makedirs('test_business', exist_ok=True)
    yield
    shutil.rmtree('test_staging')
    shutil.rmtree('test_business')

def create_sample_parquet(directory):
    data = [
        (["wifi", "kitchen"], "Some description"),
        (["gym", "pool"], "Another description")
    ]
    schema = StructType([
        StructField("amenities", ArrayType(StringType()), True),
        StructField("description", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    path = os.path.join(directory, "sample.parquet")
    df.write.mode("overwrite").parquet(path)
    return path

def test_get_latest_parquet_files(setup_directories):
    sample_path = create_sample_parquet('test_staging')
    files = HandlerBranchBusiness.get_latest_parquet_files('test_staging')
    assert len(files) > 0
    assert sample_path in files

def test_process_data():
    data = [
        (["wifi", "kitchen"], "Some description"),
        (["gym", "pool"], "Another description")
    ]
    schema = StructType([
        StructField("amenities", ArrayType(StringType()), True),
        StructField("description", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    processed_df = HandlerBranchBusiness.process_data(df)
    assert "amenities" in processed_df.columns
    processed_df.show(truncate=False)
    assert processed_df.filter(processed_df["amenities"].contains("wifi")).count() == 1

def test_partition_folder():
    base_path = 'test_raw'
    partition_path = HandlerBranchBusiness.partition_folder(base_path)
    assert os.path.exists(partition_path)
    shutil.rmtree(partition_path)

def test_export_to_csv(setup_directories):
    data = [
        (["wifi", "kitchen"], "Some description"),
        (["gym", "pool"], "Another description")
    ]
    schema = StructType([
        StructField("amenities", ArrayType(StringType()), True),
        StructField("description", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema)
    
    output_path = 'test_business'
    csv_name = "test_data"
    HandlerBranchBusiness.export_to_csv(df, output_path, csv_name)
    
    csv_files = [f for f in os.listdir(output_path) if f.endswith('.csv')]
    assert len(csv_files) > 0
    for file in csv_files:
        os.remove(os.path.join(output_path, file))

if __name__ == "__main__":
    pytest.main()
