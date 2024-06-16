# test_raw.py
import sys
import os
import pytest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.raw import HandlerBranchCode
from src.spark_session import SparkSession

TEST_CSV_FILE = 'test_airbnb_data.csv'

@pytest.fixture(scope="module")
def sample_csv():
    data = """id,name,latitude,longitude,zipcode,price
              1,Test Listing,34.0522,-118.2437,90001,100
              2,Another Listing,37.7749,-122.4194,94103,200"""
    with open(TEST_CSV_FILE, 'w') as file:
        file.write(data)
    yield TEST_CSV_FILE
    os.remove(TEST_CSV_FILE)

def test_read_csv(sample_csv):
    df = HandlerBranchCode.read_csv(sample_csv)
    assert df is not None
    assert df.count() == 2
    assert 'id' in df.columns

def test_partition_folder():
    base_path = 'test_raw'
    partition_path = HandlerBranchCode.partition_folder(base_path)
    assert os.path.exists(partition_path)
    os.removedirs(partition_path)

def test_write_data(sample_csv):
    df = HandlerBranchCode.read_csv(sample_csv)
    base_path = 'test_staging'
    output_path = HandlerBranchCode.partition_folder(base_path)
    HandlerBranchCode.write_data(df, output_path)
    parquet_files = [f for f in os.listdir(output_path) if f.endswith('.parquet')]
    assert len(parquet_files) > 0
    for file in parquet_files:
        os.remove(os.path.join(output_path, file))
    os.removedirs(output_path)

if __name__ == "__main__":
    pytest.main()
