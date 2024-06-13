import os
import pytest
from datetime import datetime
import time
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from src.raw import HandlerBranchCode

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-pyspark").getOrCreate()
    yield spark
    spark.stop()

def test_read_csv_success(spark, tmpdir):
    csv_file = tmpdir.join("test.csv")
    csv_file.write("name,age\nAlice,30\nBob,25")
    df = HandlerBranchCode.read_csv(str(csv_file))

    assert df is not None
    assert df.count() == 2
    assert "name" in df.columns
    assert "age" in df.columns

def test_read_csv_failure(spark):
    df = HandlerBranchCode.read_csv("non_existent_file.csv")    
    assert df is None

def test_partition_folder(tmpdir):
    base_path = str(tmpdir)
    partition_path = HandlerBranchCode.partition_folder(base_path)
    current_date = datetime.now()
    expected_path = os.path.join(base_path, str(current_date.year), str(current_date.month), str(current_date.day))
    assert partition_path == expected_path
    assert os.path.exists(partition_path)

@patch('src.raw.default_timer', return_value=0)
@patch('src.raw.time.sleep', return_value=None)
def test_write_data(mock_sleep, mock_timer, spark, tmpdir):
    
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    output_path = str(tmpdir)
    
    HandlerBranchCode.write_data(df, output_path)
    
    output_files = os.listdir(output_path)
    assert any(file.endswith('.parquet') for file in output_files)

def test_write_data_no_df(spark, capsys):
    HandlerBranchCode.write_data(None, "some_output_path")
    
    captured = capsys.readouterr()
    assert "No data to write." in captured.out
