# tests/test_business.py

import os
import pytest
import shutil
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from datetime import datetime
from src.business import HandlerBusiness

@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-pyspark").getOrCreate()
    yield spark
    spark.stop()

def test_get_latest_parquet_file(tmpdir):
    dir_path = tmpdir.mkdir("parquet_files")
    file1 = dir_path.join("file1.parquet")
    file2 = dir_path.join("file2.parquet")
    file1.write("")
    file2.write("")

    os.utime(file1, (datetime.now().timestamp(), datetime.now().timestamp()))
    os.utime(file2, (datetime.now().timestamp() - 10, datetime.now().timestamp() - 10))

    latest_file = HandlerBusiness.get_latest_parquet_file(str(dir_path))
    
    assert latest_file == str(file1)

def test_get_latest_parquet_file_no_files(tmpdir):
    dir_path = tmpdir.mkdir("empty_parquet_files")

    latest_file = HandlerBusiness.get_latest_parquet_file(str(dir_path))
    
    assert latest_file is None

def test_process_data(spark):
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    processed_df = HandlerBusiness.process_data(df)
    
    assert "processed_date" in processed_df.columns

def test_partition_folder(tmpdir):
    base_path = str(tmpdir)
    
    partition_path = HandlerBusiness.partition_folder(base_path)
    
    current_date = datetime.now()
    expected_path = os.path.join(base_path, str(current_date.year), str(current_date.month), str(current_date.day))
    assert partition_path == expected_path
    assert os.path.exists(partition_path)

@patch('shutil.rmtree')
@patch('os.rename')
def test_export_to_csv(mock_rename, mock_rmtree, spark, tmpdir):
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    output_path = str(tmpdir)
    
    HandlerBusiness.export_to_csv(df, output_path)
    
    temp_output_path = os.path.join(output_path, "temp_csv_output")
    mock_rename.assert_called_once()
    mock_rmtree.assert_called_once_with(temp_output_path)

@patch('src.business.HandlerStaging.partition_folder')
@patch('src.business.HandlerBusiness.get_latest_parquet_file')
@patch('shutil.rmtree')
@patch('os.rename')
def test_process_latest_staging(mock_rename, mock_rmtree, mock_get_latest_parquet, mock_partition_folder, spark, tmpdir):
    staging_path = str(tmpdir.mkdir("staging"))
    business_path = str(tmpdir.mkdir("business"))
    mock_partition_folder.side_effect = [staging_path, business_path]
    
    data = [("Alice", 30), ("Bob", 25)]
    df = spark.createDataFrame(data, ["name", "age"])
    temp_parquet_file = staging_path + "/temp.parquet"
    df.write.mode("overwrite").parquet(temp_parquet_file)
    
    mock_get_latest_parquet.return_value = temp_parquet_file
    
    with patch('src.business.spark.read.parquet', return_value=df):
        HandlerBusiness.process_latest_staging()
    
    mock_get_latest_parquet.assert_called_once_with(staging_path)
    mock_partition_folder.assert_any_call('staging')
    mock_partition_folder.assert_any_call('business')
    mock_rename.assert_called()
    mock_rmtree.assert_called()
