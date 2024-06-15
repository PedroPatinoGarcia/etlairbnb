import pytest
import os
from datetime import datetime
from pyspark.sql import SparkSession
from staging import HandlerBranchStaging

@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder \
        .appName("TestStaging") \
        .getOrCreate()
    yield spark
    spark.stop()

def test_get_latest_parquet_file():
    latest_file = HandlerBranchStaging.get_latest_parquet_file('raw')
    assert latest_file is not None, "No se encontró ningún archivo Parquet en el directorio 'raw'."

def test_clean_data(spark_session):
    test_data = [
    ]

    df = spark_session.createDataFrame(test_data, schema=...) 

    cleaned_df = HandlerBranchStaging.clean_data(df)

    assert cleaned_df.count() > 0, "El DataFrame limpiado está vacío."

def test_partition_folder():
    base_path = '/some/base/path'
    partitioned_path = HandlerBranchStaging.partition_folder(base_path)

    assert os.path.exists(partitioned_path), f"No se creó correctamente la carpeta de partición en {partitioned_path}"