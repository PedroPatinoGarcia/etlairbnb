import os
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

import src.spark_session as spark_module

def test_spark_session_creation():
    assert spark_module.spark is not None
    assert isinstance(spark_module.spark, SparkSession)
    assert spark_module.spark.conf.get("spark.app.name") == "DataHandler"
    assert spark_module.spark.conf.get("spark.executor.memory") == "10g"

@patch('os.path.exists')
@patch.dict(os.environ, {}, clear=True)
def test_windows_hadoop_setup(mock_path_exists):
    mock_path_exists.return_value = True
    with patch('builtins.exec') as mock_exec:
        exec('src.spark_session')
    
    assert os.environ['HADOOP_HOME'] == "C:\\Program Files\\Hadoop"
    assert "C:\\Program Files\\Hadoop\\bin" in os.environ['PATH']

@patch('os.path.exists')
@patch.dict(os.environ, {}, clear=True)
def test_non_windows_hadoop_setup(mock_path_exists):
    mock_path_exists.return_value = False
    with patch('builtins.exec') as mock_exec:
        exec('src.spark_session')
    assert 'HADOOP_HOME' not in os.environ
    assert "C:\\Program Files\\Hadoop\\bin" not in os.environ['PATH']
