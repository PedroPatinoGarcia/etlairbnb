import os
import pytest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession

# Asegurarse de que el directorio 'src' esté en el PYTHONPATH
os.environ['PYTHONPATH'] = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))

import src.spark_session as spark_module  # Importar el módulo desde 'src'

@patch('os.path.exists')
@patch.dict(os.environ, {}, clear=True)
def test_windows_hadoop_setup(mock_path_exists):
    # Simular que el archivo winutils.exe existe en la ruta esperada
    mock_path_exists.return_value = True
    
    # Ejecutar el módulo que configura SparkSession y el entorno
    with patch('pyspark.sql.SparkSession.builder') as mock_builder:
        mock_session = MagicMock()
        mock_builder.return_value.appName.return_value.config.return_value.getOrCreate.return_value = mock_session
        
        # Ejecutar el módulo que configura SparkSession y el entorno
        spark_module.configure_spark_session()
        
        # Verificar que HADOOP_HOME y PATH se han configurado correctamente
        assert os.environ['HADOOP_HOME'] == "C:\\Program Files\\Hadoop"
        assert "C:\\Program Files\\Hadoop\\bin" in os.environ['PATH']

        # Verificar que se ha creado una instancia de SparkSession
        mock_builder.return_value.appName.assert_called_once_with("DataHandler")
        mock_builder.return_value.config.assert_called_once_with("spark.executor.memory", "10g")
        mock_session.sparkContext.setLogLevel.assert_called_once_with("ERROR")

@patch('os.path.exists')
@patch.dict(os.environ, {}, clear=True)
def test_non_windows_hadoop_setup(mock_path_exists):
    # Simular que el archivo winutils.exe no existe en la ruta esperada
    mock_path_exists.return_value = False
    
    # Ejecutar el módulo que configura SparkSession y el entorno
    with patch('pyspark.sql.SparkSession.builder') as mock_builder:
        mock_session = MagicMock()
        mock_builder.return_value.appName.return_value.config.return_value.getOrCreate.return_value = mock_session
        
        # Ejecutar el módulo que configura SparkSession y el entorno
        spark_module.configure_spark_session()
        
        # Verificar que HADOOP_HOME no está configurado y que la ruta binaria no se ha añadido a PATH
        assert 'HADOOP_HOME' not in os.environ
        assert "C:\\Program Files\\Hadoop\\bin" not in os.environ['PATH']

        # Verificar que se ha creado una instancia de SparkSession
        mock_builder.return_value.appName.assert_called_once_with("DataHandler")
        mock_builder.return_value.config.assert_called_once_with("spark.executor.memory", "10g")
        mock_session.sparkContext.setLogLevel.assert_called_once_with("ERROR")
