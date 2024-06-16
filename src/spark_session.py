from pyspark.sql import SparkSession
import os

winutils_path = "C:\\Program Files\\Hadoop\\bin\\winutils.exe"
if os.path.exists(winutils_path):
    os.environ['HADOOP_HOME'] = "C:\\Program Files\\Hadoop"
    os.environ['PATH'] += os.pathsep + "C:\\Program Files\\Hadoop\\bin"

spark = SparkSession.builder \
    .appName("DataHandler") \
    .config("spark.executor.memory", "20g") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")