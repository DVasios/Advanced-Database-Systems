# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, IntegerType
from pyspark.sql.functions import col, count, when, to_timestamp, udf,  radians, sin, cos, sqrt, atan2, round
from pyspark.sql.functions import year, month, count, dense_rank, avg
from pyspark.sql.window import Window
from pyspark.conf import SparkConf

# Other Libraries
import os
import subprocess as sp

# Spark Session | Load Data
sc = SparkSession \
    .builder \
    .appName("Load Data") \
    .config(conf=conf) \
    .getOrCreate() 

# Combine Primary Data into one csv
crime_data_2010_2019 = sc.read.csv('hdfs://okeanos-master:54310/user/data/primary/crime_data_2010_2019.csv', header=True, inferSchema=True)
crime_data_2020_present = sc.read.csv('hdfs://okeanos-master:54310/user/data/primary/crime_data_2020_present.csv', header=True, inferSchema=True)
crime_data = crime_data_2010_2019.union(crime_data_2020_present)
crime_data.write.csv('hdfs://okeanos-master:54310/user/data/primary/crime_data.csv', header=True, mode='overwrite')

# Print Total Crime Data Rows
rows = crime_data_df.count()
print(f"Crime Data Total Rows : {rows}")

# Print Crime Data Types
crime_data_types = crime_data_df.dtypes
print(f"Date Rptd: {crime_data_types[1][1]}")
print(f"DATE OCC: {crime_data_types[2][1]}")
print(f"Vict Age: {crime_data_types[11][1]}")
print(f"LAT: {crime_data_types[26][1]}")
print(f"LON: {crime_data_types[27][1]}")

# Stop Spark Session 
sc.stop()