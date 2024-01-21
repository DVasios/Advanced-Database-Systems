# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql.functions import col, to_timestamp

# Spark Session | Load Data
sc = SparkSession \
    .builder \
    .appName("Count & Types") \
    .getOrCreate() 

# Crime Data DF
print('Loading Crime Data Dataframe')
crime_data_df = sc.read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

# Change Columns types
crime_data_df = crime_data_df \
    .withColumn('Date Rptd', to_timestamp('Date Rptd', 'MM/dd/yyyy hh:mm:ss a')) \
    .withColumn('DATE OCC', to_timestamp('DATE OCC', 'MM/dd/yyyy hh:mm:ss a')) \
    .withColumn('TIME OCC', col('TIME OCC').cast('int')) \
    .withColumn('Vict Age', col('Vict Age').cast('int')) \
    .withColumn('LAT',col('LAT').cast('double')) \
    .withColumn('LON', col('LON').cast('double'))

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