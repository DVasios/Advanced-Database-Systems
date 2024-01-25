# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# Libs
import time
import os 
project_home = os.getenv('PROJECT_HOME')

# Export Results Lib
from importlib.machinery import SourceFileLoader
export_result = SourceFileLoader("export_result", f'{project_home}/lib/export_result.py').load_module()

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

# Total Crime Data Rows
rows = crime_data_df.count()

# Crime Data Types
crime_data_types = crime_data_df.dtypes

# Export Results
export_result.export(f'{project_home}/results/count_types.csv','count', rows)
export_result.export(f'{project_home}/results/count_types.csv','date_rptd', crime_data_types[1][1])
export_result.export(f'{project_home}/results/count_types.csv','date_occ', crime_data_types[2][1])
export_result.export(f'{project_home}/results/count_types.csv','vict_age', crime_data_types[11][1])
export_result.export(f'{project_home}/results/count_types.csv','lat', crime_data_types[26][1])
export_result.export(f'{project_home}/results/count_types.csv','lon', crime_data_types[27][1])

# Stop Spark Session 
sc.stop()