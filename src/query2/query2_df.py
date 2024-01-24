# ---- Query 2 | Dataframe API ----

# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, count, col
import time

# Export Results Lib
from importlib.machinery import SourceFileLoader
export_result = SourceFileLoader("export_result", '/home/user/project/lib/export_result.py').load_module()

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Query 2 - Dataframe API") \
    .getOrCreate() 

# Crime Data DF
crime_data_df = sc.read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

## --- Start Time ----
start_time = time.time()

query_2 = crime_data_df \
    .filter(crime_data_df['Premis Desc'] == 'STREET') \
    .withColumn( 
        'PartOfDay', 
        when((crime_data_df['TIME OCC'] >= 500) & (crime_data_df['TIME OCC'] < 1200), 'Morning') \
        .when((crime_data_df['TIME OCC'] >= 1200) & (crime_data_df['TIME OCC'] < 1700), 'Noon') \
        .when((crime_data_df['TIME OCC'] >= 1700) & (crime_data_df['TIME OCC'] < 2100), 'Afternoon') \
        .when((crime_data_df['TIME OCC'] >= 2100) & (crime_data_df['TIME OCC'] < 2400) |
              (crime_data_df['TIME OCC'] >= 0) & (crime_data_df['TIME OCC'] < 500), 'Night') \
        .otherwise('NoPartOfDay')) \
    .select(col('TIME OCC').alias('time'), col('PartOfDay')) \
    .groupBy(col('PartOfDay')).agg(count('*').alias('NumberOfCrimes')) \
    .orderBy(col('NumberOfCrimes').desc()) 

# Print Output
query_2.show()

## --- Finish Time ----
finish_time = time.time()
execution_time = round(finish_time - start_time, 2)
print(f"Execution Time: {execution_time} seconds")

# Export the results
query_2.toPandas().to_csv('/home/user/project/results/q2_df.csv', index=False)

# Export Execution Time
export_result.export('q2_df', execution_time)

# Stop Spark Session
sc.stop()