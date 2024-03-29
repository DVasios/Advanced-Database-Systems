# ---- QUERY 1 | DATAFRAME API ----

# Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, count, dense_rank, to_timestamp
from pyspark.sql.window import Window

# Libs
import time
import os 
project_home = os.getenv('PROJECT_HOME')

# Export Results Lib
from importlib.machinery import SourceFileLoader
export_result = SourceFileLoader("export_result", f'{project_home}/lib/export_result.py').load_module()

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Query 1 - Dataframe API") \
    .getOrCreate() 

# Crime Data DF
crime_data_df = sc.read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

## --- Start Time ----
start_time = time.time()

# Change Columns types
crime_data_df = crime_data_df \
    .withColumn('Date Rptd', to_timestamp('Date Rptd', 'MM/dd/yyyy hh:mm:ss a')) \
    .select('Date Rptd')

# Extract year and month from the 'date_occ' column
counts = crime_data_df \
    .withColumn('Year', year('Date Rptd')) \
    .withColumn('Month', month('Date Rptd')) \
    .groupBy('Year', 'Month').agg(count('*').alias('crimetotal'))

# Order by Year and Total Crimes Crimes
partitioned = Window.partitionBy('Year').orderBy(counts['crimetotal'].desc())

# Add a rank column to the DataFrame
ranked_df = counts.withColumn('rnk', dense_rank().over(partitioned))

# Filter the top 3 counts for each year
top3_df = ranked_df.filter('rnk <= 3')

# Rename the rank column
top3 = top3_df.withColumnRenamed('rnk', '#')

# Show the results
top3.show(100)

## --- Finish Time ----
finish_time = time.time()
execution_time = finish_time - start_time
print(f"Execution Time: {execution_time} seconds")

# Export the results
top3.toPandas().to_csv(f'{project_home}/results/q1_df.csv', index=False)

# Export Execution Time
export_result.export(f'{project_home}/results/exec_times.csv', 'q1_df', execution_time)

# Stop Spark Session
sc.stop()