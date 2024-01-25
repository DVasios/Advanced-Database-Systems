# ---- Query 1 | SQL API ----

# Libs
import time
import os 
project_home = os.getenv('PROJECT_HOME')

# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp

# Export Results Lib
from importlib.machinery import SourceFileLoader
export_result = SourceFileLoader("export_result", f'{project_home}/lib/export_result.py').load_module()

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Query 1 - SQL API") \
    .getOrCreate() 

# Crime Data DF
crime_data_df = sc.read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

## --- Start Time ----
start_time = time.time()

query_1_sql = """ with MonthlyCrimeCounts AS ( 
  SELECT  
    EXTRACT(YEAR FROM `Date Rptd`) AS Year, 
    EXTRACT(MONTH FROM `Date Rptd`) AS Month, 
    COUNT(*) AS crimetotal,  
    ROW_NUMBER() OVER (PARTITION BY EXTRACT(YEAR FROM `Date Rptd`) ORDER BY COUNT(*) DESC) AS rn 
  FROM 
    crime_data
  GROUP BY 
    Year, 
    Month 
) 

SELECT 
  Year, 
  Month, 
  crimetotal, 
  rn as rank
FROM 
  MonthlyCrimeCounts 
WHERE 
  rn <= 3 
ORDER BY 
  Year ASC, 
  crimetotal DESC; """

# Change Columns types
crime_data_df = crime_data_df.withColumn('Date Rptd', to_timestamp('Date Rptd', 'MM/dd/yyyy hh:mm:ss a'))

# Create Temp View
crime_data_df.createOrReplaceTempView("crime_data")
crime_data_query_1 = sc.sql(query_1_sql)
crime_data_query_1.show(100)

## --- Finish Time ----
finish_time = time.time()
execution_time = round(finish_time - start_time, 2)
print(f"Execution Time: {execution_time} seconds")

# Export the results
crime_data_query_1.toPandas().to_csv(f'{project_home}/results/q1_sql.csv', index=False)

# Export Execution Time
export_result.export(f'{project_home}/results/exec_times.csv','q1_sql', execution_time)

# Stop Spark  Session
sc.stop()
