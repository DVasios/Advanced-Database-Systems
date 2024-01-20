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

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Queries") \
    .config(conf=conf) \
    .getOrCreate() 

# Crime Data Schema
crime_data_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", StringType()),
    StructField("AREA", StringType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", StringType()),
    StructField("Part 1-2", StringType()),
    StructField("Crm Cd", StringType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
    StructField("Vict Age", StringType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict Descent", StringType()),
    StructField("Premis Cd", StringType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", StringType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", StringType()),
    StructField("Crm Cd 2", StringType()),
    StructField("Crm Cd 3", StringType()),
    StructField("Crm Cd 4", StringType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", StringType()),
    StructField("LON", StringType()),
])

crime_data_df = sc.read.format('csv') \
    .options(header='true') \
    .schema(crime_data_schema) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data.csv")

# Change Columns types
crime_data_df = crime_data_df.withColumn('Date Rptd', to_timestamp('Date Rptd', 'MM/dd/yyyy hh:mm:ss a')) \
                             .withColumn('DATE OCC', to_timestamp('DATE OCC', 'MM/dd/yyyy hh:mm:ss a')) \
                             .withColumn('TIME OCC', col('TIME OCC').cast('int')) \
                             .withColumn('Vict Age', col('Vict Age').cast('int')) \
                             .withColumn('LAT',col('LAT').cast('double')) \
                             .withColumn('LON', col('LON').cast('double'))

# ---- Query 1 | SQL API ----
query_1_sql = """ with MonthlyCrimeCounts AS ( 
  SELECT  
    EXTRACT(YEAR FROM `Date Rptd`) AS Year, 
    EXTRACT(MONTH FROM `Date Rptd`) AS Month, 
    COUNT(*) AS crime_count,  
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
  crime_count, 
  rn AS month_rank  
FROM 
  MonthlyCrimeCounts 
WHERE 
  rn <= 3 
ORDER BY 
  Year ASC, 
  crime_count DESC; """

crime_data_df.createOrReplaceTempView("crime_data")
crime_data_query_1 = sc.sql(query_1_sql)
crime_data_query_1.show()