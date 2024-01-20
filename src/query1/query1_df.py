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
# crime_data_schema = StructType([
#     StructField("DR_NO", StringType()),
#     StructField("Date Rptd", StringType()),
#     StructField("DATE OCC", StringType()),
#     StructField("TIME OCC", StringType()),
#     StructField("AREA", StringType()),
#     StructField("AREA NAME", StringType()),
#     StructField("Rpt Dist No", StringType()),
#     StructField("Part 1-2", StringType()),
#     StructField("Crm Cd", StringType()),
#     StructField("Crm Cd Desc", StringType()),
#     StructField("Mocodes", StringType()),
#     StructField("Vict Age", StringType()),
#     StructField("Vict Sex", StringType()),
#     StructField("Vict Descent", StringType()),
#     StructField("Premis Cd", StringType()),
#     StructField("Premis Desc", StringType()),
#     StructField("Weapon Used Cd", StringType()),
#     StructField("Weapon Desc", StringType()),
#     StructField("Status", StringType()),
#     StructField("Status Desc", StringType()),
#     StructField("Crm Cd 1", StringType()),
#     StructField("Crm Cd 2", StringType()),
#     StructField("Crm Cd 3", StringType()),
#     StructField("Crm Cd 4", StringType()),
#     StructField("LOCATION", StringType()),
#     StructField("Cross Street", StringType()),
#     StructField("LAT", StringType()),
#     StructField("LON", StringType()),
# ])



# ---- QUERY 1 | DATAFRAME API ----

# Keep specific columns from the dataframe
crime_data_date = crime_data_df.select('Date Rptd')

# Extract year and month from the 'date_occ' column
crime_data_year_month = crime_data_date.withColumn('Year', year('Date Rptd')) \
                                       .withColumn('Month', month('Date Rptd'))

# Calculate counts for each year and month
counts = crime_data_year_month.groupBy('Year', 'Month').agg(count('*').alias('crimetotal'))

# Order by Year and Total Crimes Crimes
partitioned = Window.partitionBy('Year').orderBy(counts['crimetotal'].desc())

# Add a rank column to the DataFrame
ranked_df = counts.withColumn('rnk', dense_rank().over(partitioned))

# Filter the top 3 counts for each year
top3_df = ranked_df.filter('rnk <= 3')

# Rename the rank column
top3 = top3_df.withColumnRenamed('rnk', '#')

# Show the results
top3.show(50)
