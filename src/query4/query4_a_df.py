# ---- Query 4 | CASE A | Dataframe API ----

# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, udf, year, rank, count, avg, broadcast
import geopy.distance
from pyspark.sql.window import Window

# Libs
import time
import os 
import sys 
project_home = os.getenv('PROJECT_HOME')

# Export Results Lib
from importlib.machinery import SourceFileLoader
export_result = SourceFileLoader("export_result", f'{project_home}/lib/export_result.py').load_module()

# Get Lat, Lon distance
def get_distance(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km

distance = udf(lambda lat1, lon1, lat2, lon2: get_distance(lat1, lon1, lat2, lon2))

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Query 4 - Dataframe API") \
    .getOrCreate() 

# Crime Data DF
crime_data_df = sc \
    .read.format('csv') \
    .options(header='true', inferschema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

## --- Start Time ----
start_time = time.time()

# Change Columns types
crime_data_df = crime_data_df \
    .withColumn('Date Rptd', to_timestamp('Date Rptd', 'MM/dd/yyyy hh:mm:ss a')) \
    .withColumn('LAT',col('LAT').cast('double')) \
    .withColumn('LON', col('LON').cast('double'))

# Police Stations DF
police_stations_df = sc \
    .read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/secondary/LAPD_Police_Stations.csv")

# Change Column Types and Select with aliased names
police_stations_df = police_stations_df \
    .withColumn("X", col("X").cast("double")) \
    .withColumn("Y", col("Y").cast("double")) \
    .select(col("X").alias("police_station_lon"), col("Y").alias("police_station_lat"), col("PREC").alias("police_station"), col("DIVISION").alias("division")) 

# Join Data
crime_data_df = crime_data_df \
    .withColumn("AREA", col("AREA").cast('int')) \
    .withColumn("Year", year('Date Rptd')) \
    .select(col('Year'), col("Weapon Used Cd").alias("weapon"), col("LAT").alias("crime_lat"), col("LON").alias("crime_lon"), col("AREA").alias("police_station")) \
    
if (len(sys.argv) >= 3):
    joined_police_station_df = crime_data_df \
        .join(police_stations_df.hint(sys.argv[1]), on="police_station")
else:  
    joined_police_station_df = crime_data_df \
        .join(police_stations_df, on="police_station")

# ------------ POLICE STATION THAT UNDERTOOK THE CRIME ------------------

crime_distance_df =  joined_police_station_df \
    .filter(col('weapon').isNotNull() & col('weapon').rlike("^1\\d{2}$")) \
    .withColumn("Distance", distance(col("crime_lat"), col("crime_lon"), col("police_station_lat"), col("police_station_lon"))) 

# Persist Df for better performance
crime_distance_df.persist()

# Per Year
per_year_a_df = crime_distance_df \
    .groupBy(col('Year')).agg(count('*').alias('#'), avg('Distance').alias('average_distance')) \
    .orderBy(col('Year').asc()) \
    .select(col('Year'), col('average_distance'), col('#'))

# Export the results

# Per Division
per_division_a_df =  crime_distance_df \
    .groupBy(col('division')).agg(count('*').alias('#'), avg('Distance').alias('average_distance')) \
    .orderBy(col('#').desc()) \
    .select(col('division'), col('average_distance'), col('#'))

## --- Start Time ----
start_time_per_division = time.time()

# Show Results
per_year_a_df.show()
per_year_a_df.explain()
per_division_a_df.show()
per_division_a_df.explain()

## --- Finish Time ----
finish_time = time.time()
execution_time = round(finish_time - start_time, 2)
print(f"Execution Time: {execution_time} seconds")

# Export Execution Time
if (len(sys.argv) >= 2):
    export_result.export(f'{project_home}/results/exec_times.csv',f'q4_a_{sys.argv[1]}_df', execution_time)
else:
    export_result.export(f'{project_home}/results/exec_times.csv','q4_a_df', execution_time)

# Export Results
per_year_a_df.toPandas().to_csv(f'{project_home}/results/q4_per_year_a_df.csv', index=False)
per_division_a_df.toPandas().to_csv(f'{project_home}/results/q4_per_division_a_df.csv', index=False)

joined_police_station_df.unpersist()

# Stop Session
sc.stop()

