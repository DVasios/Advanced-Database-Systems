# ---- Query 4 | Dataframe API ----

# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col, udf, year, count, avg, row_number
import geopy.distance
from pyspark.sql.window import Window

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Query 4 - Dataframe API") \
    .getOrCreate() 

# Crime Data DF
crime_data_df = sc \
    .read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

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
joined_police_station_df = crime_data_df \
    .withColumn("AREA", col("AREA").cast('int')) \
    .withColumn("Year", year('Date Rptd')) \
    .select(col('Year'), col("Weapon Used Cd").alias("weapon"), col("LAT").alias("crime_lat"), col("LON").alias("crime_lon"), col("AREA").alias("police_station")) \
    .join(police_stations_df, on="police_station")

def get_distance(lat1, lon1, lat2, lon2):
    return geopy.distance.geodesic((lat1, lon1), (lat2, lon2)).km

distance = udf(lambda lat1, lon1, lat2, lon2: get_distance(lat1, lon1, lat2, lon2))

# Calculate Distance from police station that the crime happened
crime_distance_df =  joined_police_station_df \
    .filter(col('weapon').isNotNull()) \
    .withColumn("Distance", distance(col("crime_lat"), col("crime_lon"), col("police_station_lat"), col("police_station_lon"))) 

# Per Year
# per_year_df = crime_distance_df \
#     .groupBy(col('Year')).agg(count('*').alias('#'), avg('Distance').alias('average_distance')) \
#     .orderBy(col('Year').asc()) \
#     .select(col('Year'), col('average_distance'), col('#'))

# per_year_df.show()

# Per Division
# per_division_df =  crime_distance_df \
#     .groupBy(col('division')).agg(count('*').alias('#'), avg('Distance').alias('average_distance')) \
#     .orderBy(col('#').desc()) \
#     .select(col('division'), col('average_distance'), col('#'))

# per_division_df.limit(100).show()

# Cross Join
cross_join_police_station_df = crime_data_df \
    .withColumn("AREA", col("AREA").cast('int')) \
    .withColumn("Year", year('Date Rptd')) \
    .select(col('Year'), col("Weapon Used Cd").alias("weapon"), col("LAT").alias("crime_lat"), col("LON").alias("crime_lon"), col("AREA").alias("police_station")) \
    .crossJoin(police_stations_df) \
    .withColumn("distance", distance(col("crime_lat"), col("crime_lon"), col("police_station_lat"), col("police_station_lon")))

window_spec = Window.partitionBy("distance").orderBy("distance")
closest_police_station_df = cross_join_police_station_df \
    .withColumn("row_number", row_number().over(window_spec)) \
    .filter(col("row_number") == 1 ) 

per_year_df = closest_police_station_df \
    .groupBy(col('Year')).agg(count('*').alias('#'), avg('distance').alias('average_distance')) \
    .orderBy(col('Year').asc()) \
    .select(col('Year'), col('average_distance'), col('#'))

per_year_df.limit(100).show()

