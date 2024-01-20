# Pre-process Secondary Police Stations Data

#  Police Station Schema
police_stations_schema = StructType([
    StructField("X", StringType()),
    StructField("Y", StringType()),
    StructField("FID", StringType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("PREC", StringType())
])

# Load data from dfs
police_stations_df = sc \
    .read.format('csv') \
    .options(header='true') \
    .schema(police_stations_schema) \
    .load("hdfs://okeanos-master:54310/user/data/secondary/LAPD_Police_Stations.csv")

# Change Column Types and Select with aliased names
police_stations_df = police_stations_df \
    .withColumn("X", col("X").cast("double")) \
    .withColumn("Y", col("Y").cast("double")) \
    .select(col("X").alias("police_station_lat"), col("Y").alias("police_station_lon"), col("PREC").alias("police_station")) 

# Join Data
joined_police_station_df = crime_data_df \
    .withColumn("AREA", col("AREA").cast('int')) \
    .withColumn("Year", year('Date Rptd')) \
    .select(col('Year'), col("Weapon Used Cd").alias("weapon"), col("LAT").alias("crime_lat"), col("LON").alias("crime_lon"), col("AREA").alias("police_station")) \
    .join(police_stations_df, on="police_station")

# Register Distance Function
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    lat1_rad, lon1_rad, lat2_rad, lon2_rad = radians(lat1), radians(lon1), radians(lat2), radians(lon2)
    dlat, dlon = lat2_rad - lat1_rad, lon2_rad - lon1_rad
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# ---- Query 4 | Dataframe API ----

filtered_df =  joined_police_station_df \
    .filter(joined_police_station_df['weapon'] != 'NULL') \
    .withColumn("Distance", haversine(col("crime_lat"), col("crime_lon"), col("police_station_lat"), col("police_station_lon"))) \
    .groupBy(col('Year')).agg(count('*').alias('#'), avg('Distance').alias('average_distance')) \
    .orderBy(col('Year').asc()) \
    .withColumn('average_distance', round(col('average_distance'))) \
    .select(col('Year'), col('average_distance'), col('#'))

filtered_df.limit(100).show()