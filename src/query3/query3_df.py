# ---- Query 3 | Dataframe API ----

# Pyspark Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col, udf, dense_rank
from pyspark.sql.window import Window
import time

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Query 3 - Dataframe API") \
    .getOrCreate() 

# Crime Data DF
crime_data_df = sc.read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

# --------------------------------------------------------------------------------------------------------- |

# Load data from dfs
median_income_df = sc \
    .read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/secondary/median_household_incomes/LA_income_2015.csv")

# Function to alter 'estimated median income' to integer | $XX,YYY -> XXYYY
alter_median_income_col = udf(lambda x: int(x.replace('$', '').replace(',', '')))

# Change Column Types & Drop Duplicates
median_income_df = median_income_df \
    .withColumn("Zip Code", col("Zip Code").cast('int')) \
    .withColumn("Estimated Median Income", alter_median_income_col(col("Estimated Median Income"))) \
    .drop_duplicates(["Zip Code"]) \
    .select(col("Zip Code").alias("zipcode"), col("Estimated Median Income").alias('median_income'))

# ---------------------------------------------------------------------------------------------------------- |

# Load Data from DFS
revgecoding_df =  sc \
    .read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/secondary/revgecoding.csv")

# Change Column Types
revgecoding_df = revgecoding_df \
    .withColumn("LAT", col("LAT").cast("double")) \
    .withColumn("LON", col("LON").cast("double")) \
    .withColumn("ZIPcode", col("ZIPcode").cast("int")) \
    .select(col("LAT"), col("LON"), col("ZIPcode").alias("zipcode"))

# Join Secondary Dataframes to the main
income_from_coordinates_df = median_income_df.join(revgecoding_df, on="zipcode")

joined_crime_data_df = crime_data_df \
    .join(income_from_coordinates_df, (crime_data_df["LAT"] == income_from_coordinates_df["LAT"]) & (crime_data_df["LON"] == income_from_coordinates_df["LON"])) \
    .select(col('Vict Descent').alias('descent'), col('median_income'))

# Dictionary for Descent
descent_dict = {
    'A': 'Other Asian',
    'B': 'Black',
    'C': 'Chinese',
    'D': 'Cambodian',
    'F': 'Filipino',
    'G': 'Guamanian',
    'H': 'Hispanic/Latin/Mexican',
    'I': 'American Indian/Alaskan Native',
    'J': 'Japanese',
    'K': 'Korean',
    'L': 'Laotian',
    'O': 'Other',
    'P': 'Pacific Islander',
    'S': 'Samoan',
    'U': 'Hawaiian',
    'V': 'Vietnamese',
    'W': 'White',
    'X': 'Unknown',
    'Z': 'Asian Indian'
}

get_descent = udf(lambda x: descent_dict[x])


# Load Secondary Data --- Median Income & Revgecoding
query_3_filtered = joined_crime_data_df \
    .filter((col("descent").isNotNull()) & (col("descent") != '')) 

## --- Start Time ----
start_time = time.time()

# Query
def query_3 (data_df, type, num):

    if (type == 'asc'):
        window_spec_asc = Window.orderBy(col("median_income").asc())
        filtered_df = data_df \
            .withColumn("Rank", dense_rank().over(window_spec_asc)) \
            .filter(col("Rank") == num) \
            .groupBy("descent").agg(count('*').alias("#")) \
            .withColumn("descent", get_descent(col("descent"))) \
            .orderBy(col("#").desc())
        
        filtered_df.show()

        # Export Results
        filtered_df.toPandas().to_csv(f'/home/user/project/results/q3_df_{type}_{num}.csv', index=False)
        
    elif (type == 'desc'):

        window_spec_desc = Window.orderBy(col("median_income").desc())
        filtered_df = data_df \
            .withColumn("Rank", dense_rank().over(window_spec_desc)) \
            .filter(col("Rank") == num) \
            .groupBy("descent").agg(count('*').alias("#")) \
            .withColumn("descent", get_descent(col("descent"))) \
            .orderBy(col("#").desc())


        filtered_df.show()

        # Export Results
        filtered_df.toPandas().to_csv(f'/home/user/project/results/q3_df_{type}_{num}.csv', index=False)

# Print first 3 
for i in range(1,4):
    query_3(query_3_filtered, 'asc', i)

# Print last 3 
for i in range(1,4):
    query_3(query_3_filtered, 'desc', i)

## --- Finish Time ----
finish_time = time.time()
execution_time = round(finish_time - start_time, 2)
print(f"Execution Time: {execution_time} seconds")

# Stop Spark Session
sc.stop()