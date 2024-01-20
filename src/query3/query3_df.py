# Load Secondary Data --- Median Income & Revgecoding

# --------------------------------------------------------------------------------------------------------- |

# Median Income 2015 Schema
median_income_schema = StructType([
    StructField("Zip Code", StringType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
])

# Load data from dfs
median_income_df = sc \
    .read.format('csv') \
    .options(header='true') \
    .schema(median_income_schema) \
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

# Revgecoding Schema
revgecoding_schema = StructType([
    StructField("LAT", StringType()),
    StructField("LON", StringType()),
    StructField("ZIPcode", StringType()),
])

# Load Data from DFS
revgecoding_df =  sc \
    .read.format('csv') \
    .options(header='true') \
    .schema(revgecoding_schema) \
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

# ----- Query 3 | Dataframe API
query_3_filtered = joined_crime_data_df \
    .filter((col("descent").isNotNull()) & (col("descent") != '')) 

# Ranked Desc
window_spec_desc = Window.orderBy(col("median_income").desc())
ranked_desc_df = query_3_filtered \
    .withColumn("Rank", dense_rank().over(window_spec_desc)) 

# Ranked Asc
window_spec_asc = Window.orderBy(col("median_income").asc())
ranked_asc_df = query_3_filtered \
    .withColumn("Rank", dense_rank().over(window_spec_asc))

# Query
def query_3 (ranked, num): 
    return ranked \
    .filter(col("Rank") == num) \
    .groupBy("descent").agg(count('*').alias("#")) \
    .withColumn("descent", get_descent(col("descent"))) \
    .orderBy(col("#").desc())

# Print first 3 
for i in range(1,4):
    q = query_3(ranked_desc_df, i)
    q.show()

# Print last 3 
for i in range(1,4):
    q = query_3(ranked_asc_df, i)
    q.show()