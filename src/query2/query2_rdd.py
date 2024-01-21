# ---- Query 2 | RDD API ----

# Pyspark Libraries
from pyspark.sql import SparkSession
import pandas as pd
import time

# Spark Session | Queries
sc = SparkSession \
    .builder \
    .appName("Query 2 - RDD API") \
    .getOrCreate() 

# Crime Data DF
crime_data_df = sc.read.format('csv') \
    .options(header='true', inferSchema=True) \
    .load("hdfs://okeanos-master:54310/user/data/primary/crime_data")

## --- Start Time ----
start_time = time.time()

# RDD Initialization
rdd_from_df = crime_data_df.rdd

def categorize(x): 
    if x[0] >= 500 and x[0] < 1200:
        return ('Morning', 1)
    elif x[0] >= 1200 and x[0] < 1700:
        return ('Noon', 1)
    elif x[0] >= 1700 and x[0] < 2100:
        return ('Afternoon', 1)
    elif (x[0] >= 2100 and x[0] < 2400) or (x[0] >= 0 and x[0] < 500):
        return ('Night', 1)

data_rdd = rdd_from_df \
    .map(lambda x: (int(x[3]), str(x[15]))) \
    .filter(lambda x: x[1] == "STREET") \
    .map(categorize) \
    .reduceByKey(lambda x, y: x+y) 

result = data_rdd.take(4)

## --- Finish Time ----
finish_time = time.time()
execution_time = round(finish_time - start_time, 2)
print(f"Execution Time: {execution_time} seconds")

columns = [ 'PartOfDay', 'NumberOfCrimes']
rows = []
for i in result: 
    rows.append([i[0], i[1]])
df = pd.DataFrame(rows, columns=columns)
sorted_df = df.sort_values(by='NumberOfCrimes', ascending=False, ignore_index=True)
print(sorted_df)

# Export result to csv
sorted_df.to_csv('/home/user/project/results/q2_rdd.csv', index=False)
