# Pyspark Libraries
from pyspark.sql import SparkSession

# Spark Session | Load Data
sc = SparkSession \
    .builder \
    .appName("Combine Data") \
    .getOrCreate() 

print('Combine Data into one csv')

# Combine Primary Data into one csv
crime_data_2010_2019 = sc.read.csv('hdfs://okeanos-master:54310/user/data/primary/crime_data_2010_2019.csv', header=True, inferSchema=True)
crime_data_2020_present = sc.read.csv('hdfs://okeanos-master:54310/user/data/primary/crime_data_2020_present.csv', header=True, inferSchema=True)
crime_data = crime_data_2010_2019.union(crime_data_2020_present)
crime_data.write.option("header", True).mode("overwrite").csv('hdfs://okeanos-master:54310/user/data/primary/crime_data')

print('Combined Data Successfully')

# Stop Spark Session 
sc.stop()