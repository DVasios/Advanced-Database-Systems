#! /bin/bash

# Application 
param1=$1
param2=$2

## Load Data to Cluster | ./app.sh -ld
if [[ $param1 == "-ld" ]]; then

  #  Los Angeles Crime Data - Primary
  if [[ -e $PROJECT_HOME/data/primary/crime_data_2010_2019.csv ]]; then
      echo "Los Angeles Crime Data from 2010 to 2019 already exist"
  else 
      echo "Downloading Primary Data - Los Angeles Crime 2010-19... "
      wget -nc -q -O  $PROJECT_HOME/data/primary/crime_data_2010_2019.csv https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD
      echo "Download successful!"
  fi

  if [[ -e $PROJECT_HOME/data/primary/crime_data_2020_present.csv ]]; then
      echo "Los Angeles Crime Data from 2020 to present already exist"
  else 
      echo "Downloading Primary Data - Los Angeles Crime 2020-present... "
      wget -nc -q -O  $PROJECT_HOME/data/primary/crime_data_2020_present.csv https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD
      echo "Download successful!"
  fi

  # Create Cluster Data Folder
  echo "Creating User Directory in HDFS"
  hdfs dfs -mkdir /user

  echo "Inserting Data to HDFS"
  hdfs dfs -put $PROJECT_HOME/data /user/
  echo "Data is ready!"

## Combine Primary Data to Cluster | ./app.sh -cd
elif [[ $param1 == "-cd" ]]; then
  echo "Running Spark Application"
  $SPARK_HOME/bin/spark-submit \
  /home/user/project/src/data_proc/combine_data.py

  echo "Deleting Previous CSVs"
  hdfs dfs -rm /user/data/primary/crime_data_2010_2019.csv
  hdfs dfs -rm /user/data/primary/crime_data_2020_present.csv

## Count & Types | ./app.sh -ct
elif [[ $param1 == "-ct" ]]; then
  $SPARK_HOME/bin/spark-submit \
    $PROJECT_HOME/src/data_proc/count_types.py

## Query 1 - Dataframe API - 4 Executors | ./app.sh -q1 -df
elif [[ $param1 == "-q1" && $param2 == "-df" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --num-executors 4 \
    $PROJECT_HOME/src/query1/query1_df.py

## Query 1 - SQL API - 4 Executros | ./app.sh -q1 -sql
elif [[ $param1 == "-q1" && $param2 == "-sql" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --num-executors 4 \
    $PROJECT_HOME/src/query1/query1_sql.py

## Query 2 - Dataframe API - 4 Executors | ./app.sh -q2 -df
elif [[ $param1 == "-q2" && $param2 == "-df" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --num-executors 4 \
    $PROJECT_HOME/src/query2/query2_df.py

## Query 2 - RDD API - 4 Executors | ./app.sh -q2 -rdd
elif [[ $param1 == "-q2" && $param2 == "-rdd" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --num-executors 4 \
    $PROJECT_HOME/src/query2/query2_rdd.py

## Query 3 - Dataframe API - {num} Executors | ./app.sh -q3 {num} 
elif [[ $param1 == "-q3" ]]; then

  # Two Executors
  if [[ $param2 == "2" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --num-executors 2 \
    --executor-cores 2 \
    --executor-memory 2g \
    $PROJECT_HOME/src/query3/query3_df.py 2
  fi

  # Three Executors
  if [[ $param2 == "3" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --num-executors 3 \
    --executor-cores 2 \
    $PROJECT_HOME/src/query3/query3_df.py 3
  fi

  # Four Executors
  if [[ $param2 == "4" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --num-executors 1 \
    --executor-cores 1 \
    $PROJECT_HOME/src/query3/query3_df.py 4
  fi

## Query 4 - Dataframe API | ./app.sh -q4
elif [[ $param1 == "-q4" ]]; then
  $SPARK_HOME/bin/spark-submit \
    --py-files hdfs://okeanos-master:54310/lib/dep.zip \
    $PROJECT_HOME/src/query4/query4_df.py

## Usage
else 
  echo "Wrong Usage"
fi