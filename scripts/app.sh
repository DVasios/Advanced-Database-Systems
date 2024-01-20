#! /bin/bash

# Application 
param1=$1
param2=$2

## Load Data to Cluster
if [[ $param1 == "-ld" ]]; then

  #  Los Angeles Crime Data - Primary
  if [[ -e ../data/primary/crime_data_2010_2019.csv ]]; then
      echo "Los Angeles Crime Data from 2010 to 2019 already exist"
  else 
      echo "Downloading Primary Data - Los Angeles Crime 2010-19... "
      wget -nc -q -O  ../data/primary/crime_data_2010_2019.csv https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD
      echo "Download successful!"
  fi

  if [[ -e ../data/primary/crime_data_2020_present.csv ]]; then
      echo "Los Angeles Crime Data from 2020 to present already exist"
  else 
      echo "Downloading Primary Data - Los Angeles Crime 2020-present... "
      wget -nc -q -O  ../data/primary/crime_data_2020_present.csv https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD
      echo "Download successful!"
  fi

  # Create Cluster Data Folder
  echo "Creating User Directory in HDFS"
  hdfs dfs -mkdir /user

  echo "Inserting Data to HDFS"
  hdfs dfs -put ../data /user/
  echo "Data is ready!"

fi

## Combine Primary Data to Cluster
if [[ $param1 == "-cd" ]]; then
  $SPARK_HOME/bin/spark-submit \
  /home/user/project/src/data_proc/combine_data.py
fi

# $SPARK_HOME/bin/spark-submit \
# /home/user/project/src/data_proc/load_data.py

