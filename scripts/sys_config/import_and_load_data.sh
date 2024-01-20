#! /bin/bash

# Create data folders 
mkdir -p ../data
mkdir -p ../data/primary
mkdir -p ../data/secondary

# Database 1 - Los Angeles Crime Data - Primary

if [ -f ../data/primary/crime_data_2010_2019.csv ]; then
    echo "Los Angeles Crime Data from 2010 to 2019 already exist"
else 
    echo "Downloading Primary Data - Los Angeles Crime 2010-19... "
    wget -nc -q -O  ../data/primary/crime_data_2010_2019.csv https://data.lacity.org/api/views/63jg-8b9z/rows.csv?accessType=DOWNLOAD
    echo "Download successful!"
fi

if [ -f ../data/primary/crime_data_2020_present.csv ]; then
    echo "Los Angeles Crime Data from 2020 to present already exist"
else 
    echo "Downloading Primary Data - Los Angeles Crime 2020-present... "
    wget -nc -q -O  ../data/primary/crime_data_2020_present.csv https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD
    echo "Download successful!"
fi

# Database 2 - LA Police Stations - Secondary
if [ -f ../data/secondary/LAPD_Police_Stations.csv ]; then
    echo "LA Police Stations Database already in the data folder"
else 
    echo "LA Police Stations Database does not exists in the folder data, consider importing it."
fi

# Database 3 - Median Household Income - Secondary
if [ -d ../data/secondary/median_household_incomes ]; then
    echo "Median Household Income dataset already exists"
else 
     echo "Downloading Secondary Data - Median Household Income by Zip Code... "
    wget -nc -q http://www.dblab.ece.ntua.gr/files/classes/data.tar.gz
    echo "Download successful!"
    echo "Extracting data.tar.gz file..."
    tar -xvzf data.tar.gz
    rm data.tar.gz
    mv income ../data/secondary/median_household_incomes
    mv revgecoding.csv ../data/secondary/
    echo "Extracting completed"
fi

hdfs dfs -put ../data /user/
echo "Data is ready!"




