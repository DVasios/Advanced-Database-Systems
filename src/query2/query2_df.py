# ---- Query 2 | Dataframe API ----

query_2 = crime_data_df \
    .filter(crime_data_df['Premis Desc'] == 'STREET') \
    .withColumn( 
        'PartOfDay', 
        when((crime_data_df['TIME OCC'] >= 500) & (crime_data_df['TIME OCC'] < 1200), 'Morning') \
        .when((crime_data_df['TIME OCC'] >= 1200) & (crime_data_df['TIME OCC'] < 1700), 'Noon') \
        .when((crime_data_df['TIME OCC'] >= 1700) & (crime_data_df['TIME OCC'] < 2100), 'Afternoon') \
        .when((crime_data_df['TIME OCC'] >= 2100) & (crime_data_df['TIME OCC'] < 2400) |
              (crime_data_df['TIME OCC'] >= 0) & (crime_data_df['TIME OCC'] < 500), 'Night') \
        .otherwise('NoPartOfDay')) \
    .select(col('TIME OCC').alias('time'), col('PartOfDay')) \
    .groupBy(col('PartOfDay')).agg(count('*').alias('NumberOfCrimes')) \
    .orderBy(col('NumberOfCrimes').desc()) 

# Print Output
query_2.show()
