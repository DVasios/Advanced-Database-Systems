# RDD Dataframe
rdd_from_df = crime_data_df.rdd

# ---- Query 2 | RDD API ----

def categorize(x): 
    if x[0] >= 500 and x[0] < 1200:
        return ('Morning', 1)
    elif x[0] >= 1200 and x[0] < 1700:
        return ('Noon', 1)
    elif x[0] >= 1700 and x[0] < 2100:
        return ('Afternoon', 1)
    elif (x[0] >= 2100 and x[0] < 2400) or (x[0] >= 0 and x[0] < 500):
        return ('Night', 1)
        
# rdd = sc.textFile("hdfs://okeanos-master:54310/user/data/primary/crime_data.csv") 
# rdd = sc.textFile("../data/primary/crime_data.csv") 

data_rdd = rdd_from_df \
                .map(lambda x: (int(x[3]), str(x[15]))) \
                .filter(lambda x: x[1] == "STREET") \
                .map(categorize) \
                .reduceByKey(lambda x, y: x+y) 

result = data_rdd.take(4)

columns = [ 'PartOfDay', 'NumberOfCrimes']
rows = []
for i in result: 
    rows.append([i[0], i[1]])
df = pd.DataFrame(rows, columns=columns)
print(df.sort_values(by='NumberOfCrimes', ascending=False, ignore_index=True))