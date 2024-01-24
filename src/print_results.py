import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Read Results
df = pd.read_csv('../results/exec_times.csv')

# Query 1 | q1_df, q1_sql
q1_df = df[df['Name'] == 'q1_df']
q1_sql = df[df['Name'] == 'q1_sql']

print(q1_sql.loc[0]['Exec Time'])

x = np.array(['Dataframe API','SQL API'])
y = np.array([q1_df, q1_sql])

plt.bar(x,y)
plt.show()