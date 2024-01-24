import pandas as pd
import os

file_path = '../results/exec_times.csv'

def export(name, time):
    # Check if the file already exists
    if not os.path.exists(file_path):

        # Create a DataFrame with some sample numbers
        df = pd.DataFrame(columns=['Name', 'Exec Time'], index=None)
        df = pd.concat([df, pd.DataFrame({'Name': [name], 'Exec Time': [time]})], ignore_index=True)

        # Save the DataFrame to a CSV file
        df.to_csv(file_path, index=False)
        print(f"File '{file_path}' created and saved.")
    else:
        # If the file already exists, load the data from the existing file
        df = pd.read_csv(file_path)
        df = pd.concat([df, pd.DataFrame({'Name': [name], 'Exec Time': [time]})], ignore_index=True)

        # Save the DataFrame to a CSV file
        df.to_csv(file_path, index=False)
        print(f"File '{file_path}' created and saved.") 