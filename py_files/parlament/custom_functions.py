import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import csv 
def rename_duplicates(df):
    columns = df.columns.tolist()
    seen = set()
    for i in range(len(columns)):
        col = columns[i]
        if col in seen:
            # Find the new name for the duplicated column
            count = 1
            new_col = f"{col}_{count}"
            while new_col in seen:
                count += 1
                new_col = f"{col}_{count}"
            df.columns.values[i] = new_col
        else:
            seen.add(col)
    return df

def assign_value_column(df, exclude_columns=None):
    if exclude_columns is None:
        exclude_columns = []
    
    # Identify the columns to include (i.e., exclude specified columns)
    value_columns = [col for col in df.columns if col not in exclude_columns]
    
    # Initialize the 'value' column as empty
    df['vote_value'] = None
    
    # Iterate over the rows to find the first non-null value column
    for idx, row in df.iterrows():
        for col in value_columns:
            # if pd.notnull(row[col]):  # Check for non-null value
            if row[col] != "":  # Check for non-empty string
                df.at[idx, 'vote_value'] = col
                break
    
    # Return the desired columns: first_name and value
    return df[['Seimo narys', 'vote_value']]

def list_of_dicts_to_csv(data, csv_file):
    """
    Converts a list of dictionaries to a CSV file.
    :param data: List of dictionaries to be written to the CSV file.
    :param csv_file: The path where the CSV file will be saved.
    """
    if not data:
        print("No data provided")
        return
    
    # Writing to the CSV file
    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=data[0].keys())
        
        # Write the header
        writer.writeheader()
        
        # Write the data
        writer.writerows(data)

    print(f"Data has been written to {csv_file}")

def split_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]