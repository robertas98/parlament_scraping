import pandas as pd
from concurrent.futures import ThreadPoolExecutor
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
            if pd.notnull(row[col]):  # Check for non-null value
                df.at[idx, 'vote_value'] = col
                break
    
    # Return the desired columns: first_name and value
    return df[['Seimo narys', 'vote_value']]

