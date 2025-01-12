# parlament_scraping
### project goal is to retrieve Lithuania's Parlament members vote information 
- main.py file has example commands used to retrieve parlament vote information
- parlament_search_parameters.py file has defined functions that were used in main.py
- join_csv_files.py provides example how merge all votes information into single file.

  ### present limitations
- In current version, it may require to run code multiple times in batches in order to retrieve all information, since code break sometimes due to Connection termination errors.
- Questions voted upon often contain HTML tags that need to be cleaned
