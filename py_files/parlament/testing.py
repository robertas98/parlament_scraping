import pandas as pd
import requests
import re 
from bs4 import BeautifulSoup
import custom_functions
from concurrent.futures import ThreadPoolExecutor
import parlament_search_parameters 
pd.set_option('display.max_rows', None)
pd.set_option('display.max_colwidth', None)
from pandasql import sqldf
# df_1 = pd.read_csv('results_final.csv') 
# df_2 = pd.read_excel('all_votes_keys.xlsx') 

parlament_functions = parlament_search_parameters.parlament_search_keys()
parlament_ids = parlament_functions.get_parlament_ids()
def run_parallel_requests(df, max_workers=30):
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Use executor.map to apply fetch_vote_results concurrently to each row in the DataFrame
        results = list(executor.map(fetch_vote_results, [row for _, row in df.iterrows()]))
    return results



# q = """
#     SELECT distinct 
#             parlament_member
#             ,vote_value	
#             ,vote_question	
#             ,parlament_name	
#             ,parlament_session_name	
#             ,parlament_session_meeting_name	
#             ,vote_url
#         FROM df_1
        
#     """
# df = sqldf(q)
# df.to_csv('parlament_votes_final.csv', quotechar='"',sep = ',') 