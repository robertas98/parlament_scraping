import pandas as pd
import requests
import re 
from bs4 import BeautifulSoup
import custom_functions
from concurrent.futures import ThreadPoolExecutor
pd.set_option('display.max_rows', None)
from pandasql import sqldf
df_1 = pd.read_csv('results_10.csv') 
df_2 = pd.read_csv('results_9.csv') 
df_3 = pd.read_csv('results_8.csv') 
df_4 = pd.read_csv('results_7.csv') 
df_5 = pd.read_csv('results_6.csv') 
df_6 = pd.read_csv('results_5.csv') 


q = """
    SELECT
        "Seimo narys" as parlament_member
        ,vote_value	
        ,vote_question	
        ,parlament_name	
        ,parlament_session_name	
        ,parlament_session_meeting_name	
        ,vote_url
    FROM df_1
    union all 
    SELECT
        "Seimo narys" as parlament_member
        ,vote_value	
        ,vote_question	
        ,parlament_name	
        ,parlament_session_name	
        ,parlament_session_meeting_name	
        ,vote_url
    FROM df_2
    union all 
    SELECT
        "Seimo narys" as parlament_member
        ,vote_value	
        ,vote_question	
        ,parlament_name	
        ,parlament_session_name	
        ,parlament_session_meeting_name	
        ,vote_url
    FROM df_3
    union all 
    SELECT
        "Seimo narys" as parlament_member
        ,vote_value	
        ,vote_question	
        ,parlament_name	
        ,parlament_session_name	
        ,parlament_session_meeting_name	
        ,vote_url
    FROM df_4
    union all 
    SELECT
        "Seimo narys" as parlament_member
        ,vote_value	
        ,vote_question	
        ,parlament_name	
        ,parlament_session_name	
        ,parlament_session_meeting_name	
        ,vote_url
    FROM df_5
    union all 
    SELECT
        "Seimo narys" as parlament_member
        ,vote_value	
        ,vote_question	
        ,parlament_name	
        ,parlament_session_name	
        ,parlament_session_meeting_name	
        ,vote_url
    FROM df_6
    ;
    """
df = sqldf(q)
print(df.head())
df.to_csv('results_final.csv', quotechar='"',sep = ',') 