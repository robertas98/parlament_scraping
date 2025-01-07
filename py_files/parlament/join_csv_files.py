import pandas as pd
from pandasql import sqldf

df_1 = pd.read_csv('all_parlament_decisions_kade_5.csv') 
df_2 = pd.read_csv('all_parlament_decisions_kade_6.csv') 
df_3 = pd.read_csv('all_parlament_decisions_kade_7.csv') 
df_4 = pd.read_csv('all_parlament_decisions_kade_8.csv') 
df_5 = pd.read_csv('all_parlament_decisions_kade_9.csv') 
df_6 = pd.read_csv('all_parlament_decisions_kade_10.csv') 


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
df.to_csv('all_parlament_decisions_final.csv', quotechar='"',sep = ',') 