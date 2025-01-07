import pandas as pd
import parlament_search_parameters
from pandasql import sqldf
import custom_functions
from concurrent.futures import ThreadPoolExecutor
pysqldf = lambda q: sqldf(q, globals())
main_df = pd.DataFrame(columns=[
    'Seimo narys'
    , 'vote_value'
    ,'vote_question'
    ,'parlament_name'
    ,'parlament_session_name'
    ,'parlament_session_meeting_name'
    ,'vote_url'
    ])

df = pd.read_excel('all_votes_keys.xlsx')

# can choose to scrape rows matching certain criteria
q = """
    SELECT
        *
     FROM df m
     where 
        p_kade_id = 5
    ;
    """
df = sqldf(q)

for i in parlament_search_parameters.run_parallel_requests(df):
    main_df =pd.concat([main_df, i], ignore_index=True) 

print('df length', len(main_df))
print('scraping finished, writing to CSV')
main_df.to_csv('all_parlament_decisions_kade_5.csv', quotechar='"',sep = ',') 