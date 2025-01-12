import pandas as pd
import parlament_search_parameters
from pandasql import sqldf
import custom_functions
from concurrent.futures import ThreadPoolExecutor
# main_df = pd.DataFrame(columns=[
#     'Seimo narys'
#     , 'vote_value'
#     ,'vote_question'
#     ,'parlament_name'
#     ,'parlament_session_name'
#     ,'parlament_session_meeting_name'
#     ,'vote_url'
#     ])

df = pd.read_excel('all_votes_keys.xlsx')
# print(df)

# can choose to scrape rows matching certain criteria
p_kade_ids = [10,9,8,7,6,5]
for p_kade_id in p_kade_ids:
    main_df = pd.DataFrame(columns=[
        'Seimo narys'
        , 'vote_value'
        ,'vote_question'
        ,'parlament_name'
        ,'parlament_session_name'
        ,'parlament_session_meeting_name'
        ,'vote_url'
    ])
    q = """
        SELECT
            *
        FROM df m
        where 
            m.p_kade_id = {p_kade_id}
        ;
        """.format(p_kade_id = p_kade_id)
    df_kade = sqldf(q)

    for i in parlament_search_parameters.run_parallel_requests(df_kade):
        main_df =pd.concat([main_df, i], ignore_index=True) 

    print('df length', len(main_df))
    print('scraping finished, writing to CSV')
    main_df.to_csv(
        'results_{p_kade_id}.csv'.format(p_kade_id = p_kade_id)
        , quotechar='"'
        ,sep = ','
        ) 