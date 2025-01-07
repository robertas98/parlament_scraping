import pandas as pd
import parlament_search_parameters
from pandasql import sqldf
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

q = """
    SELECT
        *
     FROM df m
    ;
    """
df = sqldf(q)
# print(df)

for index, i in df.iterrows():  # Using iterrows to iterate over DataFrame rows
    print(i)
    df_row = parlament_search_parameters.get_parlament_vote_results(
        p_kade_id = i['p_kade_id']
        ,p_ses_id = i['p_ses_id']
        ,p_fakt_pos_id = i['p_fakt_pos_id']
        ,p_bals_id = i['p_bals_id']
        ,parlament_name = i['parlament_name']
        ,parlament_session_name = i['parlament_session_name']
        ,parlament_session_meeting_name = i['parlament_session_meeting_name']
    )
# df_row = parlament_search_parameters.get_parlament_vote_results(
#     p_kade_id = 10
#     ,p_ses_id = 139
#     ,p_fakt_pos_id = -502011
#     ,p_bals_id = -54803
#     ,parlament_name = '2024–2028 metų kadencija'
#     ,parlament_session_name = '1 eilinė (2024-11-14 – ...)'
#     ,parlament_session_meeting_name = 'Seimo vakarinis posėdis Nr. 16 (2024-12-19)'
# )
    # print(df_row)
    main_df = pd.concat([main_df, df_row], ignore_index=True)

main_df.to_excel('all_parlament_decisions.xlsx') 