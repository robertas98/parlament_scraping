import pandas as pd
import requests
import re 
from bs4 import BeautifulSoup
import parlament_search_parameters
import custom_functions
pd.set_option('display.max_columns', None)
# Initialize Spark session
main_df = pd.DataFrame(columns=['Seimo narys'
                                , 'vote_value'
                                ,'vote_question'
                                ,'parlament_name'
                                ,'parlament_session_name'
                                ,'parlament_session_meeting_name'
                                ,'vote_url'
                                ])
vote_ids_df = pd.DataFrame(columns=['p_kade_id'
                                , 'parlament_name'
                                ,'p_ses_id'
                                ,'parlament_session_name'
                                ,'p_fakt_pos_id'
                                ,'parlament_session_meeting_name'
                                ,'p_bals_id'
                                ])
parlament_ids = parlament_search_parameters.get_parlament_ids()
for parlament_id in parlament_ids:
    parlament_name = parlament_id['parlament_title']
    p_kade_id = parlament_id['p_kade_id']
    parlament_session_ids = parlament_search_parameters.get_parlament_session_ids(p_kade_id=p_kade_id)

    for parlament_session_id in parlament_session_ids:

        p_ses_id = parlament_session_id['p_ses_id']
        parlament_session_name = parlament_session_id['p_session_name']
        parlament_session_meeting_ids = parlament_search_parameters.get_session_meeting_ids(p_kade_id=p_kade_id
                                                                                            ,p_ses_id=p_ses_id
                                                                                            )

        for parlament_session_meeting_id in parlament_session_meeting_ids:
            p_fakt_pos_id = parlament_session_meeting_id['p_fakt_pos_id']
            parlament_session_meeting_name = parlament_session_meeting_id['p_session_meeting_name']
            parlament_session_meeting_question_ids = parlament_search_parameters.get_session_meeting_question_ids(p_kade_id=p_kade_id
                                                                                                                   ,p_ses_id=p_ses_id
                                                                                                                   ,p_fakt_pos_id=p_fakt_pos_id
                                                                                                                   )

            for parlament_session_meeting_question_id in parlament_session_meeting_question_ids:
                p_bals_id = parlament_session_meeting_question_id['p_bals_id']
                new_row = {  'p_kade_id' :p_kade_id
                            , 'parlament_name' : parlament_name
                            ,'p_ses_id' : p_ses_id 
                            ,'parlament_session_name' : parlament_session_name
                            ,'p_fakt_pos_id' : p_fakt_pos_id
                            ,'parlament_session_meeting_name' : parlament_session_meeting_name 
                            ,'p_bals_id' : p_bals_id
                            }
                vote_ids_df.loc[len(vote_ids_df)] = new_row
                print('new_row',new_row)
                      
vote_ids_df.to_excel('all_votes_keys.xlsx') 
