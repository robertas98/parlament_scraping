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
                                # , 'Prieš'
                                # ,'Susilaikė'
                                # ,'Nedalyvavo'
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
                # df = parlament_search_parameters.get_parlament_vote_results(p_kade_id
                #                ,p_ses_id
                #                ,p_fakt_pos_id
                #                ,p_bals_id
                #                ,parlament_name
                #                ,parlament_session_name
                #                ,parlament_session_meeting_name
                #                ,main_df_name = main_df
                #                ) 
                # main_df = pd.concat([main_df, df], ignore_index=True)
                      
        # main_df.to_excel("{p_kade_id}_{p_ses_id}.xlsx".format(p_kade_id=p_kade_id,p_ses_id=p_ses_id))
vote_ids_df.to_excel('all_votes_keys.xlsx') 
# url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id=10&p_ses_id=139&p_fakt_pos_id=-501996&p_bals_id=-54448#balsKlausimas'  # Replace with the actual URL

# headers = {
#     'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
# }

# response = requests.get(url, headers=headers)
# rows = []
# vote_question = ''
# if response.status_code == 200:
#     html_string = response.text
#     soup = BeautifulSoup(html_string, 'html.parser')
#     question = soup.find('div', class_ ='formBalsKlausimas formBals')
#     question = question.find('div', class_ ='dropdownOptionsContainer')
#     question = question.find('div',class_ = 'dropdownOption border-default primary-background-hover color-light-hover')
#     for q in question:
#         vote_question = vote_question + ' ' + str(q)
#     table = soup.find('table', class_='bals_table')
#     headers = [header.text.strip() for header in table.find_all('th')] 
#     for html_row in table.find_all('tr')[1:]:  # Skip the header row
#         name = table.find('a').text.strip()  # Extract text inside <a> tag
#         td_contents = []
#         for td in table.find_all('td'):
#             background_color = td.get('style')
            
#             if background_color:
#                 match = re.search(r'background:\s*(#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3}|rgb\([0-9, ]+\))', background_color)
#                 if match:
#                     td_contents.append(match.group(1))
#                 else:
#                     td_contents.append(td.text.strip())
#             else:
#                 td_contents.append(td.text.strip())         
# else:
#     print(f"Failed to retrieve the page. Status code: {response.status_code}")


# def split_list(lst, n):
#     return [lst[i:i + n] for i in range(0, len(lst), n)]

# # Split the list every 6th element
# rows = split_list(td_contents, 6)

# df = pd.DataFrame(rows, columns=headers)
# df['vote_question'] = vote_question

