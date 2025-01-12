import pandas as pd
import requests
import re 
from bs4 import BeautifulSoup
import custom_functions
from concurrent.futures import ThreadPoolExecutor
from itertools import chain



"""
Parlament votes are retrieves using variables 
 - parlament_id/p_kade_id,
 - session_id/p_ses_id
 - session_meeting_id/p_fakt_pos_id
 - meeting_question_id/p_bals_id

 In order to retrieve value for all session_ids I first need to pass all parlament_id to http request and so forth
 e.g in order to get session_meeting_ids, I need to pass http request with parlament_id,session_id <...>
"""
class parlament_search_keys():
    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
        }
    def get_parlament_ids(self):
        url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1'
        headers = self.headers
        data=[]
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('div', class_='dropdownKadencija dropdownOptions')
            table = table.find_all('div', class_='dropdownOption border-default primary-background-hover color-light-hover')
            for td in table:
                data.append({
                    'p_kade_id': td.attrs['value'],
                    'parlament_title': td.attrs['valuetitle'],
                    #'text': text
                })
        else:
            print(f" (get_parlament_ids) Failed to retrieve the page. Status code: {response.status_code}")
        return data

    def get_parlament_session_ids(self,p_kade_id,parlament_title):
        url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}'.format(p_kade_id = p_kade_id)
        headers = self.headers
        data=[]
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            html_string = response.text
            soup = BeautifulSoup(html_string, 'html.parser')
            sessions = soup.find('div', class_ ='dropdownSesija dropdownOptions')
            sessions_individual = sessions.findAll('div', class_ ='dropdownOption border-default primary-background-hover color-light-hover')
            for attribute in sessions_individual:
                data.append({
                        'p_ses_id': attribute.attrs['value'],
                        'p_session_name': attribute.attrs['valuetitle'],
                        'p_kade_id': p_kade_id,
                        'parlament_title': parlament_title
                        #'text': text
                    })
        else:
            print(f"(get_parlament_session_ids) Failed to retrieve the page. Status code: {response.status_code}")
        return data 
    
    def get_session_meeting_ids(self,p_kade_id,p_ses_id,parlament_title,p_session_name):
        url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}&p_ses_id={p_ses_id}#balsSesija'.format(p_kade_id = p_kade_id, p_ses_id = p_ses_id)
        headers = self.headers
        data=[]
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            html_string = response.text
            soup = BeautifulSoup(html_string, 'html.parser')
            sessions = soup.find('div', class_ ='dropdownPosedis dropdownOptions')
            sessions_individual = sessions.findAll('div', class_ ='dropdownOption border-default primary-background-hover color-light-hover')
            for attribute in sessions_individual:
                data.append({
                        'p_fakt_pos_id': attribute.attrs['value'],
                        'p_session_meeting_name': attribute.attrs['valuetitle'],
                        'p_kade_id': p_kade_id,
                        'p_ses_id': p_ses_id,
                        'parlament_title': parlament_title,
                        'p_session_name' : p_session_name
                    })
        else:
            print(f"(get_session_meeting_ids) Failed to retrieve the page. Status code: {response.status_code}")
        return data 

    def get_session_meeting_question_ids(self,p_kade_id,p_ses_id,p_fakt_pos_id,parlament_title,p_session_name,p_session_meeting_name):
        url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}&p_ses_id={p_ses_id}&p_fakt_pos_id={p_fakt_pos_id}'\
            .format(p_kade_id = p_kade_id
                    , p_ses_id = p_ses_id
                    , p_fakt_pos_id = p_fakt_pos_id
                    )
        headers = self.headers
        data=[]
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            html_string = response.text
            soup = BeautifulSoup(html_string, 'html.parser')
            session_meeting_question = soup.find('div', class_ ='dropdownKlausimas dropdownOptions')
            session_meeting_question_individual = session_meeting_question.findAll('div', class_ ='dropdownOption border-default primary-background-hover color-light-hover')
            for attribute in session_meeting_question_individual:
                # print(attribute)
                data.append({
                        'p_bals_id': attribute.attrs['value'],
                        'p_kade_id': p_kade_id,
                        'p_ses_id': p_ses_id, 
                        'p_fakt_pos_id':p_fakt_pos_id,
                        'parlament_title': parlament_title,
                        'p_session_name' : p_session_name,
                        'p_session_meeting_name': p_session_meeting_name
                        #'text': text
                    })
        else:
            print(f" (get_session_meeting_question_ids) Failed to retrieve the page. Status code: {response.status_code}")
        return data 
    
    def get_parlament_vote_results(self
                                ,p_kade_id
                               ,p_ses_id
                               ,p_fakt_pos_id
                               ,p_bals_id
                               ,parlament_name
                               ,parlament_session_name
                               ,parlament_session_meeting_name
                            #    ,main_df_name
                               ):
        url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}&p_ses_id={p_ses_id}&p_fakt_pos_id={p_fakt_pos_id}&p_bals_id={p_bals_id}#balsKlausimas'\
            .format(p_kade_id = p_kade_id
                    , p_ses_id = p_ses_id
                    , p_fakt_pos_id = p_fakt_pos_id
                    , p_bals_id = p_bals_id
                    ) 
        print('url: ',url)
        headers = self.headers
        with requests.Session() as session:
            response = session.get(url, headers=headers)
            rows = []
            vote_question = ''
            td_contents = []
            if response.status_code == 200:
                html_string = response.text
                soup = BeautifulSoup(html_string, 'html.parser')
                question = soup.find('div', class_ ='formBalsKlausimas formBals')
                question = question.find('div', class_ ='dropdownOptionsContainer')
                question = question.find('div',class_ = 'dropdownOption border-default primary-background-hover color-light-hover')
                for q in question:
                    vote_question = vote_question + ' ' + str(q)
                table = soup.find('table', class_='bals_table')
                column_names = [header.text.strip() for header in table.find_all('th')]
                # Prepare to store the data
                td_contents = []
                # Iterate over table rows (skip the header row)
                for html_row in table.find_all('tr')[1:]:  # Skip the header row
                    row_data = []  # Store data for each row    
                    # Extract the name (assuming it's inside an <a> tag in the first <td>)
                    name_td = html_row.find('td')  # Find the first td which should contain the name
                    if name_td:
                        name = name_td.find('a')
                        if name:
                            row_data.append(name.text.strip())  # Add name to row data  
                    # Now extract other columns for each row
                    for td in html_row.find_all('td')[1:]:  # Skip the first td since it's already handled for the name
                        background_color = td.get('style')
                        td_text = td.text.strip()  # Extract the text from the td    
                        if background_color:
                            # If there is a background color, try to extract the color
                            match = re.search(r'background:\s*(#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3}|rgb\([0-9, ]+\))', background_color)
                            if match:
                                row_data.append(match.group(1))  # Append color to row data
                            else:
                                row_data.append(td_text)  # Otherwise append the text
                        else:
                            row_data.append(td_text)  # Append the plain text if no background color is found

                    td_contents.append(row_data)  # Add the complete row data to td_contents
                rows = td_contents
                try:
                    df = pd.DataFrame(rows, columns=column_names)
                except Exception as e:
                    with open('errors.txt', 'w') as f:
                        f.write(f"Something went wrong for URL: {url}\n")
                        f.write(f"Error: {e}\n")
                    print(f"An error occurred, details have been logged to 'errors.txt'.")
                print('DF LENGTH original',len(df.index))
                if not df.columns.is_unique:
                    df = custom_functions.rename_duplicates(df)
                df= custom_functions.assign_value_column(df,'Seimo narys')
                df['vote_question'] = vote_question
                df['parlament_name'] = parlament_name
                df['parlament_session_name'] = parlament_session_name
                df['parlament_session_meeting_name'] = parlament_session_meeting_name
                df['vote_url'] = url
                return df
            else:
                print(f"Failed to retrieve the page. Status code: {response.status_code}")

def proc_extract_parlament_search_parameters():
    parlament_functions = parlament_search_keys()
    parlament_ids = parlament_functions.get_parlament_ids()
    with ThreadPoolExecutor() as executor:
    
        session_ids = list(
            executor.map(
                lambda row: parlament_functions.get_parlament_session_ids( 
                    p_kade_id=row['p_kade_id']
                    ,parlament_title = row['parlament_title']
                )
                , parlament_ids
            )
        )
        meeting_ids = list(
            executor.map(
                lambda row: parlament_functions.get_session_meeting_ids(
                    p_kade_id=row['p_kade_id']
                    ,p_ses_id = row['p_ses_id']
                    ,parlament_title = row['parlament_title']
                    ,p_session_name = row['p_session_name']
                )
                , chain.from_iterable(session_ids)
            )
        )
        question_ids = list(
            executor.map(
                lambda row: parlament_functions.get_session_meeting_question_ids(
                        p_kade_id=row['p_kade_id']
                        ,p_ses_id = row['p_ses_id']
                        ,p_fakt_pos_id = row['p_fakt_pos_id']
                        ,parlament_title = row['parlament_title']
                        ,p_session_name = row['p_session_name']
                        ,p_session_meeting_name = row['p_session_meeting_name']
                )
                , chain.from_iterable(meeting_ids)
            )
        )
    question_ids =  list(chain.from_iterable(question_ids))
    return question_ids


def proc_fetch_vote_results(search_keys_dataframe):
    parlament_functions = parlament_search_keys()
    main_df = pd.DataFrame(columns=[
            'Seimo narys'
            , 'vote_value'
            ,'vote_question'
            ,'parlament_name'
            ,'parlament_session_name'
            ,'parlament_session_meeting_name'
            ,'vote_url'
        ])
    with ThreadPoolExecutor() as executor:
        # parlament_votes = list(executor.map(parlament_functions.get_parlament_vote_results([lambda row: row for _, row in search_keys_dataframe.iterrows()])))
        parlament_votes = list(
            executor.map(
                lambda row: parlament_functions.get_parlament_vote_results(
                p_kade_id=row['p_kade_id'],
                p_ses_id=row['p_ses_id'],
                p_fakt_pos_id=row['p_fakt_pos_id'],
                p_bals_id=row['p_bals_id'],
                parlament_name=row['parlament_title'],
                parlament_session_name=row['p_session_name'],
                parlament_session_meeting_name=row['p_session_meeting_name']
                )
                , [row for _, row in search_keys_dataframe.iterrows()]
            )
        )
        # main_df = pd.DataFrame(parlament_votes)
        for i in parlament_votes:
            main_df =pd.concat([main_df, i], ignore_index=True) 
    return main_df



