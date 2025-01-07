import pandas as pd
import requests
import re 
from bs4 import BeautifulSoup
import custom_functions
# Initialize Spark session
# spark = SparkSession.builder.master("local").appName("Pandas to PySpark").getOrCreate()
# URL of the HTML page containing the table
# url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id=10&p_ses_id=139&p_fakt_pos_id=-502011#balsPosedis'  # Replace with the actual URL
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

def split_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def get_parlament_ids():
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
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

def get_parlament_session_ids(p_kade_id):
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}'.format(p_kade_id = p_kade_id)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
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
                    #'text': text
                })
    else:
        print(f"(get_parlament_session_ids) Failed to retrieve the page. Status code: {response.status_code}")
    return data 
def get_session_meeting_ids(p_kade_id,p_ses_id):
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}&p_ses_id={p_ses_id}#balsSesija'.format(p_kade_id = p_kade_id, p_ses_id = p_ses_id)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
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
                    #'text': text
                })
    else:
        print(f"(get_session_meeting_ids) Failed to retrieve the page. Status code: {response.status_code}")
    return data 

def get_session_meeting_question_ids(p_kade_id,p_ses_id,p_fakt_pos_id):
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}&p_ses_id={p_ses_id}&p_fakt_pos_id={p_fakt_pos_id}'\
        .format(p_kade_id = p_kade_id
                , p_ses_id = p_ses_id
                , p_fakt_pos_id = p_fakt_pos_id
                )
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3029.110 Safari/537.36'
    }
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
                    #'text': text
                })
    else:
        print(f" (get_session_meeting_question_ids) Failed to retrieve the page. Status code: {response.status_code}")
    return data 

def get_parlament_vote_results(p_kade_id
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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }
    response = requests.get(url, headers=headers)
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
        for html_row in table.find_all('tr')[1:]:  # Skip the header row
            name = table.find('a').text.strip()  # Extract text inside <a> tag
            for td in table.find_all('td'):
                background_color = td.get('style')
                
                if background_color:
                    match = re.search(r'background:\s*(#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3}|rgb\([0-9, ]+\))', background_color)
                    if match:
                        td_contents.append(match.group(1))
                    else:
                        td_contents.append(td.text.strip())
                else:
                    td_contents.append(td.text.strip())         
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")
    rows = split_list(td_contents, 6)
    df = pd.DataFrame(rows, columns=column_names)
    print('DF LENGTH original',len(df.index))
    if not df.columns.is_unique:
        df = custom_functions.rename_duplicates(df)
    df= custom_functions.assign_value_column(df,'Seimo narys')
    # print(df)
    df['vote_question'] = vote_question
    df['parlament_name'] = parlament_name
    df['parlament_session_name'] = parlament_session_name
    df['parlament_session_meeting_name'] = parlament_session_meeting_name
    df['vote_url'] = url
    # try:
    #     main_df_name = pd.concat([main_df_name, df], ignore_index=True)
    # except:
    #     print(df)
    return df

# data = get_parlament_vote_results(9,138,-501993)
# print(data)
