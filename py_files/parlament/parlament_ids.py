import pandas as pd
import requests
from pyspark.sql import SparkSession
import re 
from bs4 import BeautifulSoup
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Pandas to PySpark").getOrCreate()
# URL of the HTML page containing the table
# url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id=10&p_ses_id=139&p_fakt_pos_id=-502011#balsPosedis'  # Replace with the actual URL
headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

def get_parlament_ids():
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1'
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
    return data

def get_parlament_session_ids(p_kade_id):
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}'.format(p_kade_id = p_kade_id)
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
            print(f"Failed to retrieve the page. Status code: {response.status_code}")
    return data 
def get_session_meeting_ids(p_kade_id,p_ses_id):
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}&p_ses_id={p_ses_id}#balsSesija'.format(p_kade_id = p_kade_id, p_ses_id = p_ses_id)
    data=[]
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        html_string = response.text
        soup = BeautifulSoup(html_string, 'html.parser')
        sessions = soup.find('div', class_ ='dropdownSesija dropdownOptions')
        sessions_individual = sessions.findAll('div', class_ ='dropdownOption border-default primary-background-hover color-light-hover')
        for attribute in sessions_individual:
            data.append({
                    'p_fakt_pos_id': attribute.attrs['value'],
                    'p_session_meeting_name': attribute.attrs['valuetitle'],
                    #'text': text
                })
        else:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")
    return data 

def get_session_meeting_question_ids(p_kade_id,p_ses_id,p_fakt_pos_id):
    url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id={p_kade_id}&p_ses_id={p_ses_id}&p_fakt_pos_id={p_fakt_pos_id}#balsPosedis'\
        .format(p_kade_id = p_kade_id
                , p_ses_id = p_ses_id
                , p_fakt_pos_id = p_fakt_pos_id
                )
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
            print(f"Failed to retrieve the page. Status code: {response.status_code}")
    return data 
data = get_session_meeting_question_ids(9,138,-501993)
for i in data:
    print(i)
