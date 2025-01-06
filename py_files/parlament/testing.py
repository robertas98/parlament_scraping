import pandas as pd
import requests
from pyspark.sql import SparkSession
import re 
from bs4 import BeautifulSoup
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Pandas to PySpark").getOrCreate()

# URL of the HTML page containing the table
url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id=9&p_ses_id=138&p_fakt_pos_id=-501993#balsPosedis'  # Replace with the actual URL

# Step 1: Make a GET request to the URL with a User-Agent header
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

response = requests.get(url, headers=headers)
data = []
# headers = ''
# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Step 2: Parse the HTML content with Pandas
    html_string = response.text
    # print(html_string)
    # print(response.text)
    # print(html_string)
    soup = BeautifulSoup(html_string, 'html.parser')
    session_meeting_question = soup.find('div', class_ ='dropdownKlausimas dropdownOptions')
    session_meeting_question_individual = session_meeting_question.findAll('div', class_ ='dropdownOption border-default primary-background-hover color-light-hover')
    for attribute in session_meeting_question_individual:
        # print(attribute)
        data.append({
                'p_bals_id': attribute.attrs['value'],
                #'text': text
            })
    # question = question.find('div', class_ ='dropdownOptionsContainer')
    # question = question.find('div',class_ = 'dropdownOption border-default primary-background-hover color-light-hover')
    # for q in question:
    #     vote_question = vote_question + ' ' + str(q)
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")

print(data)