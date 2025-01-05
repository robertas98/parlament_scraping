import pandas as pd
import requests
from pyspark.sql import SparkSession
import re 
from bs4 import BeautifulSoup
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Pandas to PySpark").getOrCreate()

# URL of the HTML page containing the table
# url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id=10&p_ses_id=139&p_fakt_pos_id=-502011#balsPosedis'  # Replace with the actual URL
url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1'

# Step 1: Make a GET request to the URL with a User-Agent header
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
            # Append the extracted data to the list
        data.append({
            'p_kade_id': td.attrs['value'],
            'parlament_title': td.attrs['valuetitle'],
            #'text': text
        })

# Print the extracted data
print(data)
