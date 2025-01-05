import pandas as pd
import requests
from pyspark.sql import SparkSession
import re 
from bs4 import BeautifulSoup
# Initialize Spark session
spark = SparkSession.builder.master("local").appName("Pandas to PySpark").getOrCreate()

# URL of the HTML page containing the table
url = 'https://www.lrs.lt/sip/portal.show?p_r=37067&p_k=1&p_kade_id=10&p_ses_id=139&p_fakt_pos_id=-501996&p_bals_id=-54448#balsKlausimas'  # Replace with the actual URL

# Step 1: Make a GET request to the URL with a User-Agent header
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
}

response = requests.get(url, headers=headers)
rows = []
# headers = ''
# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Step 2: Parse the HTML content with Pandas
    html_string = response.text
    soup = BeautifulSoup(html_string, 'html.parser')

    # Find all <table> elements
    # tables = soup.find_all('table')
    table = soup.find('table', class_='bals_table')
    headers = [header.text.strip() for header in table.find_all('th')] 
    for html_row in table.find_all('tr')[1:]:  # Skip the header row
        # Extract the name from the <a> tag (first <td>)
        name = table.find('a').text.strip()  # Extract text inside <a> tag
        # Loop through all <td> elements and extract background color as content if it exists
        td_contents = []
        for td in table.find_all('td'):
            # Check if the <td> has a background color defined in the style
            background_color = td.get('style')
            
            if background_color:
                # Extract the background color using regex
                match = re.search(r'background:\s*(#[0-9a-fA-F]{6}|#[0-9a-fA-F]{3}|rgb\([0-9, ]+\))', background_color)
                if match:
                    # If a background color is found, set the background color as the content
                    td_contents.append(match.group(1))
                else:
                    # If no valid background color, use the text content of the <td>
                    td_contents.append(td.text.strip())
            else:
                # If no background color, use the text content of the <td>
                td_contents.append(td.text.strip())
                
else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")

# df_pandas = pd.DataFrame(rows, columns=headers)
# print(headers)
def split_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]

# Split the list every 6th element
rows = split_list(td_contents, 6)

# print(result[0])
# print(td_contents[])
df = pd.DataFrame(rows, columns=headers)

# Display the DataFrame
df.to_excel("data/output.xlsx")  