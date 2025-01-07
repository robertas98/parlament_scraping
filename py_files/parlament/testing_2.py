import pandas as pd
import requests
import re 
from bs4 import BeautifulSoup
import custom_functions

p_kade_id = 10
p_ses_id = 139
p_fakt_pos_id = -502011
p_bals_id = -54803
parlament_name = '2024–2028 metų kadencija'
parlament_session_name = '1 eilinė (2024-11-14 – ...)'
parlament_session_meeting_name = 'Seimo vakarinis posėdis Nr. 16 (2024-12-19)'
def split_list(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]

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

    # Extract the question
    question = soup.find('div', class_='formBalsKlausimas formBals')
    question = question.find('div', class_='dropdownOptionsContainer')
    question = question.find('div', class_='dropdownOption border-default primary-background-hover color-light-hover')
    vote_question = ''
    for q in question:
        vote_question = vote_question + ' ' + str(q)
    
    # Extract the table
    table = soup.find('table', class_='bals_table')
    
    # Extract column names from the table header
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

    # Now `td_contents` contains all rows without duplication
    print(len(td_contents))    

# rows = split_list(td_contents, 6)
# print(len(rows))