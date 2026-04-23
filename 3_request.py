import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def scrape_spiel_des_jahres():
    print("Initializing requests scraper for Wikipedia...")
    
    url = "https://en.wikipedia.org/wiki/Spiel_des_Jahres"
    headers = {
        'User-Agent': 'UniversityBoardGameProject/1.0 (my_email@student.edu) python-requests/2.31'
    }

    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
        return

    print("Page retrieved successfully. Parsing HTML...")
    soup = BeautifulSoup(response.text, 'html.parser')

    tables = soup.find_all('table', class_='wikitable sortable')
    
    if not tables:
        print("Could not find the awards table.")
        return

    main_table = tables[0]
    awards_data = []

    # Skip the header row [1:]
    rows = main_table.find_all('tr')[1:]
    print(f"Found {len(rows)} award winners. Extracting...")

    for row in rows:
        cols = row.find_all(['td', 'th'])
        
        # Ensure it's a valid data row
        if len(cols) >= 2:
            year_text = cols[0].get_text(strip=True)
            game_text = cols[1].get_text(strip=True)

            # --- DATA CLEANING ---
            # 1. Remove Wikipedia citation brackets e.g. "Catan[1]" -> "Catan"
            clean_title = re.sub(r'\[\d+\]', '', game_text)

            # 2. Only grab the first 4 digits of the year in case there are annotations
            year_match = re.search(r'\d{4}', year_text)
            clean_year = year_match.group() if year_match else year_text

            awards_data.append({
                'Title': clean_title,
                'Year_Won': clean_year,
                'Spiel_des_Jahres': True
            })

    # Export to CSV
    df = pd.DataFrame(awards_data)
    df.to_csv('data/awards.csv', index=False)
    print(f"SUCCESS: {len(df)} winners saved to data/awards.csv")

if __name__ == "__main__":
    scrape_spiel_des_jahres()