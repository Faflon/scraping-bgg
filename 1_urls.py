from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

def gather_bgg_urls(max_pages=10): #default 10 pages for testing
    # 1. Initialize Selenium (Headless mode can be added later, let's keep it visible for now to monitor it)
    options = webdriver.ChromeOptions()
    options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')
    # Disable automation flags that some firewalls look for
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    print("Initializing browser engine...")
    driver = webdriver.Chrome(options=options)
    
    games_data = []

    try:
        for page in range(1, max_pages + 1):
            url = f'https://boardgamegeek.com/browse/boardgame/page/{page}'
            print(f"\nTargeting: Page {page}...")
            
            # MANDATORY: 5-second delay to comply with BGG's robots.txt
            time.sleep(5)
            
            driver.get(url)
            
            # 2. Handle Cookie/Privacy Popups (Only heavily aggressively on the first page)
            # Even if its not technicaly required (because I only take the html structure and i'm not clicking anything on the page)
            # It is implemented to show use of Selenium and how it can handle popups.
            if page == 1:
                print("Scanning for popups...")
                
                # Attempt 1: Reject the consent popup 
                try:
                    reject_gdpr = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'fc-cta-do-not-consent')]"))
                    )
                    reject_gdpr.click()
                    print("Closed main GDPR overlay.")
                    time.sleep(1) # Brief pause to let animations finish
                except Exception:
                    print("GDPR overlay not found.")

                # Attempt 2: The bottom cookie banner ("Reject All")
                try:
                    reject_cookies = WebDriverWait(driver, 3).until(
                        EC.element_to_be_clickable((By.ID, "c-s-bn"))
                    )
                    reject_cookies.click()
                    print("Closed bottom cookie banner.")
                    time.sleep(1)
                except Exception:
                    print("Bottom cookie banner not found.")

            # 3. Extract the DOM and hand it to BeautifulSoup
            # We wait for the main table to be present before extracting
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "collectionitems"))
            )
            html_source = driver.page_source
            soup = BeautifulSoup(html_source, 'html.parser')
            
            # 4. Parse the data (The exact logic we built earlier)
            rows = soup.find_all('tr', id=re.compile(r'^row_')) #no need for using regex here because all of the rows starts with id="row_", but still added for the purpose of the exercise
            print(f"Extracted {len(rows)} games from this page.")
            
            for row in rows:
                rank_cell = row.find('td', class_='collection_rank')
                rank = rank_cell.get_text(strip=True) if rank_cell else None
                
                name_cell = row.find('td', class_='collection_objectname')
                if name_cell:
                    title_tag = name_cell.find('a')
                    if title_tag:
                        title = title_tag.get_text(strip=True)
                        partial_url = title_tag.get('href')
                        full_url = f"https://boardgamegeek.com{partial_url}"
                    else:
                        title, full_url = None, None
                        
                    year_span = name_cell.find('span', class_='smallerfont')
                    year = None
                    if year_span:
                        raw_year = year_span.get_text(strip=True)
                        year_match = re.search(r'\d{4}', raw_year) #any 4 consecutive digits
                        if year_match:
                            year = year_match.group()
                
                rating_cells = row.find_all('td', class_='collection_bggrating')
                
                # Safely extract by index, checking if the cells actually exist
                geek_rating = rating_cells[0].get_text(strip=True) if len(rating_cells) > 0 else None
                avg_rating = rating_cells[1].get_text(strip=True) if len(rating_cells) > 1 else None
                num_voters = rating_cells[2].get_text(strip=True) if len(rating_cells) > 2 else None
                            
                games_data.append({
                    'Rank': rank,
                    'Title': title,
                    'Year': year,
                    'Geek_Rating': geek_rating,
                    'Avg_Rating': avg_rating,
                    'Num_Voters': num_voters,
                    'URL': full_url
                })

    finally:
        # Guarantee the browser closes even if the script crashes
        driver.quit()
        print("Browser engine shut down.")

    # 5. Export Data
    df = pd.DataFrame(games_data)
    df.to_csv('data/raw_urls.csv', index=False)
    print(f"\nPipeline finished. {len(df)} records saved to data/raw_urls.csv")

if __name__ == "__main__":
    gather_bgg_urls(max_pages=10)