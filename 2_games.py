import scrapy
from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

# --- MIDDLEWARE INTERCEPTOR ---
class SeleniumMiddleware:
    @classmethod
    def from_crawler(cls, crawler):
        middleware = cls()
        crawler.signals.connect(middleware.spider_closed, signal=signals.spider_closed)
        return middleware

    def __init__(self):
        self.request_count = 0
        self.restart_threshold = 75 # Restart browser every 75 pages to clear RAM
        self.driver = None
        self._start_browser()

    def _start_browser(self):
        if self.driver:
            print("Shutting down old browser instance...")
            self.driver.quit()
            time.sleep(5) # Give the OS a second to clear the RAM
            
        print("Initializing fresh Selenium WebDriver...")
        options = Options()
        options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        self.driver = webdriver.Chrome(options=options)
        # THE HANG FIX: If a page takes longer than 30 seconds to load, abort.
        self.driver.set_page_load_timeout(30) 
        self.first_page_loaded = False

    def process_request(self, request, spider):
        self.request_count += 1
        
        # THE RAM FIX: Restart the browser periodically
        if self.request_count % self.restart_threshold == 0:
            print(f"\n[SYSTEM] Reached {self.request_count} requests. Restarting browser to clear memory...\n")
            self._start_browser()

        print(f"Selenium intercepting request: {request.url}")
        
        try:
            self.driver.get(request.url)
        except Exception as e:
            # If the page hangs, we catch it here. 
            # Returning a 500 status tells Scrapy's internal engine to Retry this URL later.
            print(f"Page load timeout or error. Flagging for Scrapy Retry.")
            return HtmlResponse(request.url, status=500, request=request)
        
        time.sleep(5) 
        
        if not self.first_page_loaded:
            try:
                reject_gdpr = WebDriverWait(self.driver, 4).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[contains(@class, 'fc-cta-do-not-consent')]"))
                )
                reject_gdpr.click()
                time.sleep(1)
            except Exception:
                pass
            try:
                reject_cookies = WebDriverWait(self.driver, 4).until(
                    EC.element_to_be_clickable((By.ID, "c-s-bn"))
                )
                reject_cookies.click()
                time.sleep(1)
            except Exception:
                pass
            self.first_page_loaded = True

        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, '//a[contains(@href, "/boardgamedesigner/")]'))
            )
        except Exception:
            pass

        body = self.driver.page_source
        return HtmlResponse(self.driver.current_url, body=body, encoding='utf-8', request=request)

    def spider_closed(self, spider):
        print("Scraping finished. Shutting down Selenium...")
        if self.driver:
            self.driver.quit()


# --- THE SCRAPY SPIDER ---
class BGGDeepSpider(scrapy.Spider):
    name = 'bgg_deep'
    
    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            '__main__.SeleniumMiddleware': 543,
        },
        'ROBOTSTXT_OBEY': False, # Disabled to prevent WAF startup crash; 5s delay enforced in middleware
        'LOG_LEVEL': 'INFO',
        'FEEDS': {
            'data/metadata.csv': {'format': 'csv', 'overwrite': True}
        }
    }

    def start_requests(self):
        try:
            df = pd.read_csv('data/raw_urls.csv')
        except FileNotFoundError:
            print("Error: raw_urls.csv not found.")
            return

        # TEST MODE: Still grabbing just the first 3 URLs
        #urls_to_scrape = df['URL'].dropna().tolist()[:3]
        urls_to_scrape = df['URL'].dropna().tolist()
        
        for url in urls_to_scrape:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        # 1. Number of Players
        raw_players = response.xpath('//li[@itemprop="numberOfPlayers"]//p[contains(@class, "gameplay-item-primary")]//text()').getall()
        players = "".join([text.strip() for text in raw_players if text.strip()]).replace("Players", "").strip() if raw_players else None

        # 2. Playing Time
        raw_playtime = response.xpath('//li[.//h3[text()="Play Time"]]//p[contains(@class, "gameplay-item-primary")]//text()').getall()
        playtime = "".join([text.strip() for text in raw_playtime if text.strip()]).replace("Min", "").strip() if raw_playtime else None
        
        # 3. Age Requirement
        raw_age = response.xpath('//li[.//h3[text()="Suggested Age"]]//p[contains(@class, "gameplay-item-primary")]//text()').getall()
        age = "".join([text.strip() for text in raw_age if text.strip()]).replace("Age:", "").strip() if raw_age else None
        
        # 4. Weight / Complexity
        weight = response.xpath('//li[.//h3[text()="Complexity"]]//span[contains(@class, "gameplay-weight")]//text()').get()
        complexity = weight.strip() if weight else None

        # 5. Designer (Filtered and Semicolon Separated)
        raw_designers = response.xpath('//li[.//h4//strong[text()="Designer"]]//popup-list//a/span//text() | //li[.//h4//strong[text()="Designer"]]//popup-list//span[@itemprop="name"]//text()').getall()
        designer = "; ".join([d.strip() for d in raw_designers if d.strip() and "more" not in d.lower() and "+" not in d]) if raw_designers else None

        # 6. Artist (Filtered and Semicolon Separated)
        raw_artists = response.xpath('//li[.//h4//strong[text()="Artist"]]//popup-list//a/span//text() | //li[.//h4//strong[text()="Artist"]]//popup-list//span[@itemprop="name"]//text()').getall()
        artist = "; ".join([a.strip() for a in raw_artists if a.strip() and "more" not in a.lower() and "+" not in a]) if raw_artists else None

        # 7. Publisher (Filtered and Semicolon Separated)
        raw_publishers = response.xpath('//li[.//h4//strong[text()="Publisher"]]//popup-list//a/span//text() | //li[.//h4//strong[text()="Publisher"]]//popup-list//span[@itemprop="name"]//text()').getall()
        publisher = "; ".join([p.strip() for p in raw_publishers if p.strip() and "more" not in p.lower() and "+" not in p]) if raw_publishers else None

        # 8. Description
        raw_description = response.xpath('//article[contains(@class, "game-description-body")]//text()').getall()
        # Join with a space, ensuring we don't accidentally merge words together where tags used to be
        description = " ".join([text.strip() for text in raw_description if text.strip()]) if raw_description else None

        yield {
            'url': response.url, 
            'players': players,
            'playtime': playtime,
            'age_requirement': age,
            'complexity_weight': complexity,
            'designer': designer,
            'artist': artist,
            'publisher': publisher,
            'description': description
        }

if __name__ == "__main__":
    process = CrawlerProcess()
    process.crawl(BGGDeepSpider)
    process.start()