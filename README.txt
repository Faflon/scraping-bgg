# BoardGameGeek Data Engineering Pipeline

## Project Overview
This project is an end-to-end data extraction and integration pipeline designed to compile a comprehensive dataset of board game metadata and historical award records. 

Due to strict Bot Management systems (Cloudflare WAF) and dynamic frontend rendering (Angular) on the primary source target (BoardGameGeek), standard synchronous scraping methods were insufficient. This project implements a **Hybrid Scraping Architecture**, combining asynchronous request handling with automated headless browsing, supplemented by standard HTML parsing for secondary unprotected sources.

## System Architecture
The pipeline is divided into three distinct extraction phases and one integration phase:

1. **Phase 1: Target Acquisition (Selenium + BeautifulSoup)**
   * Extracts the top 1000 ranked board game URLs from the heavily gated BGG index.
   * Handles GDPR/Cookie consent overlays to expose the underlying DOM.
2. **Phase 2: Deep Metadata Extraction (Scrapy + Selenium Middleware)**
   * Custom `SeleniumMiddleware` injected into the Scrapy engine to bypass Cloudflare TLS fingerprinting.
   * Utilizes an automatic session-reboot mechanism every 75 requests to prevent DOM bloat and memory leaks.
   * Leverages `WebDriverWait` to ensure Angular JavaScript elements are fully populated before HTML extraction.
3. **Phase 3: Award Enrichment (Requests + BeautifulSoup)**
   * A synchronous, lightweight scraper targeting Wikipedia to extract the historical list of *Spiel des Jahres* (Game of the Year) winners.
   * Implements inline Regex text cleaning to strip citation brackets and original German titles for cleaner downstream integration.
4. **Phase 4: Data Integration (Pandas)**
   * Performs Entity Resolution via string normalization (punctuation removal, lowercase formatting) to ensure high-fidelity matching between BGG game titles and Wikipedia award records.
   * Executes a Left Join to append award data without dropping un-awarded baseline games.

## Repository Structure
* `1_url_spider.py` - Generates `raw_urls.csv`.
* `2_dynamic_scraper.py` - Scrapy spider utilizing custom Selenium Middleware; generates `metadata.csv`.
* `3_requests_scraper.py` - Synchronous scraper for Wikipedia; generates `awards.csv`.
* `4_data_merging.py` - Pandas integration script; generates the final `final_dataset.csv`.

## Ethical Scraping Protocol
This pipeline adheres strictly to ethical data extraction standards:
* **Rate Limiting:** Hardcoded `time.sleep(5)` delays ensure the server is never overwhelmed.
* **No Authentication:** Only publicly available, non-user-specific data is accessed.
* **Factual Data:** Extracts only uncopyrightable factual metadata (player counts, designer names, play times).