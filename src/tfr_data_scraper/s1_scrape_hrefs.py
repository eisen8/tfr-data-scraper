import os
import re
from typing import Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests
import time
import random
import traceback
import zstandard as zstd  # Download this library so requests will use zstd decompression
from dotenv import load_dotenv

from common.database import Database as DB
from common.logger import Logger as L
from common.time_helper import format_time, estimate_time_remaining


def _update_url_page_number(page_url) -> (str, int):
    # Regular expression to check if URL ends with a number (e.g., /1/)
    match = re.search(r'(\d+)/$', page_url)

    if match:
        page_number = int(match.group(1))  # Extract the current page number
        updated_url = urljoin(page_url[:match.start(1)], f'{page_number + 1}/')  # Update the page number to +1
    else:
        # If no match is found then treat it as a single page. Don't change the url and return -1
        updated_url = page_url
        page_number = -1

    return updated_url, page_number


def _scrape_page_for_hrefs(page_html) -> Tuple[list, bool]:
    soup = BeautifulSoup(page_html, 'html.parser')

    # Check if page is valid (specific to website)
    detail_box = soup.find('div', class_='box-info-detail')
    if detail_box and detail_box.find('p'):
        # Extract the message inside <p> tag
        message = detail_box.find('p').get_text(strip=True)
        if message:
            L.error(message)  # Message here means the page is not valid (i.e. zero results, invalid search, etc.)
            L.info("Finished Scrapping")
            return [], True

    # Scraping code to get the hrefs on the search page (specific to website)
    rows = soup.select("tbody tr")

    hrefs = []

    for row in rows:
        name_tag = row.select_one(".coll-1.name a:nth-of-type(2)")  # The second <a> tag has the torrent link
        seeds_tag = row.select_one(".coll-2.seeds")

        if name_tag and seeds_tag:
            name = name_tag.text.strip()
            href = name_tag["href"]
            seeds = int(seeds_tag.text.strip())
            if seeds >= min_seeds:
                hrefs.append(href)

    return hrefs, False


if __name__ == "__main__":
    # Config
    load_dotenv()
    base_site = os.getenv("SCRAPE_BASE_SITE")
    if base_site is None:
        raise Exception("SCRAPE_BASE_SITE. Make sure to create a .env with SCRAPE_BASE_SITE set to the base site and update scraping script for that site")

    # Initial Search URL. If it ends in a digit (i.e. /1/) it will be treated as multiple pages to scrape and this digit will be updated.
    # examples: category-search/mysearch/Anime/1/ /top-100-anime
    initial_search_url = urljoin(base_site, "/top-100-anime")
    max_pages = 50  # The maximum number of pages to scrape
    max_fails = 3  # The maximum number of fails before stopping. Fails include network issues, page not found, and no links found.
    min_seeds = 1  # minimum number of seeds to be considered valid
    sleep_time_seconds = 15  # Sleep time between processing subsequent pages.
    sleep_time_jiggle = 5  # Jiggle time + and -. The actual sleep time will be randomly between sleep_time_seconds + or - this time.

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Referer": base_site,  # Helps with anti-bot detection
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",  # Optional (Do Not Track)
        "Sec-GPC": "1"  # Optional (General Privacy Control)
    }

    # Script
    DB.create_db()
    session = requests.Session()
    session.headers.update(headers)

    fail_messages = []
    pages_processed = 0
    should_continue = True
    url = initial_search_url
    total_hrefs_added = 0

    # Scraping loop
    start_time = time.time()
    while should_continue:
        try:
            L.info(f"Processing Url {url}")
            page_to_scrape = session.get(url)
            L.info(f"Status code: {page_to_scrape.status_code}")
            if page_to_scrape.status_code != 200:
                fail_messages.append(f"Unsuccessful status code: {page_to_scrape.status_code} for {url}")
            else:
                hrefs, should_break = _scrape_page_for_hrefs(page_to_scrape.text)
                if should_break:
                    break

                tprint(f"Found {len(hrefs)} hrefs")
                if len(hrefs) == 0:
                    fail_messages.append(f"No min seed hrefs found for {url}")
                else:
                    total_hrefs_added += DB.bulk_insert_hrefs(hrefs)  # Insert into DB and return actual HREFs added (not including duplicates)
        except Exception as e:
            L.error(f"Exception for {url}", e)
            fail_messages.append(f"Exception for {url} - {e}\n{traceback.format_exc()}")
        L.info(f"Finished processing url {url}")
        url, currentPage = _update_url_page_number(initial_search_url)
        pages_processed += 1

        # Check loop ending conditions
        if len(fail_messages) >= max_fails:  # Max fails occurred
            L.info(f"Failed {len(fail_messages)} times. Stopping the scrape")
            should_continue = False
        elif pages_processed >= max_pages:  # Processed Max Pages
            L.info(f"Processed max page of {max_pages}")
            should_continue = False
        elif currentPage == -1:  # Only one page to process and finished
            should_continue = False
        else:  # Sleep with jiggle
            time.sleep(random.uniform(sleep_time_seconds - sleep_time_jiggle, sleep_time_seconds + sleep_time_jiggle))

    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time() - start_time)}")
    L.info(f"Results: ")
    L.info(f"{total_hrefs_added} Hrefs added to DB.")
    L.info(f"{pages_processed} pages successfully processed.")
    L.info(f'{len(fail_messages)} errors occurred')
    for i, m in enumerate(fail_messages):
        L.error(f'Error {i + 1} - {m}')
