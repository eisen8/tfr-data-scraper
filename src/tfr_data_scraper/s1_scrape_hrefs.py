import os
import re
from typing import Tuple
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests
import time
import random
import zstandard as zstd  # Download this library so requests will use zstd decompression
from dotenv import load_dotenv

from common.database import Database as DB
from common.constants import Constants as C
from common.logger import Logger as L
from common.time_helper import format_time, estimate_time_remaining


def _update_url_page_number(page_url: str) -> (str, int):
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


def _scrape_page_for_hrefs(page_html: str) -> list[str]:
    soup = BeautifulSoup(page_html, 'html.parser')

    # Check if page is valid
    detail_box = soup.find('div', class_='box-info-detail')
    if detail_box and detail_box.find('p'):
        # Extract the message inside <p> tag
        message = detail_box.find('p').get_text(strip=True)
        if message:
            L.info(message)  # Message here means the page is not valid (i.e. zero results, invalid search, etc.)
            L.info("Finished Scrapping")
            return []

    # Scraping code to get the hrefs on the search page
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

    return hrefs


if __name__ == "__main__":
    # -- CONFIG --
    load_dotenv()
    base_site = os.getenv("SCRAPE_BASE_SITE")
    if base_site is None:
        raise Exception("SCRAPE_BASE_SITE. Make sure to create a .env with SCRAPE_BASE_SITE set to the base site and update scraping script for that site")

    # Initial Search URL. If it ends in a digit (i.e. /1/) it will be treated as multiple pages to scrape and this digit will be updated.
    # examples: category-search/mysearch/Anime/1/ /top-100-anime
    initial_search_url = urljoin(base_site, "category-search/mysearch/Anime/1/")
    max_pages = 50  # The maximum number of pages to scrape
    max_fails = 3  # The maximum number of fails before stopping. Fails include network issues, page not found, and no links found.
    min_seeds = 1  # minimum number of seeds to be considered valid
    sleep_time_seconds = 15  # Sleep time between processing subsequent pages.
    sleep_time_jiggle = 5  # Jiggle time + and -. The actual sleep time will be randomly between sleep_time_seconds + or - this time.

    # -- SCRIPT --
    DB.create_db()
    session = requests.Session()
    session.headers.update(C.get_headers(base_site))

    pages_processed = 0
    current_page_num = 0
    url = initial_search_url
    total_hrefs_added = 0

    # Scraping loop
    start_time = time.time()
    while pages_processed < max_pages and current_page_num != -1 and L.num_errors < max_fails:
        try:
            L.info(f"Processing Url {url}")

            # Get Page
            response = session.get(url, timeout=30)
            L.info(f"Status code: {response.status_code}")
            response.raise_for_status()  # Raise exception for 4XX/5XX responses

            # Scrape Page
            hrefs = _scrape_page_for_hrefs(response.text)

            L.info(f"Found {len(hrefs)} hrefs")
            if len(hrefs) == 0:
                L.error(f"No hrefs found on page {url}")
            else:
                total_hrefs_added += DB.bulk_insert_hrefs(hrefs) # Add to DB
        except Exception as e:
            L.error(f"Exception for {url}", e)

        # Update current page url
        L.info(f"Finished processing url {url}")
        url, current_page_num = _update_url_page_number(url)
        pages_processed += 1

        # Sleep with jiggle
        time.sleep(random.uniform(sleep_time_seconds - sleep_time_jiggle, sleep_time_seconds + sleep_time_jiggle))

    # Summary
    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time() - start_time)}")
    L.info(f"Results: ")
    L.info(f"{total_hrefs_added} Hrefs added to DB.")
    L.info(f"{pages_processed} pages successfully processed.")
    L.info(f'{L.num_errors} errors occurred:')
    L.print_error_messages()
