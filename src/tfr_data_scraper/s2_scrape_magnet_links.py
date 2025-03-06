import os
import traceback
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests
import time
import random
import zstandard as zstd  # Download this library so requests will use zstd decompression
from dotenv import load_dotenv

from common.database import Database as DB
import re

from common.time_helper import format_time, estimate_time_remaining
from common.logger import Logger as L

if __name__ == "__main__":
    # Config
    load_dotenv()
    base_site = os.getenv("SCRAPE_BASE_SITE")
    if base_site is None:
        raise Exception("SCRAPE_BASE_SITE. Make sure to create a .env with SCRAPE_BASE_SITE set to the base site and update scraping script for that site")

    shuffle = True  # Shuffle the rows before processing.
    max_fails = 3  # The maximum number of fails before stopping. Fails include network issues, page not found, and no links found.
    sleep_time_seconds = 10  # Sleep time between processing subsequent pages.
    sleep_time_jiggle = 3  # Jiggle time + and -. The actual sleep time will be randomly between sleep_time_seconds + or - this time.

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
    session = requests.Session()
    session.headers.update(headers)

    DB.open_db()
    hrefs = DB.get_hrefs_without_magnet_links()
    if shuffle:
        random.shuffle(hrefs)
    L.info(f'Found {len(hrefs)} hrefs to process')

    fail_messages = []
    total_links = 0
    start_time = time.time()
    for i, href in enumerate(hrefs):
        try:
            url = urljoin(base_site, href)
            L.info(f"Processing Url {url}")
            page_to_scrape = session.get(url)

            L.info(f"Status code: {page_to_scrape.status_code}")
            if page_to_scrape.status_code != 200:
                L.error(f"Page Status Code: {page_to_scrape.status_code}")
                fail_messages.append(f'Unsuccessful http request. Status code: {page_to_scrape.status_code}')
            else:
                soup = BeautifulSoup(page_to_scrape.text, 'html.parser')

                # Extract the magnet link
                match = re.search(r"\"(magnet:\S+)\"", page_to_scrape.text)
                if match and match.group(1):
                    magnet_link = match.group(1)
                    L.info(magnet_link)
                    DB.update_href_with_magnet(href, magnet_link)
                    total_links += 1
                else:
                    magnet_link = "Not Found"
                    L.error("Magnet URL not found.")
                    fail_messages.append(f'No magnet url found on page {url}')
        except Exception as e:
            L.error(f"Exception for {url}", e)
            fail_messages.append(f"Exception for {url} - {e}\n{traceback.format_exc()}")

        L.info(f"Finished processing url {i} of {len(hrefs)}")
        L.info(f"Estimated time remaining: {estimate_time_remaining(start_time, i, len(hrefs), sleep_time_seconds+3)}")
        L.info("----------------------")

        if len(fail_messages) >= max_fails:
            L.info(f"Failed {len(fail_messages)} times. Stopping the scrape")
            break
        else:
            time.sleep(random.uniform(sleep_time_seconds - sleep_time_jiggle, sleep_time_seconds + sleep_time_jiggle))

    DB.close_db()
    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time()-start_time)}")
    L.info(f"Results: ")
    L.info(f"{total_links} Links added to DB.")
    L.info(f'{len(fail_messages)} errors occurred')
    for i, m in enumerate(fail_messages):
        L.error(f'Error {i+1} - {m}')
