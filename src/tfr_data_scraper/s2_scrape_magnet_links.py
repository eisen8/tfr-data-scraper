import os
import traceback
from urllib.parse import urljoin

from bs4 import BeautifulSoup
import requests
import time
import random
import re
import zstandard as zstd  # Download this library so requests will use zstd decompression
from dotenv import load_dotenv

from common.database import Database as DB
from common.constants import Constants as C
from common.time_helper import format_time, estimate_time_remaining
from common.logger import Logger as L

if __name__ == "__main__":
    # -- CONFIG --
    load_dotenv()
    base_site = os.getenv("SCRAPE_BASE_SITE")
    if base_site is None:
        raise Exception("SCRAPE_BASE_SITE. Make sure to create a .env with SCRAPE_BASE_SITE set to the base site and update scraping script for that site")

    shuffle = True  # Shuffle the rows before processing.
    max_fails = 3  # The maximum number of fails before stopping. Fails include network issues, page not found, and no links found.
    sleep_time_seconds = 10  # Sleep time between processing subsequent pages.
    sleep_time_jiggle = 3  # Jiggle time + and -. The actual sleep time will be randomly between sleep_time_seconds + or - this time.

    # -- SCRIPT --
    session = requests.Session()
    session.headers.update(C.get_headers(base_site))
    hrefs = DB.get_hrefs_without_magnet_links()
    if shuffle:
        random.shuffle(hrefs)

    total_links = 0
    start_time = time.time()
    L.info(f'Found {len(hrefs)} hrefs to process')
    for i, href in enumerate(hrefs):
        try:
            # Create URL
            url = urljoin(base_site, href)
            L.info(f"Processing Url {url}")

            # Get request
            response = session.get(url, timeout=30)
            L.info(f"Status code: {response.status_code}")
            response.raise_for_status()  # Raise exception for 4XX/5XX responses

            # Extract the magnet link
            soup = BeautifulSoup(response.text, 'html.parser')
            match = re.search(r"\"(magnet:\S+)\"", response.text)
            if match and match.group(1):
                magnet_link = match.group(1)
                L.info(f"magnet link: {magnet_link}")
                DB.update_href_with_magnet_link(href, magnet_link)
                total_links += 1
            else:
                L.error("Magnet URL not found.")
        except Exception as e:
            L.error(f"Exception for {url}", e)

        L.info(f"Finished processing url {i+1} of {len(hrefs)}")
        L.info(f"Estimated time remaining: {estimate_time_remaining(start_time, i+1, len(hrefs), sleep_time_seconds+3)}")
        L.info("----------------------")

        if L.num_errors >= max_fails:
            L.info(f"Failed {L.num_errors} times. Stopping the scrape")
            break

        time.sleep(random.uniform(sleep_time_seconds - sleep_time_jiggle, sleep_time_seconds + sleep_time_jiggle))

    # Summary
    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time()-start_time)}")
    L.info(f"Results: ")
    L.info(f"{total_links} Links added to DB.")
    L.info(f'{L.num_errors} errors occurred:')
    L.print_error_messages()
