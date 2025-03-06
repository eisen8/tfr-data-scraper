import os
import re
import subprocess
import time
import random
import traceback
from urllib.parse import urljoin

from dotenv import load_dotenv

from common.database import Database as DB
from common.constants import Constants as C
from common.time_helper import estimate_time_remaining, format_time
from common.logger import Logger as L

def _get_torrent(h, source, output_dir):
    # Create output dir
    os.makedirs(output_dir, exist_ok=True)

    url = urljoin(source, f"{h}.torrent")
    output_path = output_dir / f"{h}.torrent"
    if os.path.exists(output_path):
        L.info(f"Torrent file {output_path} already exists")
        os.remove(output_path)  # For now lets delete the file and redownload it
        # raise Exception(f"Torrent file {output_path} already exists")

    L.info(f"Downloading: {url}")

    # PowerShell command to use Invoke-WebRequest
    powershell_cmd = f"powershell -Command \"try {{ Invoke-WebRequest -Uri '{url}' -OutFile '{output_path}' -TimeoutSec 10 }} catch {{ Write-Host 'ERROR: ' + $_.Exception.Message; exit 1 }}\""

    result = subprocess.run(powershell_cmd, shell=True, capture_output=True, text=True)

    # Check for failure
    if result.returncode != 0:
        message = result.stderr or result.stdout
        L.error(f"Download failed: {message}")
        raise Exception(f"Unable to download torrent file - {message}")

    # Ensure file was actually created
    if os.path.exists(output_path):
        L.info("File was created successfully.")
    else:
        L.error("File was not created.")
        raise Exception(f"Torrent file {output_path} was not created")
    L.info("Download successful")

    return output_path


def _extract_magnet_hash(magnet_link):
    match = re.search(r"btih:([A-Fa-f0-9]{40}|[A-Fa-f0-9]{32})", magnet_link)
    return match.group(1).upper() if match else None  # Normalize to uppercase


if __name__ == "__main__":
    # Config
    load_dotenv()
    base_site = os.getenv("DEMAGNETIZE_BASE_SITE")
    if base_site is None:
        raise Exception("DEMAGNETIZE_BASE_SITE. Make sure to create a .env with DEMAGNETIZE_BASE_SITE and update demagnetize script for that site")

    shuffle = True  # Shuffle the rows before processing.
    max_fails = 3  # The maximum number of fails before stopping. Fails include network issues, page not found, and no links found.
    sleep_time_seconds = 10  # Sleep time between processing subsequent pages.
    sleep_time_jiggle = 5  # Jiggle time + and -. The actual sleep time will be randomly between sleep_time_seconds + or - this time.

    # Script
    DB.open_db()
    rows = DB.get_magnet_links_without_torrent()
    if shuffle:
        random.shuffle(rows)
    L.info(f'Found {len(rows)} magnet links to process')
    fail_messages = []
    total_demagnetized = 0
    start_time = time.time()
    for i, row in enumerate(rows):
        magnet_link = row['magnet_link']
        tor_hash = _extract_magnet_hash(magnet_link)
        try:
            torrent_file_path = _get_torrent(tor_hash, base_site, C.TORRENT_FOLDER_PATH)
            L.info(f'Extracted {torrent_file_path.name} from {tor_hash}')
            DB.set_torrent(row['id'], tor_hash, torrent_file_path.name)
            total_demagnetized += 1
        except Exception as e:
            L.error(f"Exception for Hash {tor_hash}", e)
            fail_messages.append(f"Exception for {tor_hash} - {e}\n{traceback.format_exc()}")

        L.info(f"Finished processing torrent {i} of {len(rows)}.")
        L.info(f"Estimated time remaining: {estimate_time_remaining(start_time, i, len(rows), sleep_time_seconds + 3)}")
        L.info("----------------------")
        time.sleep(random.uniform(sleep_time_seconds - sleep_time_jiggle, sleep_time_seconds + sleep_time_jiggle))

    DB.close_db()
    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time() - start_time)}")
    L.info(f"Results: ")
    L.info(f"{total_demagnetized} Torrent Demagnetized")
    L.info(f'{len(fail_messages)} errors occurred')
    for i, m in enumerate(fail_messages):
        L.error(f'Error {i + 1} - {m}')
