import os
import re
import subprocess
import time
import random
import traceback
from urllib.parse import urljoin

from dotenv import load_dotenv
from pathlib import Path

from common.database import Database as DB
from common.constants import Constants as C
from common.time_helper import estimate_time_remaining, format_time
from common.logger import Logger as L


def _get_torrent(h: str, source: str, output_dir: Path) -> str:
    """ Downloads the torrent from the source site and saves it to the output_dir
    :param h: The hash of the torrent file
    :param source: The cache site to download the .torrent file from
    :param output_dir: The output directory to save the .torrent files to
    :return: The output path of the .torrent file
    :raise: Exception If the .torrent file was unable to be downloaded.
    """

    # Create output dir
    os.makedirs(output_dir, exist_ok=True)

    url = urljoin(source, f"{h}.torrent")
    output_path = output_dir / f"{h}.torrent"
    if os.path.exists(output_path):
        L.info(f"Torrent file {output_path} already exists")
        os.remove(output_path)  # For now lets delete the file and redownload it

    L.info(f"Downloading: {url}")

    # PowerShell command to use Invoke-WebRequest to download torrent
    powershell_cmd = f"powershell -Command \"try {{ Invoke-WebRequest -Uri '{url}' -OutFile '{output_path}' -TimeoutSec 10 }} catch {{ Write-Host 'ERROR: ' + $_.Exception.Message; exit 1 }}\""

    result = subprocess.run(powershell_cmd, shell=True, capture_output=True, text=True)

    # Check for failure
    if result.returncode != 0:
        message = result.stderr or result.stdout
        raise Exception(f"Unable to download torrent file - {message}")

    # Ensure file was actually created
    if os.path.exists(output_path):
        L.info("File was created successfully.")
    else:
        raise Exception(f"Torrent file {output_path} was not created")
    L.info("Download successful")

    return output_path


def _extract_magnet_hash(magnet_link: str) -> str | None:
    """ Extracts just the hash portion of the magnet link
    :param magnet_link: The full magnet link
    :return: The hash
    """

    match = re.search(r"btih:([A-Fa-f0-9]{40}|[A-Fa-f0-9]{32})", magnet_link)
    return match.group(1).upper() if match else None  # Normalize to uppercase


if __name__ == "__main__":
    # -- CONFIG --
    load_dotenv()
    base_site = os.getenv("DEMAGNETIZE_BASE_SITE")
    if base_site is None:
        raise Exception("DEMAGNETIZE_BASE_SITE. Make sure to create a .env with DEMAGNETIZE_BASE_SITE and update demagnetize script for that site")

    shuffle = True  # Shuffle the rows before processing.
    max_fails = 3  # The maximum number of fails before stopping. Fails include network issues, page not found, and no links found.
    sleep_time_seconds = 10  # Sleep time between processing subsequent pages.
    sleep_time_jiggle = 5  # Jiggle time + and -. The actual sleep time will be randomly between sleep_time_seconds + or - this time.

    # -- SCRIPT --
    rows = DB.get_magnet_links_without_torrent()
    if shuffle:
        random.shuffle(rows)
    L.info(f'Found {len(rows)} magnet links to process')

    total_demagnetized = 0
    start_time = time.time()
    for i, row in enumerate(rows):
        try:
            magnet_link = row['magnet_link']
            tor_hash = _extract_magnet_hash(magnet_link)
            if tor_hash:
                # Download Torrent from cache site; saving to Torrent folder path
                torrent_file_path = _get_torrent(tor_hash, base_site, C.TORRENT_FOLDER_PATH)
                L.info(f'Extracted {torrent_file_path.name} from {tor_hash}')

                # Save details in database
                DB.set_torrent(row['id'], tor_hash, torrent_file_path.name)
                total_demagnetized += 1
            else:
                raise Exception(f"Unable to extract tor_hash from {magnet_link}")
        except Exception as e:
            L.error(f"Exception for row {row}", e)

        L.info(f"Finished processing torrent {i+1} of {len(rows)}.")
        L.info(f"Estimated time remaining: {estimate_time_remaining(start_time, i+1, len(rows), sleep_time_seconds + 3)}")
        L.info("----------------------")

        if L.num_errors >= max_fails:
            L.info(f"Failed {L.num_errors} times. Stopping the scrape")
            break
        time.sleep(random.uniform(sleep_time_seconds - sleep_time_jiggle, sleep_time_seconds + sleep_time_jiggle))

    # Summary
    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time() - start_time)}")
    L.info(f"Results: ")
    L.info(f"{total_demagnetized} Torrent Demagnetized")
    L.info(f'{L.num_errors} errors occurred:')
    L.print_error_messages()
