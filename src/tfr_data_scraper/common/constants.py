from pathlib import Path
from typing import Dict


class Constants:
    """
    Class containing Constants.
    """

    BASE_DIR_PATH = Path(__file__).parent.parent.resolve()  # Project directory path /tfr-data-scraper/src/
    DATA_FOLDER_PATH = (BASE_DIR_PATH / "data").resolve()  # Data folder path:  <project dir>/data/
    TORRENT_FOLDER_PATH = (DATA_FOLDER_PATH / "torrent").resolve()  # Torrent folder path:  <project dir>/data/torrent/
    DB_FILE_PATH = (DATA_FOLDER_PATH / "database.db").resolve()  # <project dir>/data/database.db

    @staticmethod
    def get_headers(referrer: str) -> dict[str, str]:
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Referer": referrer,  # Helps with anti-bot detection
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "DNT": "1",  # Optional (Do Not Track)
            "Sec-GPC": "1"  # Optional (General Privacy Control)
        }
