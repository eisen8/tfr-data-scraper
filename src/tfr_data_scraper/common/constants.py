from pathlib import Path


class Constants:
    """
    Class containing Constants.
    """

    BASE_DIR_PATH = Path(__file__).parent.parent.resolve()  # Project directory path /tfr-data-scraper/src/
    DATA_FOLDER_PATH = (BASE_DIR_PATH / "data").resolve()  # Data folder path:  <project dir>/data/
    RESPONSES_FOLDER_PATH = (DATA_FOLDER_PATH / "responses").resolve()  # Torrent folder path:  <project dir>/data/annotations/
    TORRENT_FOLDER_PATH = (DATA_FOLDER_PATH / "torrent").resolve()  # Torrent folder path:  <project dir>/data/torrent/
    DB_FILE_PATH = (DATA_FOLDER_PATH / "database.db").resolve()  # <project dir>/data/database.db
