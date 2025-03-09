import os
import sqlite3
from common.constants import Constants as C


class Database:
    """
    A facade class for interacting with the Database.
    """

    @staticmethod
    def _connect():
        conn = sqlite3.connect(C.DB_FILE_PATH)
        conn.row_factory = sqlite3.Row  # Treat rows as dictionaries rather than tuples
        return conn

    @staticmethod
    def _close(conn):
        if conn:
            conn.commit()
            conn.close()

    @staticmethod
    def create_db():
        """ Creates the DB if not already created
        :return: None
        """
        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            # id: a unique id for each row / torrent file
            # href: an href scraped from a search page (s1) that links to a more specific page with a torrent magnet link
            # magnet_link: the magnet link scraped (s2)
            # torrent_hash: the torrent hash from that magnet link. Can be used to generate torrent file.
            # torrent_file: the filename and path indicating that the magnetic link has been processed (s3)
            # file_names: The filenames and paths (newline separated) from the torrent file (s4)
            # training_group: The training group (T for training or E for evaluating) assigned to the torrent (s5)
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                href TEXT UNIQUE,
                magnet_link TEXT,
                torrent_hash TEXT,
                torrent_file TEXT,
                file_names TEXT,
                training_group CHAR(1)
            )
            """)

            # filename: The filename
            # annotation_json: Annotations/labels for the given filename
            # annotation_json_indiced: annotation_json with start and end indices added for each label.
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS annotations (
                filename TEXT UNIQUE,
                annotation_json TEXT,
                annotation_json_indiced TEXT
            )
            """)

            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def bulk_insert_hrefs(hrefs: list[str]) -> int:
        """ Bulk insert of href links as part of S1. Only inserts hrefs that are unique.
        :param hrefs: An array of hrefs scraped from a search page that links to a more specific page with a torrent magnet link.
        :return: he number of actual items inserted.
        """

        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            hrefs = [(href,) for href in hrefs]  # ExecuteMany expects a list of tuples
            cursor.executemany("INSERT OR IGNORE INTO links (href) VALUES (?)", hrefs)
            conn.commit()
            return cursor.rowcount
        finally:
            Database._close(conn)

    @staticmethod
    def get_hrefs_without_magnet_links() -> list[str]:
        """ Retrieves all hrefs that do not have magnetic links.
        :return: A list of hrefs to be processed by S2.
        """

        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT href FROM links WHERE href IS NOT NULL and magnet_link IS NULL")
            hrefs = [row[0] for row in cursor.fetchall()]  # fetchall returns a tuple, convert to list
            return hrefs
        finally:
            Database._close(conn)

    @staticmethod
    def update_href_with_magnet_link(href: str, magnet_link: str) -> None:
        """ Updates a given href with a magnet_link
        :param href: The href link of the page.
        :param magnet_link: The magnet link scraped from the page.
        :return: None
        """

        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            cursor.execute("UPDATE links SET magnet_link = ? WHERE href = ?", (magnet_link, href))
            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def get_magnet_links_without_torrent() -> list[sqlite3.Row]:
        """ Retrieves all magnet links that don't have associated torrent files yet.
        :return: A list of rows (id, magnet_link) for processing.
        """

        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT id, magnet_link FROM links WHERE magnet_link IS NOT NULL and torrent_file IS NULL")
            return cursor.fetchall()
        finally:
            Database._close(conn)

    @staticmethod
    def set_torrent(id: int, tor_hash: str, torrent_file_name: str) -> None:
        """ Updates a record with torrent hash and file information.
        :param id: The database record ID.
        :param tor_hash: The hash of the torrent.
        :param torrent_file_name: The filename of the saved torrent file.
        :return: None
        """

        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            cursor.execute("UPDATE links SET torrent_hash = ?, torrent_file = ? WHERE id = ?", (tor_hash, torrent_file_name, id))
            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def get_torrents_without_files() -> list[sqlite3.Row]:
        """ Retrieves the ids and torrent hash of torrents without files
        :return: A list of rows (id, torrent_file) for processing.
        """

        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            cursor.execute("SELECT id, torrent_file FROM links WHERE torrent_file IS NOT NULL and file_names IS NULL")
            return cursor.fetchall()
        finally:
            Database._close(conn)

    @staticmethod
    def set_file_names(id: int, file_names: list[str]) -> None:
        """ Updates a record with the list of file names from a torrent.
        :param id: The database record ID.
        :param file_names: List of file names/paths from the torrent.
        :return: None
        """

        conn = None
        try:
            conn = Database._connect()
            cursor = conn.cursor()
            file_name_string = "\n".join(file_names)
            cursor.execute("UPDATE links SET file_names = ? WHERE id = ?", (file_name_string, id))
            conn.commit()
        finally:
            Database._close(conn)