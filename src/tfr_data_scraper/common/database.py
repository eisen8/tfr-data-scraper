import os
import sqlite3
from common.constants import Constants as C


class Database:
    """
    A facade class for interacting with the Database.
    """

    @staticmethod
    def _connect():
        conn = sqlite3.connect(db_path)
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

        Returns:
            None
        """
        conn = None
        try:
            conn = Database._connect()
            # id: a unique id for each row / torrent file
            # href: an href scraped from a search page (s1) that links to a more specific page with a torrent magnet link
            # magnet_link: the magnet link scraped (s2)
            # torrent_hash: the torrent hash from that magnet link. Can be used to generate torrent file.
            # internal_hash: an internal hash generated from the torrent_hash. Is used to ensure uniqueness and cannot be used to generate torrent file.
            # torrent_file: the filename and path indicating that the magnetic link has been processed (s3)
            # file_names: The filenames and paths (newline separated) from the torrent file (s4)
            # training_group: The training group (T for training or E for evaluating) assigned to the torrent (s5)
            conn.cursor().execute("""
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
            # annotation_kson_indiced: annotation_json with start and end indices added for each label.
            conn.cursor().execute("""
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

        Args:
            hrefs (list[str]): An array of hrefs scraped from a search page that links to a more specific page with a torrent magnet link.

        Returns:
            int: The number of actual items inserted.
        """

        conn = None
        try:
            conn = Database._connect()
            hrefs = [(href,) for href in hrefs]  # ExecuteMany expects a list of tuples
            conn.cursor().executemany("INSERT OR IGNORE INTO links (href) VALUES (?)", hrefs)
            conn.commit()
            return conn.cursor().rowcount
        finally:
            Database._close(conn)

    @staticmethod
    def get_hrefs_without_magnet_links() -> list[str]:
        """ Retrieves all hrefs that do not have magnetic links.

        Returns:
            list[str]: A list of hrefs to be processed by S2.
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("SELECT href FROM links WHERE href IS NOT NULL and magnet_link IS NULL")
            hrefs = [row[0] for row in conn.cursor().fetchall()]  # fetchall returns a tuple, convert to list
            return hrefs
        finally:
            Database._close(conn)

    @staticmethod
    def update_href_with_magnet_link(href: str, magnet_link: str) -> None:
        """ Updates a given href with a magnet_link

        Args:
            href (str): The href link of the page.
            magnet_link (str): The magnet link scraped from the page.

        Returns:
            None
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("UPDATE links SET magnet_link = ? WHERE href = ?", (magnet_link, href))
            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def get_magnet_links_without_torrent() -> list[sqlite3.Row]:
        """ Retrieves all magnet links that don't have associated torrent files yet.

        Returns:
            list[sqlite3.Row]: A list of rows containing id and magnet_link for processing.
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("SELECT id, magnet_link FROM links WHERE magnet_link IS NOT NULL and torrent_file IS NULL")
            return conn.cursor().fetchall()
        finally:
            Database._close(conn)

    @staticmethod
    def set_torrent(id: int, tor_hash: str, torrent_file_name: str) -> None:
        """ Updates a record with torrent hash and file information.

        Args:
            id (int): The database record ID.
            tor_hash (str): The hash of the torrent.
            torrent_file_name (str): The filename of the saved torrent file.

        Returns:
            None
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("UPDATE links SET torrent_hash = ?, torrent_file = ? WHERE id = ?", (tor_hash, torrent_file_name, id))
            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def get_rows_with_file_names() -> list[sqlite3.Row]:
        """ Retrieves all rows that have file names but no training group assigned.

        Returns:
            list[sqlite3.Row]: A list of rows containing ids that need training group assignment.
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("SELECT id FROM links WHERE file_names IS NOT NULL and training_group IS NULL")
            return conn.cursor().fetchall()
        finally:
            Database._close(conn)

    @staticmethod
    def set_training_group(id: int, training_group: str) -> None:
        """ Assigns a training group to a specific record.

        Args:
            id (int): The database record ID.
            training_group (str): The training group to assign ('T' for training or 'E' for evaluating).

        Returns:
            None
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("UPDATE links SET training_group = ? WHERE id = ?", (training_group, id))
            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def get_id_by_torrent_name(torrent_file: str) -> int:
        """ Retrieves the database ID for a given torrent filename.

        Args:
            torrent_file (str): The torrent filename to look up.

        Returns:
            int: The id if found or -1 if not found.
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("SELECT id FROM links WHERE torrent_file = ?", (torrent_file,))
            first_row = conn.cursor().fetchone()
            if first_row:
                return first_row['id']
            else:
                return -1
        finally:
            Database._close(conn)

    @staticmethod
    def set_file_names(id: int, file_names: list[str]) -> None:
        """ Updates a record with the list of file names from a torrent.

        Args:
            id (int): The database record ID.
            file_names (list[str]): List of file names/paths from the torrent.

        Returns:
            None
        """

        conn = None
        try:
            conn = Database._connect()
            file_name_string = "\n".join(file_names)
            conn.cursor().execute("UPDATE links SET file_names = ? WHERE id = ?", (file_name_string, id))
            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def get_file_names(training_group: str) -> list[sqlite3.Row]:
        """ Retrieves file names for a specific training group.

        Args:
            training_group (str): The training group to retrieve files for ('T' or 'E').

        Returns:
            list[sqlite3.Row]: A list of rows containing file_names for the specified training group.
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("SELECT file_names FROM links WHERE training_group = ? and file_names IS NOT NULL", (training_group,))
            return conn.cursor().fetchall()
        finally:
            Database._close(conn)

    @staticmethod
    def get_files_to_annotate(n: int) -> list[str]:
        """ Retrieves a limited number of files that need annotation.

        Args:
            n (int): The maximum number of files to retrieve.

        Returns:
            list[str]: A list of filenames that need annotation.
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("SELECT filename FROM annotations WHERE annotation_json IS NULL LIMIT ?", (n,))
            rows = [row[0] for row in conn.cursor().fetchall()]
            return rows
        finally:
            Database._close(conn)

    @staticmethod
    def get_count_of_files_to_annotate() -> int:
        """ Gets the count of files that still need annotation.

        Returns:
            int: The number of files that need annotation.
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("SELECT COUNT(filename) FROM annotations WHERE annotation_json IS NULL")
            count = conn.cursor().fetchone()[0]
            return count
        finally:
            Database._close(conn)

    @staticmethod
    def bulk_insert_files_to_annotate(filenames: list[str]) -> int:
        """ Bulk inserts filenames into the annotations table for annotation.

        Args:
            filenames (list[str]): List of filenames to be annotated.

        Returns:
            int: The number of filenames successfully inserted.
        """

        conn = None
        try:
            conn = Database._connect()
            filenames = [(filename,) for filename in filenames]  # ExecuteMany expects a list of tuples
            conn.cursor().executemany("INSERT OR IGNORE INTO annotations (filename) VALUES (?)", filenames)
            conn.commit()
            return conn.cursor().rowcount
        finally:
            Database._close(conn)

    @staticmethod
    def add_annotation(filename: str, annotation: str) -> None:
        """ Adds an annotation to a specific file.

        Args:
            filename (str): The filename to annotate.
            annotation (str): The annotation JSON string.

        Returns:
            None
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("UPDATE annotations SET annotation_json = ? WHERE filename = ?", (annotation, filename))
            conn.commit()
        finally:
            Database._close(conn)

    @staticmethod
    def clear_all_annotations() -> None:
        """ Clears all annotations from the database.

        Returns:
            None
        """

        conn = None
        try:
            conn = Database._connect()
            conn.cursor().execute("UPDATE annotations SET annotation_json = NULL WHERE annotation_json IS NOT NULL")
            conn.commit()
        finally:
            Database._close(conn)
