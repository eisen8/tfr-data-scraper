import os
import sqlite3
from common.constants import Constants as C


class Database:
    _cursor = None
    _conn = None

    @staticmethod
    def open_db():
        Database._conn = sqlite3.connect(str(C.DB_FILE_PATH))
        Database._conn.row_factory = sqlite3.Row  # Treat rows as dictionaries rather than tuples
        Database._cursor = Database._conn.cursor()

        # id: a unique id for each row / torrent file
        # href: an href scraped from a search page (s1) that links to a more specific page with a torrent magnet link
        # magnet_link: the magnet link scraped (s2)
        # torrent_hash: the torrent hash from that magnet link. Can be used to generate torrent file.
        # internal_hash: an internal hash generated from the torrent_hash. Is used to ensure uniqueness and cannot be used to generate torrent file.
        # torrent_file: the filename and path indicating that the magnetic link has been processed (s3)
        # file_names: The filenames and paths (newline separated) from the torrent file (s4)
        # training_group: The training group (T for training or E for evaluating) assigned to the torrent (s5)

        Database._cursor.execute("""
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

        Database._cursor.execute("""
        CREATE TABLE IF NOT EXISTS annotations (
            filename TEXT UNIQUE,
            annotation_json TEXT
        )
        """)

        Database._conn.commit()

    @staticmethod
    def close_db():
        if Database._conn:
            Database._conn.close()

    @staticmethod
    def bulk_insert_hrefs(hrefs: []) -> int:
        hrefs = [(href,) for href in hrefs]  # ExecuteMany expects a list of tuples
        Database._cursor.executemany("INSERT OR IGNORE INTO links (href) VALUES (?)", hrefs)
        Database._conn.commit()
        return Database._cursor.rowcount

    @staticmethod
    def get_hrefs_without_magnets():
        Database._cursor.execute("SELECT href FROM links WHERE href IS NOT NULL and magnet_link IS NULL")
        hrefs = Database._cursor.fetchall()
        return hrefs

    @staticmethod
    def update_href_with_magnet(href, magnet_link):
        Database._cursor.execute("UPDATE links SET magnet_link = ? WHERE href = ?", (magnet_link, href))
        Database._conn.commit()

    @staticmethod
    def get_magnet_link_without_torrent():
        Database._cursor.execute("SELECT id, magnet_link FROM links WHERE magnet_link IS NOT NULL and torrent_file IS NULL")
        return Database._cursor.fetchall()

    @staticmethod
    def set_torrent(id, tor_hash, torrent_file_name):
        Database._cursor.execute("UPDATE links SET torrent_hash = ?, torrent_file = ? WHERE id = ?", (tor_hash, torrent_file_name, id))
        Database._conn.commit()

    @staticmethod
    def get_rows_with_file_names():
        Database._cursor.execute("SELECT id FROM links WHERE file_names IS NOT NULL and training_group IS NULL")
        return Database._cursor.fetchall()

    @staticmethod
    def set_training_group(id, training_group):
        Database._cursor.execute("UPDATE links SET training_group = ? WHERE id = ?", (training_group, id))
        Database._conn.commit()

    @staticmethod
    def get_id_by_torrent_name(torrent_file):
        Database._cursor.execute("SELECT id FROM links WHERE torrent_file = ?", (torrent_file,))
        first_row = Database._cursor.fetchone()
        if first_row:
            return first_row['id']
        else:
            return None

    @staticmethod
    def set_file_names(id, file_names):
        file_name_string = "\n".join(file_names)
        Database._cursor.execute("UPDATE links SET file_names = ? WHERE id = ?", (file_name_string, id))
        Database._conn.commit()

    @staticmethod
    def get_file_names(training_group):
        Database._cursor.execute("SELECT file_names FROM links WHERE training_group = ? and file_names IS NOT NULL", (training_group,))
        return Database._cursor.fetchall()

    @staticmethod
    def get_files_to_annotate(n):
        Database._cursor.execute("SELECT filename FROM annotations WHERE annotation_json IS NULL LIMIT ?", (n,))
        rows = [row[0] for row in Database._cursor.fetchall()]
        return rows

    @staticmethod
    def get_count_of_files_to_annotate():
        Database._cursor.execute("SELECT COUNT(filename) FROM annotations WHERE annotation_json IS NULL")
        count = Database._cursor.fetchone()[0]
        return count

    @staticmethod
    def bulk_insert_files_to_annotate(filenames: []) -> int:
        filenames = [(filename,) for filename in filenames]  # ExecuteMany expects a list of tuples
        Database._cursor.executemany("INSERT OR IGNORE INTO annotations (filename) VALUES (?)", filenames)
        Database._conn.commit()
        return Database._cursor.rowcount

    @staticmethod
    def add_annotation(filename, annotation):
        Database._cursor.execute("UPDATE annotations SET annotation_json = ? WHERE filename = ?", (annotation, filename))
        Database._conn.commit()