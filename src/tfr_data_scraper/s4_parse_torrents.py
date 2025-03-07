import os
import time

import bencodepy
from common.database import Database as DB
from common.constants import Constants as C

from common.time_helper import format_time
from common.logger import Logger as L

video_extensions = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mpeg", ".mpg", ".ogv", ".3gp"}


def _parse_torrent(file_path: str) -> (str, list[str]):
    """
    Parses the .torrent file for the filenames.
    :param file_path: The file path to the torrent.
    :return: A tuple of the torrent name and its list of filenames
    """

    # Open and decode torrent file
    with open(file_path, "rb") as f:
        torrent_data = bencodepy.decode(f.read())

    info = torrent_data[b'info']
    torrent_name = info[b'name'].decode()

    file_list = []

    if b'files' in info:  # Multi-file torrent
        for file in info[b'files']:
            file_path = "/".join(part.decode() for part in file[b'path'])
            if file_path.lower().endswith(tuple(video_extensions)):
                file_list.append(file_path)
    else:  # Single-file torrent
        if torrent_name.lower().endswith(tuple(video_extensions)):
            file_list.append(torrent_name)

    return torrent_name, file_list


if __name__ == "__main__":
    #  DevNotes: Some errors are expected here. Some files will be corrupted or missing metadata (fault of the site).

    # -- SCRIPT --
    torrent_files_processed = 0
    subfiles_added = 0
    fail_messages = []

    start_time = time.time()
    L.info(f"Getting torrent files from {C.TORRENT_FOLDER_PATH}")
    for path, folders, torr_files in os.walk(C.TORRENT_FOLDER_PATH):
        for torr_file in torr_files:
            if torr_file.endswith('.torrent'):
                try:
                    # Get id
                    id = DB.get_id_by_torrent_name(torr_file)
                    if id is None:
                        L.error(f"Could not find torrent {torr_file} in DB")
                        continue

                    # Parse file names from torrent file
                    name, files = _parse_torrent(str(os.path.join(path, torr_file)))
                    L.info(f"files: {",".join(files)}")

                    # Add file names to DB
                    DB.set_file_names(id, files)
                    subfiles_added += len(files)
                    torrent_files_processed += 1
                except Exception as e:
                    L.error(f"Exception for {torr_file}", e)

    # Summary
    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time() - start_time)}")
    L.info(f"Results: ")
    L.info(f"{torrent_files_processed} Torrent files Processed.")
    L.info(f"{subfiles_added} Subfiles Added.")
    L.info(f'{L.num_errors} errors occurred')
    L.print_error_messages()
