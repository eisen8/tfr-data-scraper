import os
import time
import traceback

import bencodepy
from common.database import Database as DB
from common.constants import Constants as C

from common.time_helper import format_time
from common.logger import Logger as L

video_extensions = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mpeg", ".mpg", ".ogv", ".3gp"}


def _parse_torrent(file_path):
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

    torrent_files_processed = 0
    subfiles_added = 0
    fail_messages = []

    start_time = time.time()
    DB.open_db()
    for path, folders, torr_files in os.walk(C.TORRENT_FOLDER_PATH):
        for torr_file in torr_files:
            if torr_file.endswith('.torrent'):
                try:
                    id = DB.get_id_by_torrent_name(torr_file)
                    if id is None:
                        fail_messages.append(f"Could not find torrent {torr_file} in DB")
                        continue

                    name, files = _parse_torrent(os.path.join(path, torr_file))
                    L.info(f"files: {",".join(files)}")
                    DB.set_file_names(id, files)

                    subfiles_added += len(files)
                    torrent_files_processed += 1
                except Exception as e:
                    L.error(f"Exception for {torr_file}", e)
                    fail_messages.append(f"Exception for {torr_file} - {e}\n{traceback.format_exc()}")
    L.info(f"---- Script has finished. ----")
    L.info(f"Run time: {format_time(time.time() - start_time)}")
    L.info(f"Results: ")
    L.info(f"{torrent_files_processed} Torrent files Processed.")
    L.info(f"{subfiles_added} Subfiles Added.")
    L.info(f'{len(fail_messages)} errors occurred')
    for i, m in enumerate(fail_messages):
        L.error(f'Error {i+1} - {m}')
