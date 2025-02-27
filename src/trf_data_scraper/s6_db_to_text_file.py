import os
import random
from common.database import Database as DB
from common.constants import Constants as C
from common.print_helper import tprint


def contains_non_ascii_characters(string):
    non_ascii = [char for char in string if ord(char) > 127]
    return len(non_ascii) > 0


if __name__ == "__main__":
    # Config
    remove_non_ascii_files = True
    training_group = "T"
    output_file_name = "Training_Data.txt"
    output_path = C.DATA_FOLDER_PATH / output_file_name
    max_files_per_torrent = 10
    min_file_name_size = 15

    # Script
    tprint(f"Training group: {training_group}")
    DB.open_db()
    rows = DB.get_file_names(training_group)
    combined = []
    for index, row in enumerate(rows):
        # Extract only filenames from paths
        filenames = [os.path.basename(f) for f in row["file_names"].split("\n")]

        # Remove extensions
        filenames = [os.path.splitext(f)[0].strip() for f in filenames]

        # Ignore filenames less than min_file_name_size chars, to avoid files like Sample.mkv that don't have enough data to be annotated.
        filenames = [name for name in filenames if len(name) >= min_file_name_size]

        # Remove files with non-ascii character
        if remove_non_ascii_files:
            filenames = [name for name in filenames if contains_non_ascii_characters(name) is False]

        # only take up to max_files_per_torrent filenames per group
        if len(filenames) > max_files_per_torrent:
            filenames = random.sample(filenames, 10)

        combined.extend(filenames)

    tprint(f"Writing to file {output_path}")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(combined))  # Newline separated

    DB.close_db()
    tprint(f"\n\n ---- Script has finished. ----")
    tprint(f"Results: ")
    tprint(f"{len(rows)} Rows Processed.")
    tprint(f"{len(combined)} File Names added")
