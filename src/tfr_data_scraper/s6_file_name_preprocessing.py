import os
import random
import re
import time

from common.database import Database as DB
from common.constants import Constants as C
from common.print_helper import tprint, tprintln
from src.tfr_data_scraper.common.time_helper import format_time


def _preprocessor(s):
    # Remove websites links
    s = re.sub(r'https?:\/\/', '', s)
    s = re.sub(r'www\.([a-zA-Z0-9]+)\.([a-zA-Z0-9.\/]+)', r'', s, flags=re.IGNORECASE)  # websites with periods
    s = re.sub(r'www\s([a-zA-Z0-9]+)\s([a-zA-Z0-9.\/]+)', r'', s, flags=re.IGNORECASE)  # websites with whitespace

    # Add space between Season and Episode (i.e. S05E05 -> S05 E05) so it can be tokenized easier
    s = re.sub(r'S(\d+)E(\d+)', r'S\1 E\2', s, flags=re.IGNORECASE)
    s = re.sub(r'S(\d+)P(\d+)E(\d+)', r'S\1 P\2 E\3', s, flags=re.IGNORECASE) # season part episode
    s = re.sub(r'S(\d+)P(\d+)', r'S\1 P\2', s, flags=re.IGNORECASE) # season part
    s = re.sub(r'S(\d+)Ep(\d+)', r'S\1 E\2', s, flags=re.IGNORECASE)  # case where e is ep

    # Replaces all periods with space except if between two digits (i.e. '7.1' for sound)
    s = re.sub(r'\.(?!\d)|(?<!\d)\.', ' ', s)
    s = re.sub(r'(?<!\d\.\d)\.(?!(\d$|\d\D|\d\.\d))', ' ', s)  # cases of year/ep/resolution i.e. "1973.480p" or "E02.720p"

    # Replace _ with spaces
    s = re.sub(r'_', ' ', s)

    # Remove dashes
    s = re.sub(r'-([\[\(\]\)])', '\g<1>', s)  # dashes before brackets
    s = re.sub(r'([\[\(\]\)])-', '\g<1>', s)  # dash after brackets
    s = re.sub(r'(?<=[\w\[\]\(\)])(-)(?=[\w\[\]\(\)]*$)', ' ', s)  # dash at end such as x265-GalaxyTV or x264-mSD[eztv]

    # Add whitespace before non-prefix open brackets/parenthesis
    s = re.sub(r'(?<=[a-zA-Z0-9\]\)])[\[\(]', ' \g<0>', s)

    # Add whitespace after non-suffix closed brackets/parenthesis
    s = re.sub(r'[\]\)](?=[a-zA-Z0-9\[\(])', '\g<0> ', s)

    # We allow '-', this is just to normalize them with either no spaces or spaces on both sides.
    # Without this "x264-[yts]" would become "x264- yts". This normalizes it to "x264 - yts"
    s = re.sub(r'(?<=\S) -|-(?= \S)', ' - ', s)

    # Remove empty brackets and parenthesis
    s = re.sub(r'\(\)', ' ', s)
    s = re.sub(r'\[\]', ' ', s)

    # Replace multiple spaces or dashes with a single
    s = re.sub(r'\s+', ' ', s)
    s = re.sub(r'-+', '-', s)

    # Random fixes
    s = re.sub(r' x 26', ' x26', s, flags=re.IGNORECASE)  # some uploaders separate the x in x265
    s = re.sub(r' h 26', ' h26', s, flags=re.IGNORECASE)  # some uploaders separate the h in x265

    # Strip whitespace or dashes from the start and end of the string
    s = re.sub(r'^[\s-]+|[\s-]+$', '', s)

    return s


def _contains_non_ascii_characters(string):
    non_ascii = [char for char in string if ord(char) > 127]
    return len(non_ascii) > 0


if __name__ == "__main__":
    # Config
    remove_non_ascii_files = True
    training_group = "T"
    shuffle = True
    max_files_per_torrent = 10
    min_file_name_size = 12

    # Script
    tprint(f"Training group: {training_group}")
    DB.open_db()
    rows = DB.get_file_names(training_group)
    if shuffle:
        random.shuffle(rows)

    combined = []
    start_time = time.time()
    for index, row in enumerate(rows):
        # Extract only filenames from paths
        filenames = [os.path.basename(f) for f in row["file_names"].split("\n")]

        # Remove extensions
        filenames = [os.path.splitext(f)[0].strip() for f in filenames]

        # Ignore filenames less than min_file_name_size chars, to avoid files like Sample.mkv that don't have enough data to be annotated.
        filenames = [name for name in filenames if len(name) >= min_file_name_size]

        # Remove files with non-ascii character
        if remove_non_ascii_files:
            filenames = [name for name in filenames if _contains_non_ascii_characters(name) is False]

        # only take up to max_files_per_torrent filenames per group
        if len(filenames) > max_files_per_torrent:
            filenames = random.sample(filenames, 10)

        processed_filenames = []
        for filename in filenames:
            processed = _preprocessor(filename)
            # tprint(f"{filename} ->")
            # tprint(f"{processed}")
            processed_filenames.append(processed)

        combined.extend(processed_filenames)

    tprint(f"Writing to db")

    annotation_rows_count = DB.bulk_insert_files_to_annotate(combined)

    DB.close_db()
    tprintln()
    tprint(f"---- Script has finished. ----")
    tprint(f"Run time: {format_time(time.time() - start_time)}")
    tprint(f"Results: ")
    tprint(f"{len(rows)} Rows Processed.")
    tprint(f"{len(combined)} File Names added")
    tprint(f"{annotation_rows_count} Actual rows added to annotations database")
