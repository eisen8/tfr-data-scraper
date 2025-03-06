from datetime import datetime
import time


def estimate_time_remaining(start_time, rows_processed, total_rows, guess_per_row_time) -> str:
    """ Estimates the remaining time to process all rows based on current progress.

    Args:
        start_time (float): Timestamp when processing started (from time.time())
        rows_processed (int): Number of rows that have been processed so far
        total_rows (int): Total number of rows to process
        guess_per_row_time (float): Initial estimate of time per row in seconds

    Returns:
        str: Formatted time string in the format "Xh Ym Zs"

    Note:
        For the first 50 rows, uses the guess_per_row_time for estimation.
        After 50 rows, calculates the actual average time per row based on performance.
    """
    remaining_rows = total_rows - rows_processed

    if rows_processed <= 50:  # if we don't have much data, use the guess time
        return format_time(guess_per_row_time * remaining_rows)
    else:
        elapsed_time = time.time() - start_time
        avg_time_per_row = elapsed_time / rows_processed
        estimated_remaining_time = remaining_rows * avg_time_per_row

        return format_time(estimated_remaining_time)


def format_time(seconds) -> str:
    """
    Formats a time duration in seconds to a human-readable string.

    Args:
        seconds (float): Time duration in seconds

    Returns:
        str: Formatted time string in the format "Xh Ym Zs"
    """
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"


def get_timestamp() -> str:
    """
    Generates a formatted timestamp string for the current time.

    Returns:
        str:  Timestamp formatted as "YYYY-MM-DD_HH-MM-SS"
    """
    timestamp = time.time()
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d_%H-%M-%S")
