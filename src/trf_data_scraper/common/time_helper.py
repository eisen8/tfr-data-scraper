import time


def estimate_time_remaining(start_time, rows_processed, total_rows, guess_per_row_time):
    remaining_rows = total_rows - rows_processed

    if rows_processed <= 50:  # if we don't have much data, use the guess time
        return format_time(guess_per_row_time * remaining_rows)
    else:
        elapsed_time = time.time() - start_time
        avg_time_per_row = elapsed_time / rows_processed
        estimated_remaining_time = remaining_rows * avg_time_per_row

        return format_time(estimated_remaining_time)


def format_time(seconds):
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours}h {minutes}m {seconds}s"
