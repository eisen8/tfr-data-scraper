import sqlite3
import random
from common.database import Database as DB
from common.print_helper import tprint

if __name__ == "__main__":
    # Config
    training_groups = ['T', 'E']
    weights = [0.5, 0.5]  # 50% 'T', 50% 'E'

    # Script
    DB.open_db()
    rows = DB.get_rows_with_file_names()

    assigned_t = 0
    assigned_e = 0
    for row in rows:
        random_pool = random.choices(training_groups, weights=weights)[0]
        DB.set_training_group(row['id'], random_pool)

        if random_pool == 'T':
            assigned_t += 1
        else:
            assigned_e += 1

    DB.close_db()
    tprint(f"\n\n ---- Script has finished. ----")
    tprint(f"Results: ")
    tprint(f"{len(rows)} Rows Processed.")
    tprint(f"{assigned_t} assigned T and {assigned_e} assigned E")
