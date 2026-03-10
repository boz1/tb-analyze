"""
For each day folder in data/case_study/bids/:
  1. Read all .csv.gzip files
  2. Concatenate into a single DataFrame
  3. Save as bids_YYYY-MM-DD.csv in the day folder
  4. Delete the individual .csv.gzip files
  5. Delete the (now empty) day folder and move the merged CSV up one level

Final structure: data/case_study/bids/bids_YYYY-MM-DD.csv
"""

import gzip
import glob
import shutil
from pathlib import Path

import pandas as pd

BIDS_DIR = Path("data/case_study/bids")


def process_day(day_dir: Path) -> int:
    files = sorted(day_dir.glob("*.csv.gzip"))
    if not files:
        print(f"  {day_dir.name}  no files, skipping")
        day_dir.rmdir()
        return 0

    frames = []
    for f in files:
        with gzip.open(f, "rt") as fh:
            frames.append(pd.read_csv(fh))
    merged = pd.concat(frames, ignore_index=True)

    out = BIDS_DIR / f"bids_{day_dir.name}.csv"
    merged.to_csv(out, index=False)

    # Delete individual zip files and the now-empty folder
    for f in files:
        f.unlink()
    day_dir.rmdir()

    return len(merged)


def main():
    day_dirs = sorted(d for d in BIDS_DIR.iterdir() if d.is_dir())
    print(f"Processing {len(day_dirs)} day folders...")
    total_rows = 0
    for day_dir in day_dirs:
        n = process_day(day_dir)
        total_rows += n
        print(f"  {day_dir.name}  {n:>7,} rows")
    print(f"\nDone. {total_rows:,} total rows across {len(day_dirs)} files.")


if __name__ == "__main__":
    main()
