"""
Download validated-timeboost-bids from S3 for 2026-02-01 through 2026-03-05 (inclusive).
Files are saved to data/case_study/bids/YYYY-MM-DD/ preserving the daily folder structure.

Usage:
    python fetch_bids.py
"""

import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

S3_BASE   = "s3://timeboost-auctioneer-arb1/ue2/validated-timeboost-bids"
LOCAL_BASE = Path("data/case_study/bids")

START = date(2026, 2, 1)
END   = date(2026, 3, 5)  # inclusive


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def sync_day(d: date) -> int:
    s3_path    = f"{S3_BASE}/{d.year}/{d.month:02d}/{d.day:02d}/"
    local_path = LOCAL_BASE / f"{d.isoformat()}"
    local_path.mkdir(parents=True, exist_ok=True)

    cmd = [
        "aws", "s3", "sync", s3_path, str(local_path),
        "--no-sign-request",
        "--quiet",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  [WARN] {d}: {result.stderr.strip()}")
        return 0

    n = len(list(local_path.glob("*.csv.gzip")))
    return n


def main():
    days = list(date_range(START, END))
    print(f"Fetching {len(days)} days ({START} to {END}) into {LOCAL_BASE}/")
    total_files = 0
    for d in days:
        n = sync_day(d)
        total_files += n
        print(f"  {d}  {n:3d} files")
    print(f"\nDone. {total_files} files total.")


if __name__ == "__main__":
    main()
