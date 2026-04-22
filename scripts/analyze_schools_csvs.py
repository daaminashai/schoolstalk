#!/usr/bin/env python3
"""
Analyze CSV files under a directory (default: ./schools).

Reports:
- total_csvs: total number of .csv files found (case-insensitive)
- empty_csvs: CSVs that have a header but no data rows (blank lines ignored)
- csvs_with_emails: number of CSV files containing at least one email address anywhere

Usage:
  python3 scripts/analyze_schools_csvs.py [root_dir] [--list empty emails all]

Notes:
- "empty" means header-only: the first non-empty row is treated as the header; if there are no subsequent non-empty rows with any value, it's counted as empty. Completely blank files also count as empty.
- Email detection uses a pragmatic regex; it may not catch every esoteric address but is sufficient for typical school emails.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
from typing import Iterator


EMAIL_REGEX = re.compile(
    r"\b[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}\b"
)


def iter_csv_paths(root: str) -> Iterator[str]:
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name.lower().endswith(".csv"):
                yield os.path.join(dirpath, name)


def csv_has_data_rows(path: str) -> bool:
    """Return True if CSV has at least one data row after the header.

    The first non-empty row is treated as the header. Subsequent rows that have
    at least one non-empty cell are considered data rows. Blank lines and rows
    where all cells are empty/whitespace are ignored.
    """
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore", newline="") as f:
            reader = csv.reader(f)
            header_seen = False
            for row in reader:
                if not row or all((str(cell).strip() == "") for cell in row):
                    # Skip completely blank rows
                    continue
                if not header_seen:
                    header_seen = True
                    continue
                # Any subsequent non-empty row counts as data
                if any((str(cell).strip() != "") for cell in row):
                    return True
            # No data rows found
            return False
    except OSError:
        # On read error, conservatively return False (assume no data)
        return False


def csv_contains_email(path: str) -> bool:
    # Fast path: scan raw lines first to avoid CSV parsing overhead when possible.
    # If regex matches anywhere, we can return early. If not, no need to parse as CSV.
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                if EMAIL_REGEX.search(line):
                    return True
        return False
    except OSError:
        return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Analyze CSVs under a directory")
    parser.add_argument(
        "root",
        nargs="?",
        default=os.path.join(os.getcwd(), "schools"),
        help="Root directory to search (default: ./schools)",
    )
    parser.add_argument(
        "--list",
        dest="list_modes",
        choices=["empty", "emails", "all"],
        nargs="+",
        help="Optionally list CSV paths for one or more categories: empty, emails, all",
    )
    args = parser.parse_args()

    root = os.path.abspath(args.root)
    total_csvs = 0
    empty_csvs = 0
    csvs_with_emails = 0
    all_paths: list[str] = []
    empty_paths: list[str] = []
    email_paths: list[str] = []

    for path in iter_csv_paths(root):
        total_csvs += 1
        all_paths.append(path)
        # Empty means header-only or blank, not zero-byte
        if not csv_has_data_rows(path):
            empty_csvs += 1
            empty_paths.append(path)
            # Header-only files are unlikely to have emails, but still check below
            # We intentionally do not 'continue' so we still count emails in headers if present
        if csv_contains_email(path):
            csvs_with_emails += 1
            email_paths.append(path)

    print(
        "\n".join(
            [
                f"root: {root}",
                f"total_csvs: {total_csvs}",
                f"empty_csvs: {empty_csvs}",
                f"csvs_with_emails: {csvs_with_emails}",
            ]
        )
    )

    # Optional path listings
    if args.list_modes:
        for mode in args.list_modes:
            if mode == "all":
                print("\n# all csvs")
                for p in all_paths:
                    print(p)
            elif mode == "empty":
                print("\n# empty csvs (header-only or blank)")
                for p in empty_paths:
                    print(p)
            elif mode == "emails":
                print("\n# csvs with emails")
                for p in email_paths:
                    print(p)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
