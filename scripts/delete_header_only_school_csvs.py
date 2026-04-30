#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def csv_is_header_only(path: Path) -> bool:
    """Return True when a CSV has a header row and no non-empty data rows."""
    try:
        with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as file:
            reader = csv.reader(file)
            header_seen = False

            for row in reader:
                if not row or all(cell.strip() == "" for cell in row):
                    continue

                if not header_seen:
                    header_seen = True
                    continue

                if any(cell.strip() != "" for cell in row):
                    return False

            return header_seen
    except (OSError, csv.Error):
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Delete school CSV files that contain only column names."
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=Path.cwd() / "schools",
        type=Path,
        help="Root directory to search (default: ./schools)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List files that would be deleted without deleting them.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Only print the final count.",
    )
    args = parser.parse_args()

    root = args.root.resolve()
    if not root.is_dir():
        print(f"Directory not found: {root}")
        return 1

    deleted = 0
    for path in sorted(root.rglob("*.csv")):
        if not path.is_file() or not csv_is_header_only(path):
            continue

        if not args.dry_run:
            path.unlink()
        if not args.quiet:
            action = "would delete" if args.dry_run else "deleted"
            print(f"{action}: {path}")
        deleted += 1

    action = "would delete" if args.dry_run else "deleted"
    print(f"{action}: {deleted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
