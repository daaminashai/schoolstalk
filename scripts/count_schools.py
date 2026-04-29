#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Count school CSV files under a directory"
    )
    parser.add_argument(
        "root",
        nargs="?",
        default=Path.cwd() / "schools",
        help="Root directory to search (default: ./schools)",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    if not root.exists() or not root.is_dir():
        print(f"Directory not found: {root}")
        return 1

    count = sum(1 for path in root.rglob("*.csv") if path.is_file())
    print(f"schools: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
