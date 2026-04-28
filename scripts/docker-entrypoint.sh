#!/usr/bin/env sh
set -eu

if [ "$#" -eq 0 ]; then
  set -- --schools-csv "${SCHOOLYANK_INPUT_CSV:-/app/input/schools_with_staff_urls.csv}"
fi

exec bun run index.ts "$@"
