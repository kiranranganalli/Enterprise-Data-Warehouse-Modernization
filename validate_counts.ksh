#!/usr/bin/ksh
# Purpose: Validate row counts between landing and stage; fail fast if mismatched
# Usage:   ./validate_counts.ksh YYYYMMDD
set -euo pipefail

RUN_DATE=${1:-$(date +%Y%m%d)}
LANDING_DIR="/data/landing/${RUN_DATE}"
STAGE_DIR="/data/stage/${RUN_DATE}"

count_rows() {
  awk 'END{print NR-1}' "$1"
}

for src in ${LANDING_DIR}/*.csv; do
  tgt=${STAGE_DIR}/$(basename "$src")
  if [ ! -f "$tgt" ]; then
    print "[ERROR] Missing staged file: $tgt"; exit 1
  fi
  c1=$(count_rows "$src")
  c2=$(count_rows "$tgt")
  if [ "$c1" -ne "$c2" ]; then
    print "[ERROR] Rowcount mismatch: $(basename "$src") src=$c1 tgt=$c2"; exit 2
  fi
  print "[OK] $(basename "$src") rows=$c1"
done
print "[DONE] Rowcount validation passed"
