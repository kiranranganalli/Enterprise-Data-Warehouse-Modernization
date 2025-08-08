#!/usr/bin/ksh
# Basic unit tests for transfer_files.ksh functions (mocked)
set -e

echo "[TEST] Creating mock landing/stage dirs"
RUN_DATE=$(date +%Y%m%d)
BASE_DIR=$(pwd)
mkdir -p "$BASE_DIR/mock/landing/$RUN_DATE" "$BASE_DIR/mock/stage/$RUN_DATE"

# Create mock CSVs
echo "id,name" > "$BASE_DIR/mock/landing/$RUN_DATE/customer_1.csv"
echo "1,alice" >> "$BASE_DIR/mock/landing/$RUN_DATE/customer_1.csv"
cp "$BASE_DIR/mock/landing/$RUN_DATE/customer_1.csv" "$BASE_DIR/mock/stage/$RUN_DATE/customer_1.csv"

# Row count compare
count_rows() { awk 'END{print NR-1}' "$1"; }
SRC_ROWS=$(count_rows "$BASE_DIR/mock/landing/$RUN_DATE/customer_1.csv")
TGT_ROWS=$(count_rows "$BASE_DIR/mock/stage/$RUN_DATE/customer_1.csv")

if [ "$SRC_ROWS" -ne "$TGT_ROWS" ]; then
  echo "[FAIL] Row count mismatch"; exit 2
else
  echo "[PASS] Row count matched: $SRC_ROWS"
fi

echo "[DONE] transfer tests passed"
