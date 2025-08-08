#!/usr/bin/ksh
# Purpose: Securely pull vendor files via SFTP, checksum them, and drop to landing.
# Usage:   ./transfer_files.ksh YYYYMMDD
set -euo pipefail

RUN_DATE=${1:-$(date +%Y%m%d)}
VENDOR_HOST="sftp.vendor.example"
VENDOR_USER="vendoruser"
LANDING_DIR="/data/landing/${RUN_DATE}"
LOG_DIR="/data/logs"
mkdir -p "${LANDING_DIR}" "${LOG_DIR}"

print "[INFO] Starting SFTP pull for ${RUN_DATE}"

sftp ${VENDOR_USER}@${VENDOR_HOST} <<EOF
get /outbound/${RUN_DATE}/customer_*.csv ${LANDING_DIR}/
get /outbound/${RUN_DATE}/sales_*.csv    ${LANDING_DIR}/
bye
EOF

# checksums
echo "[INFO] Checksumming files …"
for f in ${LANDING_DIR}/*.csv; do
  md5sum "$f" | tee -a ${LOG_DIR}/checksums_${RUN_DATE}.log
done

echo "[DONE] Vendor transfers complete"
