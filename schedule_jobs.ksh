#!/usr/bin/ksh
# Purpose: Daily batch scheduler — run Informatica/SSIS + Python orchestration.
# Usage:   ./schedule_jobs.ksh YYYYMMDD
set -euo pipefail

RUN_DATE=${1:-$(date +%Y%m%d)}
BASE_DIR="/opt/edw"
LOG_DIR="${BASE_DIR}/logs"
mkdir -p "${LOG_DIR}"

print "[INFO] ${RUN_DATE}: Starting daily batch"

# Example: call Informatica/SSIS (placeholders)
# pmcmd startworkflow -sv Intg_Sv -d Domain -u user -p pass -f EDW_Folder -w wf_Load_Dimensions
# dtexec /f "C:\\ssis\\LoadFacts.dtsx" /SET \Package.Variables[RunDate];${RUN_DATE}

# Python orchestration
cd "${BASE_DIR}"
source .venv/bin/activate || true
python etl_modernization.py >> ${LOG_DIR}/orchestration_${RUN_DATE}.log 2>&1

print "[DONE] Batch complete"
