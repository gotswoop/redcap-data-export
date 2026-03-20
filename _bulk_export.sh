#!/bin/bash

set -euo pipefail

#!/bin/bash

set -euo pipefail

# -----------------------------------------------------------------------------
# REDCap Project Data Export Pipeline
#
# Author:
#   Swaroop Samek
#   March 2026
#
# Description:
#   This script exports data and metadata for all active REDCap projects,
#   compresses and encrypts the data exports, and uploads the results to S3.
#
# Workflow:
#   1. Loads required configuration from /root/.settings.conf
#   2. Retrieves active project IDs from the REDCap database
#   3. For each project (in parallel):
#        - Exports raw data in CSV and XML formats (gzip compressed)
#        - Retries failed exports up to a defined limit
#        - Encrypts output files using GPG
#        - Exports project metadata (CSV and XML, uncompressed/unencrypted)
#        - Logs all activity per project
#   4. Tracks successes and failures
#   5. Syncs all output files to an S3 bucket (day-based path)
#        - Removes stale files from S3 using --delete
#
# Output:
#   - Encrypted data files:
#       project_<PID>_raw_data.csv.gz.gpg
#       project_<PID>_raw_data.xml.gz.gpg
#   - Metadata files:
#       project_<PID>_metadata.csv
#       project_<PID>_metadata.xml
#   - Logs:
#       logs/project_<PID>.log
#   - Status tracking:
#       success.txt
#       failed.txt
#
# Requirements:
#   - AWS CLI configured (via ${AWS})
#   - GPG installed and passphrase file available
#   - MySQL access configured (via ~/.my.cnf)
#   - Python export script available at ${SCRIPT}
#
# Notes:
#   - Parallelism controlled via MAX_JOBS
#   - Temporary working directory is fully reset on each run
#   - S3 sync uses --delete to ensure destination mirrors source
# -----------------------------------------------------------------------------

# Load settings
source /root/.settings.conf

: "${CONFIG_DIR:?CONFIG_DIR not set}"
: "${S3_BUCKET:?S3_BUCKET not set}"
: "${LOCAL_FOLDER:?LOCAL_FOLDER not set}"
: "${DB:?DB not set}"

SCRIPT="/root/_CRONS/export_project_data/redcap_export.py"

DAY=$(date +%d)

TMP_DIR="${LOCAL_FOLDER}/tmp_project_data"
LOGDIR="${TMP_DIR}/logs"

S3_PATH="${S3_BUCKET}/project_data/day_${DAY}"
KEY_FILE="${CONFIG_DIR}/gpg_passphrase"

MAX_JOBS=4

# ---------------------------
# Clean temp directory
# ---------------------------
echo "Cleaning temp directory..."
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
mkdir -p "${LOGDIR}"

SUCCESS_FILE="${TMP_DIR}/success.txt"
FAILED_FILE="${TMP_DIR}/failed.txt"

: > "${SUCCESS_FILE}"
: > "${FAILED_FILE}"

# ---------------------------
# Export function
# ---------------------------
process_project() {
	local PID="$1"
	local LOGFILE="${LOGDIR}/project_${PID}.log"

	echo "[$(date)] START project ${PID}" >> "${LOGFILE}"

	for FORMAT in csv xml; do
		local ATTEMPT=1
		local MAX_RETRIES=2
		local SUCCESS=0

		while [[ ${ATTEMPT} -le ${MAX_RETRIES} ]]; do
			echo "[$(date)] project ${PID} ${FORMAT} attempt ${ATTEMPT}" >> "${LOGFILE}"

			rm -f "${TMP_DIR}/project_${PID}_raw_data.${FORMAT}.gz"

			(
				cd "${TMP_DIR}"
				"${SCRIPT}" "${PID}" --gzip --format "${FORMAT}"
			) >> "${LOGFILE}" 2>&1

			if [[ $? -eq 0 ]]; then
				SUCCESS=1
			else
				SUCCESS=0
			fi

			if [[ ${SUCCESS} -eq 1 ]]; then
				OUTFILE="project_${PID}_raw_data.${FORMAT}.gz"

				if [[ -f "${TMP_DIR}/${OUTFILE}" ]]; then
					echo "[$(date)] Encrypting ${OUTFILE}" >> "${LOGFILE}"

					if [[ ! -s "${TMP_DIR}/${OUTFILE}" ]]; then
						echo "[$(date)] ERROR: empty file ${OUTFILE}" >> "${LOGFILE}"
						SUCCESS=0
					else
						if ! /bin/gpg --batch --yes --pinentry-mode loopback \
							--passphrase-file "${KEY_FILE}" \
							--symmetric --output "${TMP_DIR}/${OUTFILE}.gpg" \
							"${TMP_DIR}/${OUTFILE}" >> "${LOGFILE}" 2>&1; then

							echo "[$(date)] ERROR: gpg failed for ${OUTFILE}" >> "${LOGFILE}"
							SUCCESS=0
						else
							rm -f "${TMP_DIR}/${OUTFILE}"
						fi
					fi
				else
					echo "[$(date)] ERROR: Expected file not found: ${OUTFILE}" >> "${LOGFILE}"
					SUCCESS=0
				fi
				break
			fi

			((ATTEMPT++))
			echo "[$(date)] RETRY project ${PID} ${FORMAT}" >> "${LOGFILE}"
		done

		if [[ ${SUCCESS} -ne 1 ]]; then
			echo "[$(date)] FAILED project ${PID} ${FORMAT}" >> "${LOGFILE}"
			echo "${PID}" >> "${FAILED_FILE}"
			return 1
		fi
	done

	(
		cd "${TMP_DIR}"
		"${SCRIPT}" "${PID}" --export-metadata
	) >> "${LOGFILE}" 2>&1

	(
		cd "${TMP_DIR}"
		"${SCRIPT}" "${PID}" --export-metadata --metadata-format xml
	) >> "${LOGFILE}" 2>&1

	echo "[$(date)] SUCCESS project ${PID}" >> "${LOGFILE}"
	echo "${PID}" >> "${SUCCESS_FILE}"
}

export -f process_project
export TMP_DIR SCRIPT LOGDIR SUCCESS_FILE FAILED_FILE KEY_FILE AWS DB

# ---------------------------
# Run exports
# ---------------------------
echo "Fetching project IDs..."
PROJECT_IDS=$(mysql -N -e "SELECT project_id FROM ${DB}.redcap_projects WHERE status = 1;")
echo "Starting exports..."
echo "${PROJECT_IDS}" | xargs -n1 -P${MAX_JOBS} -I{} bash -c 'process_project "$@"' _ {}

echo "Export phase complete"

# ---------------------------
# Upload EVERYTHING to S3 (day-based path)
# ---------------------------
echo "Uploading to S3: ${S3_PATH}"

"${AWS}" s3 sync "${TMP_DIR}" "${S3_PATH}" --no-progress --delete

echo "Upload complete"

# ---------------------------
# Summary
# ---------------------------
echo "----------------------------------------"
echo "All jobs complete"
echo "Success count: $(wc -l < "${SUCCESS_FILE}")"
echo "Failed count: $(wc -l < "${FAILED_FILE}")"
echo "S3 location: ${S3_PATH}"
