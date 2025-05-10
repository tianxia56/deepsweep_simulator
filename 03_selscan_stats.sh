#!/bin/bash

# This script runs selscan (nSL and iHH12) on simulation outputs.
# It takes an argument to specify whether to process neutral or selection simulations.

# Usage: ./03_selscan_stats.sh <neut|sel>

# --- Script Argument Validation ---
if [ "$#" -ne 1 ] || ([ "$1" != "neut" ] && [ "$1" != "sel" ]); then
    echo "Usage: $0 <neut|sel>"
    echo "  neut: Process neutral simulations"
    echo "  sel:  Process selection simulations"
    exit 1
fi
SIM_TYPE="$1" # "neut" or "sel"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/selscan_stats_${SIM_TYPE}.log" # Log file name now includes SIM_TYPE

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Helper Functions (for host script) ---
log_message() {
    local type="$1"
    local message="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${type}] - ${message}"
}

# --- Host Configuration ---
CONFIG_FILE="00config.json"

# Read POP1_TARGET from JSON using jq
if ! command -v jq &> /dev/null; then
    log_message "WARNING_JQ" "jq command not found. Please install jq or ensure Python fallback works."
    POP1_TARGET_FOR_SELSCAN=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1]))['selected_pop'])" "${CONFIG_FILE}" 2>/dev/null)
    PYTHON_EXIT_CODE_POP1=$?
    if [ $PYTHON_EXIT_CODE_POP1 -ne 0 ] || [ -z "$POP1_TARGET_FOR_SELSCAN" ] || [ "$POP1_TARGET_FOR_SELSCAN" = "null" ]; then
        log_message "ERROR_CONFIG" "Failed to read 'selected_pop' using Python fallback from ${CONFIG_FILE}."
        exit 1
    fi
    log_message "INFO" "Read config using Python fallback for POP1_TARGET. jq is highly recommended."
else
    POP1_TARGET_FOR_SELSCAN=$(jq -r '.selected_pop' "${CONFIG_FILE}")
fi

if [ -z "$POP1_TARGET_FOR_SELSCAN" ] || [ "$POP1_TARGET_FOR_SELSCAN" = "null" ]; then
    log_message "ERROR_CONFIG" "'selected_pop' not found or null in ${CONFIG_FILE}."
    exit 1
fi

# Type-specific configurations
INPUT_SIM_DIR=""
INPUT_CSV_BASENAME=""
PATH_PREFIX_FOR_FILES="" # "neut" or "sel" for internal file naming
SELSCAN_OUTPUT_DIR_BASE="one_pop_stats" # Base name, will append _neut or _sel

if [ "${SIM_TYPE}" == "neut" ]; then
    INPUT_SIM_DIR="neutral_sims"
    INPUT_CSV_BASENAME="cosi.neut.runtime.csv" # Input from 01_cosi_neut.sh
    PATH_PREFIX_FOR_FILES="neut"
    SELSCAN_OUTPUT_DIR="${SELSCAN_OUTPUT_DIR_BASE}_neut"
else # sel
    INPUT_SIM_DIR="selected_sims"
    INPUT_CSV_BASENAME="cosi.sel.runtime.csv" # Input from 02_cosi_sel.sh
    PATH_PREFIX_FOR_FILES="sel"
    SELSCAN_OUTPUT_DIR="${SELSCAN_OUTPUT_DIR_BASE}_sel"
fi

RUNTIME_DIR="runtime" 
DOCKER_IMAGE_SELSCAN="docker.io/tx56/deepsweep_simulator:latest"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
log_message "INFO" "Host Script (03_selscan_stats.sh for ${SIM_TYPE}) Started: $(date)"
log_message "INFO" "Reading configuration from ${CONFIG_FILE}"
log_message "INFO" "Target Population ID for Selscan (from .selected_pop): ${POP1_TARGET_FOR_SELSCAN}"
log_message "INFO" "Processing SIM_TYPE: ${SIM_TYPE}"
log_message "INFO" "TPED Input Directory: ${HOST_CWD}/${INPUT_SIM_DIR}"
log_message "INFO" "Selscan Output Directory: ${HOST_CWD}/${SELSCAN_OUTPUT_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Configuration file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${SELSCAN_OUTPUT_DIR}"
mkdir -p "${RUNTIME_DIR}" 

INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV file '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV file '${INPUT_CSV_FILE_HOST}' has no numeric sim_ids in 2nd col."; fi

if ! docker image inspect "$DOCKER_IMAGE_SELSCAN" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_SELSCAN} not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_SELSCAN"; then log_message "ERROR" "Failed to pull Docker image ${DOCKER_IMAGE_SELSCAN}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_SELSCAN} already exists locally."
fi
log_message "INFO" "Starting Docker container for Selscan (${SIM_TYPE})..."

# --- Docker Execution ---
docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_POP1_TARGET="${POP1_TARGET_FOR_SELSCAN}" \
    -e CONTAINER_INPUT_SIM_DIR="${INPUT_SIM_DIR}" \
    -e CONTAINER_SELSCAN_OUTPUT_DIR="${SELSCAN_OUTPUT_DIR}" \
    -e CONTAINER_RUNTIME_DIR="${RUNTIME_DIR}" \
    -e CONTAINER_INPUT_CSV_BASENAME="${INPUT_CSV_BASENAME}" \
    -e CONTAINER_PATH_PREFIX="${PATH_PREFIX_FOR_FILES}" \
    "$DOCKER_IMAGE_SELSCAN" /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 

cleanup_and_exit() {
    log_container "Caught signal! Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM

log_container "----------------------------------------------------"
log_container "Container Script (Selscan Stats for ${CONTAINER_PATH_PREFIX}) Started: $(date)"
log_container "----------------------------------------------------"
# (Echo received ENV VARS - condensed for brevity)
log_container "Received POP1_TARGET: [${CONTAINER_POP1_TARGET}]"
log_container "Received INPUT_SIM_DIR: [./${CONTAINER_INPUT_SIM_DIR}]" # e.g. ./neutral_sims or ./selected_sims
log_container "Received PATH_PREFIX: [${CONTAINER_PATH_PREFIX}]" # e.g. neut or sel

INPUT_CSV_FILE_IN_CONTAINER="./${CONTAINER_RUNTIME_DIR}/${CONTAINER_INPUT_CSV_BASENAME}"

if [ ! -f "${INPUT_CSV_FILE_IN_CONTAINER}" ]; then
    log_container "CRITICAL ERROR: Input CSV file '${INPUT_CSV_FILE_IN_CONTAINER}' not found. Exiting."
    exit 1
fi

run_selscan_for_sim() {
    local sim_id_from_csv=$1 
    local target_pop_for_tped="${CONTAINER_POP1_TARGET}"
    local current_path_prefix="${CONTAINER_PATH_PREFIX}" # "neut" or "sel"

    # TPED filename: e.g. ./neutral_sims/neut.hap.0_0_1.tped or ./selected_sims/sel.hap.0_0_1.tped
    local tped_file="./${CONTAINER_INPUT_SIM_DIR}/${current_path_prefix}.hap.${sim_id_from_csv}_0_${target_pop_for_tped}.tped"
    # Selscan output base: e.g. neut.0_pop1 or sel.0_pop1
    local base_name_selscan_out="${current_path_prefix}.${sim_id_from_csv}_pop${target_pop_for_tped}"
    
    log_container "Processing sim_id ${sim_id_from_csv} for pop ${target_pop_for_tped} (type: ${current_path_prefix}) with TPED: ${tped_file}"

    if [ ! -f "${tped_file}" ]; then
        log_container "WARNING: TPED file ${tped_file} not found. Skipping selscan."
        return 1
    fi

    mkdir -p "./${CONTAINER_SELSCAN_OUTPUT_DIR}"
    mkdir -p "./${CONTAINER_RUNTIME_DIR}"

    # nSL
    log_container "Running selscan --nsl for ${base_name_selscan_out}"
    local start_time_nsl=$(date +%s)
    selscan --nsl --tped "${tped_file}" --out "./${CONTAINER_SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}" --threads 4
    local end_time_nsl=$(date +%s)
    local runtime_nsl=$((end_time_nsl - start_time_nsl))
    if [ -f "./${CONTAINER_SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}.nsl.out" ]; then
        echo "sim_id,${sim_id_from_csv},pop_id,${target_pop_for_tped},nsl_runtime,${runtime_nsl},seconds" >> "./${CONTAINER_RUNTIME_DIR}/nsl.${current_path_prefix}.runtime.csv"
        log_container "nSL for ${base_name_selscan_out} completed. Runtime: ${runtime_nsl}s"
    else
        log_container "WARNING: nSL output file not found for ${base_name_selscan_out}."
    fi
    
    # iHH12
    log_container "Running selscan --ihh12 for ${base_name_selscan_out}"
    local start_time_ihh12=$(date +%s)
    selscan --ihh12 --tped "${tped_file}" --out "./${CONTAINER_SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}" --threads 4
    local end_time_ihh12=$(date +%s)
    local runtime_ihh12=$((end_time_ihh12 - start_time_ihh12))
    if [ -f "./${CONTAINER_SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}.ihh12.out" ]; then
        echo "sim_id,${sim_id_from_csv},pop_id,${target_pop_for_tped},ihh12_runtime,${runtime_ihh12},seconds" >> "./${CONTAINER_RUNTIME_DIR}/ihh12.${current_path_prefix}.runtime.csv"
        log_container "iHH12 for ${base_name_selscan_out} completed. Runtime: ${runtime_ihh12}s"
    else
        log_container "WARNING: iHH12 output file not found for ${base_name_selscan_out}."
    fi
}

log_container "Reading all sim_ids from ${INPUT_CSV_FILE_IN_CONTAINER} for linear processing..."
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_IN_CONTAINER}" | sort -un)

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_container "No valid sim_ids found in ${INPUT_CSV_FILE_IN_CONTAINER}. Nothing to process."
else
    log_container "Found unique sim_ids to process (sorted): ${sim_ids_to_run[*]}"
    for sim_id_val in "${sim_ids_to_run[@]}"; do
        run_selscan_for_sim "$sim_id_val"
    done
    log_container "All sim_ids from CSV processed by selscan for target population."
fi

log_container "Selscan processing for ${CONTAINER_PATH_PREFIX} simulations finished."
log_container "----------------------------------------------------"
log_container "Container Script (Selscan Stats for ${CONTAINER_PATH_PREFIX}) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
log_message "INFO" "Docker container (Selscan Stats for ${SIM_TYPE}) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then log_message "INFO" "Script (Selscan Stats for ${SIM_TYPE}) likely interrupted by user (Ctrl+C)."; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then log_message "ERROR" "Docker container (Selscan Stats for ${SIM_TYPE}) reported an error."; fi
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (03_selscan_stats.sh for ${SIM_TYPE}) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"
