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

# --- Helper Functions ---
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

INPUT_CSV_FILE_LOCAL="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_LOCAL}" ]; then log_message "ERROR" "Input CSV file '${INPUT_CSV_FILE_LOCAL}' not found."; exit 1; fi
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_LOCAL}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV file '${INPUT_CSV_FILE_LOCAL}' has no numeric sim_ids in 2nd col."; fi

# Check for local selscan executable
if ! command -v selscan &> /dev/null; then
    log_message "CRITICAL_ERROR" "'selscan' command not found. Please install selscan and ensure it's in your PATH. Exiting."
    exit 1
fi
log_message "INFO" "Local 'selscan' executable found."

log_message "INFO" "Starting Selscan processing locally for ${SIM_TYPE} simulations..."

# --- Selscan Execution (Local) ---
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Local Selscan Processing for ${SIM_TYPE} Started: $(date)"
log_message "INFO" "----------------------------------------------------"

cleanup_and_exit() {
    log_message "INFO" "Caught signal! Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM

run_selscan_for_sim() {
    local sim_id_from_csv=$1 
    local target_pop_for_tped="${POP1_TARGET_FOR_SELSCAN}"
    local current_path_prefix="${PATH_PREFIX_FOR_FILES}" # "neut" or "sel"

    # TPED filename: e.g. ./neutral_sims/neut.hap.0_0_1.tped or ./selected_sims/sel.hap.0_0_1.tped
    local tped_file="./${INPUT_SIM_DIR}/${current_path_prefix}.hap.${sim_id_from_csv}_0_${target_pop_for_tped}.tped"
    # Selscan output base: e.g. neut.0_pop1 or sel.0_pop1
    local base_name_selscan_out="${current_path_prefix}.${sim_id_from_csv}_pop${target_pop_for_tped}"
    
    log_message "INFO" "Processing sim_id ${sim_id_from_csv} for pop ${target_pop_for_tped} (type: ${current_path_prefix}) with TPED: ${tped_file}"

    if [ ! -f "${tped_file}" ]; then
        log_message "WARNING" "TPED file ${tped_file} not found. Skipping selscan for this simulation."
        return 1
    fi

    mkdir -p "./${SELSCAN_OUTPUT_DIR}"
    mkdir -p "./${RUNTIME_DIR}"

    # nSL
    log_message "INFO" "Running selscan --nsl for ${base_name_selscan_out}"
    local start_time_nsl=$(date +%s)
    selscan --nsl --tped "${tped_file}" --out "./${SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}" --threads 4
    local end_time_nsl=$(date +%s)
    local runtime_nsl=$((end_time_nsl - start_time_nsl))
    if [ -f "./${SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}.nsl.out" ]; then
        echo "sim_id,${sim_id_from_csv},pop_id,${target_pop_for_tped},nsl_runtime,${runtime_nsl},seconds" >> "./${RUNTIME_DIR}/nsl.${current_path_prefix}.runtime.csv"
        log_message "INFO" "nSL for ${base_name_selscan_out} completed. Runtime: ${runtime_nsl}s"
    else
        log_message "WARNING" "nSL output file not found for ${base_name_selscan_out}. Selscan command might have failed."
    fi
    
    # iHH12
    log_message "INFO" "Running selscan --ihh12 for ${base_name_selscan_out}"
    local start_time_ihh12=$(date +%s)
    selscan --ihh12 --tped "${tped_file}" --out "./${SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}" --threads 4
    local end_time_ihh12=$(date +%s)
    local runtime_ihh12=$((end_time_ihh12 - start_time_ihh12))
    if [ -f "./${SELSCAN_OUTPUT_DIR}/${base_name_selscan_out}.ihh12.out" ]; then
        echo "sim_id,${sim_id_from_csv},pop_id,${target_pop_for_tped},ihh12_runtime,${runtime_ihh12},seconds" >> "./${RUNTIME_DIR}/ihh12.${current_path_prefix}.runtime.csv"
        log_message "INFO" "iHH12 for ${base_name_selscan_out} completed. Runtime: ${runtime_ihh12}s"
    else
        log_message "WARNING" "iHH12 output file not found for ${base_name_selscan_out}. Selscan command might have failed."
    fi
}

log_message "INFO" "Reading all sim_ids from ${INPUT_CSV_FILE_LOCAL} for linear processing..."
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_LOCAL}" | sort -un)

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_message "WARNING" "No valid sim_ids found in ${INPUT_CSV_FILE_LOCAL}. Nothing to process."
else
    log_message "INFO" "Found unique sim_ids to process (sorted): ${sim_ids_to_run[*]}"
    for sim_id_val in "${sim_ids_to_run[@]}"; do
        run_selscan_for_sim "$sim_id_val"
    done
    log_message "INFO" "All sim_ids from CSV processed by selscan for target population."
fi

log_message "INFO" "Selscan processing for ${SIM_TYPE} simulations finished."
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Local Selscan Processing for ${SIM_TYPE} Finished: $(date)"
log_message "INFO" "----------------------------------------------------"

log_message "INFO" "Host Script (03_selscan_stats.sh for ${SIM_TYPE}) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"
exit 0