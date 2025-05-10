#!/bin/bash

# This script will run all simulation and analysis steps linearly.

# --- Configuration ---
CONFIG_FILE="00config.json"
LOG_DIR="logs"
MAIN_LOG_FILE="${LOG_DIR}/main_launch.log"

# --- Initial Setup ---
mkdir -p "${LOG_DIR}"

# --- Helper Functions ---
log_message() {
    local message="$1"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] - ${message}" | tee -a "${MAIN_LOG_FILE}"
}

check_exit_status() {
    local exit_status=$1
    local script_name=$2
    local script_args="${3:-}" 
    if [ $exit_status -ne 0 ]; then
        log_message "ERROR: ${script_name} ${script_args} failed with exit status ${exit_status}. Aborting pipeline."
        exit $exit_status
    else
        log_message "SUCCESS: ${script_name} ${script_args} completed successfully."
    fi
}

# --- Main Script Execution ---

log_message "Main pipeline script started."
log_message "Full output of individual scripts will be in their respective log files."

if [ ! -f "${CONFIG_FILE}" ]; then
    log_message "CRITICAL ERROR: Configuration file ${CONFIG_FILE} not found at $(pwd)/${CONFIG_FILE}! Aborting."
    exit 1
fi
log_message "Configuration file ${CONFIG_FILE} found."

# Step 1: Clean up
log_message "Step 1: Running clean.sh..."
if [ -f "./clean.sh" ]; then
    bash clean.sh
    check_exit_status $? "clean.sh"
else
    log_message "WARNING: clean.sh not found. Skipping cleanup."
fi
log_message "Clean up complete (or skipped)."

# Step 2: Validate AND UPDATE .par file length
log_message "Step 2: Validating/Updating simulation length in .par file from ${CONFIG_FILE}..."
PAR_FILE_BASENAME=""
JSON_SIM_LENGTH=""
if ! command -v jq &> /dev/null; then
    log_message "Warning: jq command not found. Attempting Python fallback."
    PAR_FILE_BASENAME=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1]))['demographic_model'])" "${CONFIG_FILE}")
    PYTHON_EXIT_CODE_1=$?
    JSON_SIM_LENGTH=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1]))['simulation_length'])" "${CONFIG_FILE}")
    PYTHON_EXIT_CODE_2=$?
    if [ $PYTHON_EXIT_CODE_1 -ne 0 ] || [ $PYTHON_EXIT_CODE_2 -ne 0 ] || [ -z "${PAR_FILE_BASENAME}" ] || [ "${PAR_FILE_BASENAME}" = "null" ] || [ -z "${JSON_SIM_LENGTH}" ] || [ "${JSON_SIM_LENGTH}" = "null" ]; then
         log_message "ERROR: Python fallback failed to read required config values from ${CONFIG_FILE}."
         exit 1
    fi
else
    PAR_FILE_BASENAME=$(jq -r '.demographic_model' "${CONFIG_FILE}")
    JSON_SIM_LENGTH=$(jq -r '.simulation_length' "${CONFIG_FILE}")
fi
if [ -z "${PAR_FILE_BASENAME}" ] || [ "${PAR_FILE_BASENAME}" = "null" ]; then log_message "ERROR: Could not read 'demographic_model' from ${CONFIG_FILE}."; exit 1; fi
if [ -z "${JSON_SIM_LENGTH}" ] || [ "${JSON_SIM_LENGTH}" = "null" ]; then log_message "ERROR: Could not read 'simulation_length' from ${CONFIG_FILE}."; exit 1; fi
if ! [[ "${JSON_SIM_LENGTH}" =~ ^[0-9]+$ ]]; then log_message "ERROR: 'simulation_length' ('${JSON_SIM_LENGTH}') from ${CONFIG_FILE} is not a valid integer."; exit 1; fi
PAR_FILE_PATH="demographic_models/${PAR_FILE_BASENAME}"
if [ ! -f "${PAR_FILE_PATH}" ]; then log_message "ERROR: Demographic model file '${PAR_FILE_PATH}' not found!"; exit 1; fi
PAR_FILE_LENGTH=$(grep -E "^length[[:space:]]+" "${PAR_FILE_PATH}" | awk '{print $2}')
log_message "Target simulation_length from config: ${JSON_SIM_LENGTH}"
if [ -n "${PAR_FILE_LENGTH}" ]; then log_message "Current length in .par file: ${PAR_FILE_LENGTH}"; fi
if [ "${PAR_FILE_LENGTH}" != "${JSON_SIM_LENGTH}" ]; then
    log_message "INFO: Updating .par file length to ${JSON_SIM_LENGTH}..."
    cp "${PAR_FILE_PATH}" "${PAR_FILE_PATH}.bak_$(date +%Y%m%d_%H%M%S)"
    sed -i.bak_sed_op "s/^length[[:space:]].*/length ${JSON_SIM_LENGTH}/" "${PAR_FILE_PATH}"
    if [ $? -eq 0 ]; then rm -f "${PAR_FILE_PATH}.bak_sed_op" ; NEW_PAR_FILE_LENGTH=$(grep -E "^length[[:space:]]+" "${PAR_FILE_PATH}" | awk '{print $2}')
        if [ "${NEW_PAR_FILE_LENGTH}" == "${JSON_SIM_LENGTH}" ]; then log_message "SUCCESS: .par file length updated and verified."; else log_message "ERROR: Failed to verify .par file length update."; exit 1; fi
    else log_message "ERROR: sed command failed to update .par file length."; exit 1; fi
else log_message "SUCCESS: .par file length already matches config (${JSON_SIM_LENGTH})."; fi
log_message "Validation/Update of .par file length complete."

# Step 3: Make haplotypes
log_message "Step 3: Generating neutral simulations (01_cosi_neut.sh)..."
bash 01_cosi_neut.sh
check_exit_status $? "01_cosi_neut.sh"

log_message "Step 4: Generating selected simulations (02_cosi_sel.sh)..."
bash 02_cosi_sel.sh
check_exit_status $? "02_cosi_sel.sh"
log_message "Haplotype generation complete."

# Step 5: Run selscan
log_message "Step 5: Running selscan for neutral simulations (03_selscan_stats.sh neut)..."
bash 03_selscan_stats.sh neut
check_exit_status $? "03_selscan_stats.sh" "neut"

log_message "Step 6: Running selscan for selected simulations (03_selscan_stats.sh sel)..."
bash 03_selscan_stats.sh sel
check_exit_status $? "03_selscan_stats.sh" "sel"
log_message "Selscan computations complete."

# Step 7: Run iSAFE (for selection simulations)
log_message "Step 7: Running iSAFE processing (04_isafe.sh)..."
bash 04_isafe.sh
check_exit_status $? "04_isafe.sh"
log_message "iSAFE processing complete."

# Step 8: Run XPEHH Processing 
log_message "Step 8: Running XPEHH processing for neutral simulations (05_hapbin_xpehh.sh neut)..."
bash 05_hapbin_xpehh.sh neut 
check_exit_status $? "05_hapbin_xpehh.sh" "neut"

log_message "Step 9: Running XPEHH processing for selected simulations (05_hapbin_xpehh.sh sel)..."
bash 05_hapbin_xpehh.sh sel 
check_exit_status $? "05_hapbin_xpehh.sh" "sel"
log_message "XPEHH processing complete."

# Step 10: Run IHS Processing
log_message "Step 10: Running IHS processing for neutral simulations (06_hapbin_ihs_delihh.sh neut)..."
bash 06_hapbin_ihs_delihh.sh neut
check_exit_status $? "06_hapbin_ihs_delihh.sh" "neut"

log_message "Step 11: Running IHS processing for selected simulations (06_hapbin_ihs_delihh.sh sel)..."
bash 06_hapbin_ihs_delihh.sh sel
check_exit_status $? "06_hapbin_ihs_delihh.sh" "sel"
log_message "IHS processing complete."

# Step 12: Run FST/DelDAF Processing (ONLY FOR SELECTION SIMULATIONS)
log_message "Step 12: Running FST/DelDAF for selected simulations (07_fst_deldaf_processing.sh)..."
bash 07_fst_deldaf_processing.sh 
check_exit_status $? "07_fst_deldaf_processing.sh"
log_message "FST/DelDAF processing complete."

# Step 13: Create Normalization Bins (from NEUTRAL simulations)
log_message "Step 13: Creating normalization bins from neutral simulation stats (08_create_norm_bins.sh)..."
bash 08_create_norm_bins.sh
check_exit_status $? "08_create_norm_bins.sh"
log_message "Normalization bin creation complete."

# Step 14: Normalize One-Population Statistics (for SELECTION simulations)
log_message "Step 14: Normalizing one-population stats for selection simulations..."
for stat in nsl ihs delihh ihh12; do
    log_message "Normalizing ${stat}..."
    bash 09_normalize_onepop_stats.sh "${stat}"
    check_exit_status $? "09_normalize_onepop_stats.sh" "${stat}"
done
log_message "One-population statistics normalization complete."

# Step 15: Normalize XPEHH scores and calculate Max XPEHH (for SELECTION simulations)
log_message "Step 15: Normalizing XPEHH scores and calculating Max XPEHH for selection simulations (10_normalize_xpehh_max.sh)..."
bash 10_normalize_xpehh_max.sh
check_exit_status $? "10_normalize_xpehh_max.sh"
log_message "XPEHH normalization and Max XPEHH calculation complete."

# Step 16: Collate all statistics and finalize outputs
log_message "Step 16: Collating statistics and finalizing outputs (11_collate_and_finalize.sh)..."
bash 11_collate_and_finalize.sh
check_exit_status $? "11_collate_and_finalize.sh"
log_message "Collation and finalization complete."


log_message "All pipeline steps completed successfully!"
log_message "Main pipeline script finished."
exit 0