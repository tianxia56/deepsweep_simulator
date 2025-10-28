#!/bin/bash

# --- Bulletproof Bash and Conda Activation ---
# 1. Ensure the script is running with Bash, not sh/dash
if [ -z "$BASH_VERSION" ]; then
    # Re-execute the script with the same arguments using bash.
    echo "Re-executing with /bin/bash..." >&2
    exec /bin/bash "$0" "$@"
fi

# 2. Manually find and activate the Conda environment by manipulating the PATH.
ENV_NAME="deepsweep_simulator"
echo "Attempting to set PATH for Conda environment: ${ENV_NAME}" >&2
CONDA_ENV_PATH=$(conda info --envs | grep -E "^${ENV_NAME}\s" | awk '{print $NF}')
if [ -z "${CONDA_ENV_PATH}" ]; then
    echo "ERROR: Could not find Conda environment path for '${ENV_NAME}'." >&2
    exit 1
fi
if [ ! -d "${CONDA_ENV_PATH}/bin" ]; then
    echo "ERROR: bin directory not found in '${CONDA_ENV_PATH}'." >&2
    exit 1
fi
export PATH="${CONDA_ENV_PATH}/bin:${PATH}"
echo "Successfully set PATH for Conda environment." >&2
# --- End of Activation Block ---

# This script processes simulations (neutral or selection) to:
# 1. Run ihsbin for a target population locally.
# 2. Add DAF, position, and delihh to ihsbin outputs using a Python helper script.
#
# This version runs all commands directly on the host system without Docker.

# Usage: ./06_ihs_processing.sh <neut|sel>

set -e # Exit immediately if a command exits with a non-zero status.
set -o pipefail # Fail a pipeline if any command fails.

# --- Python Helper Script Name ---
PYTHON_IHS_POSTPROCESS_SCRIPT_NAME="ihs_postprocess_core.py"

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
LOG_FILE="${LOG_DIR}/ihs_processing_${SIM_TYPE}.log"

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Helper Functions ---
log_message() {
    local type="$1"
    local message="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${type}] - ${message}"
}

# --- Configuration & Path Setup ---
CONFIG_FILE="00config.json"

log_message "INFO" "Host Script (06_ihs_processing.sh for ${SIM_TYPE}) Started: $(date)"

# --- Local Dependency Checks ---
log_message "INFO" "Checking for local dependencies..."
log_message "DEBUG" "Python interpreter being used by script is: $(which python3)"
if ! command -v ihsbin &> /dev/null; then
    log_message "ERROR" "'ihsbin' command not found. Please ensure it is installed and in your system's PATH."
    exit 1
fi
if ! command -v python3 &> /dev/null; then
    log_message "ERROR" "'python3' command not found. Python 3 is required for post-processing."
    exit 1
fi
if ! python3 -c "import pandas" &> /dev/null; then
    log_message "ERROR" "Python 'pandas' library not found. The Conda env may not be activated correctly."
    exit 1
fi
log_message "INFO" "All local dependencies found."

# --- Host Configuration ---
if ! command -v jq &> /dev/null; then
    log_message "WARNING_JQ" "jq command not found. Python fallback will be attempted."
    TARGET_POP_FOR_IHS=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1]))['selected_pop'])" "${CONFIG_FILE}" 2>/dev/null)
    if [ $? -ne 0 ] || [ -z "$TARGET_POP_FOR_IHS" ] || [ "$TARGET_POP_FOR_IHS" = "null" ]; then
        log_message "ERROR_CONFIG" "Failed to read 'selected_pop' using Python fallback from ${CONFIG_FILE}."
        exit 1
    fi
    log_message "INFO" "Read config using Python fallback. jq is highly recommended."
else
    TARGET_POP_FOR_IHS=$(jq -r '.selected_pop' "${CONFIG_FILE}")
fi

if [ -z "$TARGET_POP_FOR_IHS" ] || [ "$TARGET_POP_FOR_IHS" = "null" ]; then
    log_message "ERROR_CONFIG" "'selected_pop' not found or null in ${CONFIG_FILE}."
    exit 1
fi

# Type-specific configurations
ORIGINAL_TPED_INPUT_DIR=""
INPUT_CSV_BASENAME=""
PATH_PREFIX_FOR_FILES=""
FINAL_OUTPUT_DIR_BASE="one_pop_stats"

if [ "${SIM_TYPE}" == "neut" ]; then
    ORIGINAL_TPED_INPUT_DIR="neutral_sims"
    INPUT_CSV_BASENAME="xpehh.neut.map_hap_gen.runtime.csv"
    PATH_PREFIX_FOR_FILES="neut"
    FINAL_OUTPUT_DIR="${FINAL_OUTPUT_DIR_BASE}_neut"
else # sel
    ORIGINAL_TPED_INPUT_DIR="selected_sims"
    INPUT_CSV_BASENAME="xpehh.sel.map_hap_gen.runtime.csv"
    PATH_PREFIX_FOR_FILES="sel"
    FINAL_OUTPUT_DIR="${FINAL_OUTPUT_DIR_BASE}_sel"
fi

HAPBIN_DIR="hapbin"
RUNTIME_DIR="runtime"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
log_message "INFO" "Reading configuration from ${CONFIG_FILE}"
log_message "INFO" "Target Population for IHS: ${TARGET_POP_FOR_IHS}"
log_message "INFO" "Processing SIM_TYPE: ${SIM_TYPE}"
log_message "INFO" "Original TPED Input Dir: ${HOST_CWD}/${ORIGINAL_TPED_INPUT_DIR}"
log_message "INFO" "Hap/Map Input & Intermediate IHS Output Dir: ${HOST_CWD}/${HAPBIN_DIR}"
log_message "INFO" "Final IHS Output Dir: ${HOST_CWD}/${FINAL_OUTPUT_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Configuration file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${HAPBIN_DIR}"
mkdir -p "${FINAL_OUTPUT_DIR}"
mkdir -p "${RUNTIME_DIR}"

INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV file '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi

# --- Main Logic ---

# Cleanup function to remove the temporary python script on exit
cleanup_and_exit() {
    log_message "INFO" "Caught signal or script ending. Cleaning up temporary Python script..."
    rm -f "${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}"
    log_message "INFO" "Exiting."
}
trap cleanup_and_exit INT TERM EXIT

# Create Python Postprocessing Script (omitted for brevity, it's correct)
log_message "INFO" "Creating Python helper script: ${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}"
cat > "${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}" <<'PYTHON_POST_EOF'
import os
import pandas as pd
import sys

def add_id_pos_and_daf_column(sim_id_str, pop1_target_str, path_prefix_str, original_tped_dir_str):
    tped_file_for_daf = f"{original_tped_dir_str}/{path_prefix_str}.hap.{sim_id_str}_0_{pop1_target_str}.tped"
    if not os.path.exists(tped_file_for_daf):
        print(f"Python IHS Post: ERROR: TPED file for DAF {tped_file_for_daf} not found.")
        return None
    try:
        tped_data_ids_pos = pd.read_csv(tped_file_for_daf, sep='\\s+', usecols=[1, 3], header=None, dtype=str)
        tped_data_ids_pos.columns = ['ID', 'pos']
    except Exception as e:
        print(f"Python IHS Post: Error reading ID/pos from {tped_file_for_daf}: {e}")
        return None
    try:
        with open(tped_file_for_daf, 'r') as f:
            first_line = f.readline()
            num_cols_total = len(first_line.split())
        if num_cols_total < 5:
            print(f"Python IHS Post: Error: Not enough columns in {tped_file_for_daf} for allele data.")
            return None
        allele_cols_indices = list(range(4, num_cols_total))
        pop_allele_data = pd.read_csv(tped_file_for_daf, sep='\\s+', usecols=allele_cols_indices, header=None)
    except Exception as e:
        print(f"Python IHS Post: Error reading allele data from {tped_file_for_daf}: {e}")
        return None
    daf = pop_allele_data.apply(pd.to_numeric, errors='coerce').mean(axis=1)
    results_df = pd.DataFrame({'ID': tped_data_ids_pos['ID'], 'pos': tped_data_ids_pos['pos'], 'daf': daf})
    results_df['ID'] = results_df['ID'].astype(str)
    return results_df

def main_ihs_postprocess(sim_id_str, pop1_target_str, path_prefix_str, hapbin_dir_str, final_out_dir_str, original_tped_dir_str):
    print(f"Python IHS Post: Postprocessing IHS for sim {sim_id_str}, pop {pop1_target_str}")
    base_results_df = add_id_pos_and_daf_column(sim_id_str, pop1_target_str, path_prefix_str, original_tped_dir_str)
    if base_results_df is None:
        print(f"Python IHS Post: Failed to get base ID/pos/DAF data for sim {sim_id_str}. Aborting postprocessing.")
        return False
    intermediate_ihs_file = f"{hapbin_dir_str}/{path_prefix_str}.hap.{sim_id_str}.ihs.out"
    if not os.path.exists(intermediate_ihs_file):
        print(f"Python IHS Post: ERROR: Intermediate IHS file {intermediate_ihs_file} not found.")
        return False
    try:
        ihs_data_df = pd.read_csv(intermediate_ihs_file, sep='\\s+', header=None, usecols=[1, 2, 3, 4, 5, 6], dtype=str)
        ihs_data_df.columns = ['ID', 'Freq', 'iHH_0', 'iHH_1', 'iHS_unstd', 'iHS']
    except Exception as e:
        print(f"Python IHS Post: Error reading intermediate IHS data from {intermediate_ihs_file}: {e}")
        return False
    ihs_data_df['ID'] = ihs_data_df['ID'].astype(str)
    for col_to_numeric in ['Freq', 'iHH_0', 'iHH_1', 'iHS']:
        ihs_data_df[col_to_numeric] = pd.to_numeric(ihs_data_df[col_to_numeric], errors='coerce')
    ihs_data_df['delihh'] = ihs_data_df['iHH_1'] - ihs_data_df['iHH_0']
    merged_df = pd.merge(base_results_df, ihs_data_df, on='ID', how='inner')
    if merged_df.empty:
        print(f"Python IHS Post: WARNING: Merge for {intermediate_ihs_file} resulted in an empty DataFrame. Check 'ID' consistency.")
    if merged_df.empty:
        print(f"Python IHS Post: WARNING: Final DataFrame is empty. No output file will be created for sim {sim_id_str}.")
        return True # Not a fatal error, just no output
    final_df = merged_df[['pos', 'daf', 'iHS', 'delihh']]
    final_output_file = f"{final_out_dir_str}/{path_prefix_str}.{sim_id_str}_0_{pop1_target_str}.ihs.out"
    os.makedirs(final_out_dir_str, exist_ok=True)
    try:
        final_df.to_csv(final_output_file, sep=' ', index=False, header=True)
        print(f"Python IHS Post: SUCCESS: Created final IHS file with {len(final_df)} rows: {final_output_file}")
    except Exception as e:
        print(f"Python IHS Post: Error writing final IHS data to {final_output_file}: {e}")
        return False
    return True

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(f"Usage: python {sys.argv[0]} <sim_id> <pop1_target> <path_prefix> <hapbin_dir> <final_out_dir> <original_tped_dir>")
        sys.exit(1)
    sim_id_arg, pop1_target_arg, path_prefix_arg, hapbin_dir_arg, final_out_dir_arg, original_tped_dir_arg = sys.argv[1:7]
    if main_ihs_postprocess(sim_id_arg, pop1_target_arg, path_prefix_arg, hapbin_dir_arg, final_out_dir_arg, original_tped_dir_arg): sys.exit(0)
    else: sys.exit(1)
PYTHON_POST_EOF
chmod +x "${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}"

# --- Main IHS Processing Loop ---
log_message "INFO" "Starting main processing loop..."
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_HOST}" | sort -un)

# --- ADDED DEBUG BLOCK ---
log_message "DEBUG" "Reading sim_ids from: ${INPUT_CSV_FILE_HOST}"
log_message "DEBUG" "----------------- CSV Content Start -----------------"
cat "${INPUT_CSV_FILE_HOST}"
log_message "DEBUG" "------------------ CSV Content End ------------------"
log_message "DEBUG" "Sim IDs extracted by awk: [$(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_HOST}" | sort -un | tr '\n' ' ')]"
log_message "DEBUG" "Total sim_ids found in array: ${#sim_ids_to_run[@]}"
# --- END DEBUG BLOCK ---

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_message "WARNING" "No valid sim_ids found in ${INPUT_CSV_FILE_HOST}. Nothing to process for IHS. Script will exit successfully."
else
    log_message "INFO" "Found unique sim_ids for IHS: ${sim_ids_to_run[*]}"
    for current_sim_id in "${sim_ids_to_run[@]}"; do
        log_message "INFO" "--- Starting IHS pipeline for sim_id: ${current_sim_id}, target_pop: ${TARGET_POP_FOR_IHS} ---"
        
        hap_file_path="${HAPBIN_DIR}/${PATH_PREFIX_FOR_FILES}.${current_sim_id}_0_${TARGET_POP_FOR_IHS}.hap"
        map_file_path="${HAPBIN_DIR}/${PATH_PREFIX_FOR_FILES}.${current_sim_id}_0_${TARGET_POP_FOR_IHS}.map"
        intermediate_ihs_out_file="${HAPBIN_DIR}/${PATH_PREFIX_FOR_FILES}.hap.${current_sim_id}.ihs.out"
        ihsbin_stderr_log="${LOG_DIR}/ihsbin_${SIM_TYPE}_${current_sim_id}.stderr.log"

        if [ ! -f "${hap_file_path}" ]; then log_message "ERROR" ".hap file ${hap_file_path} not found! Skipping sim ${current_sim_id}."; continue; fi
        if [ ! -f "${map_file_path}" ]; then log_message "ERROR" ".map file ${map_file_path} not found! Skipping sim ${current_sim_id}."; continue; fi

        log_message "INFO" "Step 1: Running ihsbin for sim_id ${current_sim_id}, pop ${TARGET_POP_FOR_IHS}..."
        ihsbin_cmd="ihsbin --hap ${hap_file_path} --map ${map_file_path} --out ${intermediate_ihs_out_file}"
        log_message "INFO" "Executing: ${ihsbin_cmd}"
        
        # --- MODIFIED: Capture stderr and check exit code robustly ---
        ihs_start_time=$(date +%s)
        { ${ihsbin_cmd} 2> "${ihsbin_stderr_log}"; }
        ihs_exit_status=$?
        ihs_end_time=$(date +%s)
        ihs_runtime=$((ihs_end_time - ihs_start_time))

        if [ $ihs_exit_status -eq 0 ] && [ -f "${intermediate_ihs_out_file}" ]; then
            log_message "INFO" "ihsbin for sim ${current_sim_id} completed successfully. Runtime: ${ihs_runtime}s"
            echo "sim_id,${current_sim_id},pop_id,${TARGET_POP_FOR_IHS},ihs_raw_runtime,${ihs_runtime},seconds" >> "${RUNTIME_DIR}/ihs.${PATH_PREFIX_FOR_FILES}.runtime.csv"
            # Log any potential warnings from stderr even on success
            if [ -s "${ihsbin_stderr_log}" ]; then
                log_message "WARNING" "ihsbin produced output on stderr even though it succeeded. Content of ${ihsbin_stderr_log}:"
                cat "${ihsbin_stderr_log}"
            fi
        else
            log_message "ERROR" "ihsbin FAILED for sim ${current_sim_id} with exit status ${ihs_exit_status}. Skipping DAF addition."
            if [ -f "${ihsbin_stderr_log}" ]; then
                log_message "ERROR" "Content of ihsbin stderr log (${ihsbin_stderr_log}):"
                cat "${ihsbin_stderr_log}"
            else
                log_message "ERROR" "No stderr log was captured for the failed ihsbin run."
            fi
            # Since we have `set -e`, the script would exit here, but `continue` is safer if you remove it.
            exit 1
        fi
        log_message "INFO" "Step 1 completed for sim_id ${current_sim_id}."

        log_message "INFO" "Step 2: Adding DAF/pos/delihh to IHS output..."
        python3 "./${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}" \
            "${current_sim_id}" \
            "${TARGET_POP_FOR_IHS}" \
            "${PATH_PREFIX_FOR_FILES}" \
            "${HAPBIN_DIR}" \
            "${FINAL_OUTPUT_DIR}" \
            "${ORIGINAL_TPED_INPUT_DIR}"
        # The python script will exit 1 on failure, and set -e will catch it.
        
        log_message "INFO" "Step 2 completed for sim_id ${current_sim_id}."
        log_message "INFO" "--- Finished IHS pipeline for sim_id: ${current_sim_id} ---"
    done
    log_message "INFO" "All sim_ids from CSV processed by IHS pipeline."
fi

log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (06_ihs_processing.sh for ${SIM_TYPE}) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"