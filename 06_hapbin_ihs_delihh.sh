#!/bin/bash

# This script processes simulations (neutral or selection) to:
# 1. Run ihsbin for a target population.
# 2. Add DAF, position, and delihh to ihsbin outputs.

# Usage: ./06_ihs_processing.sh <neut|sel>

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

# --- Helper Functions (for host script) ---
log_message() {
    local type="$1"
    local message="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${type}] - ${message}"
}

# --- Host Configuration ---
CONFIG_FILE="00config.json"

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
ORIGINAL_TPED_INPUT_DIR="" # Where original TPEDs are (e.g., neutral_sims, selected_sims)
INPUT_CSV_BASENAME=""      # CSV listing completed sim_ids (from xpehh map/hap gen step)
PATH_PREFIX_FOR_FILES=""   # "neut" or "sel"
FINAL_OUTPUT_DIR_BASE="one_pop_stats" # Base for final output, e.g., one_pop_stats_neut

if [ "${SIM_TYPE}" == "neut" ]; then
    ORIGINAL_TPED_INPUT_DIR="neutral_sims"
    INPUT_CSV_BASENAME="xpehh.neut.map_hap_gen.runtime.csv" # From 05_hapbin_xpehh.sh
    PATH_PREFIX_FOR_FILES="neut"
    FINAL_OUTPUT_DIR="${FINAL_OUTPUT_DIR_BASE}_neut"
else # sel
    ORIGINAL_TPED_INPUT_DIR="selected_sims"
    INPUT_CSV_BASENAME="xpehh.sel.map_hap_gen.runtime.csv" # From 05_hapbin_xpehh.sh
    PATH_PREFIX_FOR_FILES="sel"
    FINAL_OUTPUT_DIR="${FINAL_OUTPUT_DIR_BASE}_sel"
fi

HAPBIN_DIR="hapbin" # Where .hap, .map from step 05 are, and intermediate ihs.out
RUNTIME_DIR="runtime"
DOCKER_IMAGE_IHS="docker.io/tx56/deepsweep_simulator:latest" # Assumed to have ihsbin, python, pandas
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
log_message "INFO" "Host Script (06_ihs_processing.sh for ${SIM_TYPE}) Started: $(date)"
log_message "INFO" "Reading configuration from ${CONFIG_FILE}"
log_message "INFO" "Target Population for IHS: ${TARGET_POP_FOR_IHS}"
log_message "INFO" "Processing SIM_TYPE: ${SIM_TYPE}"
log_message "INFO" "Original TPED Input Directory (for DAF): ${HOST_CWD}/${ORIGINAL_TPED_INPUT_DIR}"
log_message "INFO" "Hap/Map Input & Intermediate IHS Output Dir: ${HOST_CWD}/${HAPBIN_DIR}"
log_message "INFO" "Final IHS Output Directory: ${HOST_CWD}/${FINAL_OUTPUT_DIR}"


if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Configuration file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${HAPBIN_DIR}" # Ensure hapbin dir exists for intermediate ihs.out
mkdir -p "${FINAL_OUTPUT_DIR}" # Ensure final output dir exists
mkdir -p "${RUNTIME_DIR}"

INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV file '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
# xpehh map_hap_gen runtime CSV format: sim_id,<sim_id_val>,<path>_map_hap_gen_runtime,all_pops,<runtime>,seconds
# Sim ID is in the 2nd column.
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV file '${INPUT_CSV_FILE_HOST}' has no numeric sim_ids in 2nd col."; fi

if ! docker image inspect "$DOCKER_IMAGE_IHS" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_IHS} not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_IHS"; then log_message "ERROR" "Failed to pull Docker image ${DOCKER_IMAGE_IHS}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_IHS} already exists locally."
fi
log_message "INFO" "Starting Docker container for IHS processing (${SIM_TYPE})..."

# --- Docker Execution ---
docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_TARGET_POP_FOR_IHS="${TARGET_POP_FOR_IHS}" \
    -e CONTAINER_PATH_PREFIX="${PATH_PREFIX_FOR_FILES}" \
    -e CONTAINER_ORIGINAL_TPED_INPUT_DIR="${ORIGINAL_TPED_INPUT_DIR}" \
    -e CONTAINER_HAPBIN_DIR="${HAPBIN_DIR}" \
    -e CONTAINER_FINAL_OUTPUT_DIR="${FINAL_OUTPUT_DIR}" \
    -e CONTAINER_RUNTIME_DIR="${RUNTIME_DIR}" \
    -e CONTAINER_INPUT_CSV_BASENAME="${INPUT_CSV_BASENAME}" \
    -e PYTHON_IHS_POSTPROCESS_SCRIPT_NAME="${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}" \
    "$DOCKER_IMAGE_IHS" /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 

cleanup_and_exit() {
    log_container "Caught signal! Cleaning up temporary Python script..."
    rm -f "./${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}"
    log_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM

log_container "----------------------------------------------------"
log_container "Container Script (IHS Processing for ${CONTAINER_PATH_PREFIX}) Started: $(date)"
log_container "----------------------------------------------------"
# (Echo received ENV VARS - condensed for brevity)
log_container "Received TARGET_POP_FOR_IHS: [${CONTAINER_TARGET_POP_FOR_IHS}]"
log_container "Received PATH_PREFIX: [${CONTAINER_PATH_PREFIX}]"


# Create Python Postprocessing Script (07add_ihs_daf.py equivalent)
cat > "./${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}" <<'PYTHON_POST_EOF'
import os
import pandas as pd
import sys

def add_id_pos_and_daf_column(sim_id_str, pop1_target_str, path_prefix_str, original_tped_dir_str):
    # TPED file to get ID, pos, and DAF for the target population
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

    # Intermediate ihs.out file from ihsbin
    intermediate_ihs_file = f"{hapbin_dir_str}/{path_prefix_str}.hap.{sim_id_str}.ihs.out"
    
    if not os.path.exists(intermediate_ihs_file):
        print(f"Python IHS Post: ERROR: Intermediate IHS file {intermediate_ihs_file} not found.")
        return False
    
    try:
        # ihsbin output format: locID SNPid physPos freq allele0_iHH allele1_iHH norm_iHS
        # We need SNPid (col 1), Freq (col 3), iHH_0 (col 4), iHH_1 (col 5), iHS (col 6, after normalization)
        ihs_data_df = pd.read_csv(intermediate_ihs_file, sep='\\s+', header=None, 
                                  usecols=[1, 2, 3, 4, 5, 6], dtype=str) # Read as string first
        ihs_data_df.columns = ['ID', 'Freq', 'iHH_0', 'iHH_1', 'iHS_unstd', 'iHS'] # 'iHS' is col 6
    except Exception as e:
        print(f"Python IHS Post: Error reading intermediate IHS data from {intermediate_ihs_file}: {e}")
        return False

    # Convert necessary columns to numeric, ensure ID is string
    ihs_data_df['ID'] = ihs_data_df['ID'].astype(str)
    for col_to_numeric in ['Freq', 'iHH_0', 'iHH_1', 'iHS']: # iHS_unstd could also be converted if needed
        ihs_data_df[col_to_numeric] = pd.to_numeric(ihs_data_df[col_to_numeric], errors='coerce')
    
    ihs_data_df['delihh'] = ihs_data_df['iHH_1'] - ihs_data_df['iHH_0']
            
    merged_df = pd.merge(base_results_df, ihs_data_df, on='ID', how='inner')
    
    if merged_df.empty:
        print(f"Python IHS Post: WARNING: Merge for {intermediate_ihs_file} resulted in an empty DataFrame. Check 'ID' consistency.")
        # Not necessarily a failure of the whole script, but for this file.

    final_df = merged_df[['pos', 'daf', 'iHS', 'delihh']] # Select and order final columns
    
    # Final output path
    final_output_file = f"{final_out_dir_str}/{path_prefix_str}.{sim_id_str}_0_{pop1_target_str}.ihs.out"
    os.makedirs(final_out_dir_str, exist_ok=True) # Ensure final output directory exists

    try:
        final_df.to_csv(final_output_file, sep=' ', index=False, header=True)
        print(f"Python IHS Post: Created final IHS file: {final_output_file}")
    except Exception as e:
        print(f"Python IHS Post: Error writing final IHS data to {final_output_file}: {e}")
        return False
    return True

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(f"Usage: python {sys.argv[0]} <sim_id> <pop1_target> <path_prefix> <hapbin_dir> <final_out_dir> <original_tped_dir>")
        sys.exit(1)
    
    sim_id_arg = sys.argv[1]
    pop1_target_arg = sys.argv[2]
    path_prefix_arg = sys.argv[3]
    hapbin_dir_arg = sys.argv[4]
    final_out_dir_arg = sys.argv[5]
    original_tped_dir_arg = sys.argv[6]
    
    if main_ihs_postprocess(sim_id_arg, pop1_target_arg, path_prefix_arg, hapbin_dir_arg, final_out_dir_arg, original_tped_dir_arg):
        sys.exit(0)
    else:
        sys.exit(1)
PYTHON_POST_EOF
chmod +x "./${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}"
log_container "Python IHS Postprocessing script created."


INPUT_CSV_FILE_IN_CONTAINER="./${CONTAINER_RUNTIME_DIR}/${CONTAINER_INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_IN_CONTAINER}" ]; then
    log_container "CRITICAL ERROR: Input CSV file '${INPUT_CSV_FILE_IN_CONTAINER}' not found. Exiting."
    exit 1
fi

# --- Main IHS Processing Loop ---
log_container "Reading sim_ids from ${INPUT_CSV_FILE_IN_CONTAINER} for IHS processing..."
# xpehh.map_hap_gen.runtime.csv format: sim_id,<sim_id_val>,<path>_map_hap_gen_runtime,all_pops,<runtime>,seconds
# Sim_id value is in the second column ($2)
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_IN_CONTAINER}" | sort -un)

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_container "No valid sim_ids found in ${INPUT_CSV_FILE_IN_CONTAINER}. Nothing to process for IHS."
else
    log_container "Found unique sim_ids for IHS: ${sim_ids_to_run[*]}"
    for current_sim_id in "${sim_ids_to_run[@]}"; do
        log_container "--- Starting IHS pipeline for sim_id: ${current_sim_id}, target_pop: ${CONTAINER_TARGET_POP_FOR_IHS} ---"
        
        # Define file paths for this sim_id and target_pop
        hap_file_path="./${CONTAINER_HAPBIN_DIR}/${CONTAINER_PATH_PREFIX}.${current_sim_id}_0_${CONTAINER_TARGET_POP_FOR_IHS}.hap"
        map_file_path="./${CONTAINER_HAPBIN_DIR}/${CONTAINER_PATH_PREFIX}.${current_sim_id}_0_${CONTAINER_TARGET_POP_FOR_IHS}.map"
        # ihsbin output is intermediate, stored in hapbin_dir
        intermediate_ihs_out_file="./${CONTAINER_HAPBIN_DIR}/${CONTAINER_PATH_PREFIX}.hap.${current_sim_id}.ihs.out"


        if [ ! -f "${hap_file_path}" ]; then log_container "ERROR: .hap file ${hap_file_path} not found! Skipping IHS for sim ${current_sim_id}."; continue; fi
        if [ ! -f "${map_file_path}" ]; then log_container "ERROR: .map file ${map_file_path} not found! Skipping IHS for sim ${current_sim_id}."; continue; fi

        # 1. Run ihsbin
        log_container "Step 1: Running ihsbin for sim_id ${current_sim_id}, pop ${CONTAINER_TARGET_POP_FOR_IHS}..."
        ihsbin_cmd="ihsbin --hap ${hap_file_path} --map ${map_file_path} --out ${intermediate_ihs_out_file}"
        log_container "Executing: ${ihsbin_cmd}"
        
        ihs_start_time=$(date +%s)
        ${ihsbin_cmd}
        ihs_exit_status=$?
        ihs_end_time=$(date +%s)
        ihs_runtime=$((ihs_end_time - ihs_start_time))

        if [ $ihs_exit_status -eq 0 ] && [ -f "${intermediate_ihs_out_file}" ]; then
            log_container "ihsbin for sim ${current_sim_id}, pop ${CONTAINER_TARGET_POP_FOR_IHS} completed. Runtime: ${ihs_runtime}s"
            echo "sim_id,${current_sim_id},pop_id,${CONTAINER_TARGET_POP_FOR_IHS},ihs_raw_runtime,${ihs_runtime},seconds" >> "./${CONTAINER_RUNTIME_DIR}/ihs.${CONTAINER_PATH_PREFIX}.runtime.csv"
        else
            log_container "ERROR: ihsbin failed for sim ${current_sim_id}, pop ${CONTAINER_TARGET_POP_FOR_IHS} (exit status ${ihs_exit_status}). Skipping DAF addition."
            continue # Skip to next sim_id if ihsbin failed
        fi
        log_container "Step 1 completed for sim_id ${current_sim_id}."

        # 2. Add DAF, pos, and calculate delihh
        log_container "Step 2: Adding DAF/pos/delihh to IHS output for sim_id ${current_sim_id}..."
        python_post_start_time=$(date +%s)
        python "./${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}" \
            "${current_sim_id}" \
            "${CONTAINER_TARGET_POP_FOR_IHS}" \
            "${CONTAINER_PATH_PREFIX}" \
            "./${CONTAINER_HAPBIN_DIR}" \
            "./${CONTAINER_FINAL_OUTPUT_DIR}" \
            "./${CONTAINER_ORIGINAL_TPED_INPUT_DIR}"
        
        python_exit_status=$?
        python_post_end_time=$(date +%s)
        python_post_runtime=$((python_post_end_time - python_post_start_time))

        if [ $python_exit_status -ne 0 ]; then
            log_container "ERROR: Python IHS postprocessing (add DAF/pos) failed for sim ${current_sim_id}."
            # Optionally record this failure in a more specific way
            echo "sim_id,${current_sim_id},pop_id,${CONTAINER_TARGET_POP_FOR_IHS},ihs_daf_add_status,failed_python_exit_${python_exit_status}" >> "./${CONTAINER_RUNTIME_DIR}/ihs.${CONTAINER_PATH_PREFIX}.runtime.csv"
        else
            log_container "DAF/pos/delihh addition completed for sim ${current_sim_id}. Runtime: ${python_post_runtime}s"
            # Append to the main ihs runtime, or have a separate one for this step
            echo "sim_id,${current_sim_id},pop_id,${CONTAINER_TARGET_POP_FOR_IHS},ihs_daf_add_runtime,${python_post_runtime},seconds" >> "./${CONTAINER_RUNTIME_DIR}/ihs.${CONTAINER_PATH_PREFIX}.runtime.csv"
        fi
        log_container "Step 2 completed for sim_id ${current_sim_id}."

        log_container "--- Finished IHS pipeline for sim_id: ${current_sim_id} ---"
    done
    log_container "All sim_ids from CSV processed by IHS pipeline."
fi

rm -f "./${PYTHON_IHS_POSTPROCESS_SCRIPT_NAME}"
log_container "Python IHS helper script removed."

log_container "IHS processing for ${CONTAINER_PATH_PREFIX} simulations finished."
log_container "----------------------------------------------------"
log_container "Container Script (IHS Processing for ${CONTAINER_PATH_PREFIX}) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
log_message "INFO" "Docker container (IHS for ${SIM_TYPE}) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then log_message "INFO" "Script (IHS for ${SIM_TYPE}) likely interrupted by user (Ctrl+C)."; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then log_message "ERROR" "Docker container (IHS for ${SIM_TYPE}) reported an error."; fi
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (06_ihs_processing.sh for ${SIM_TYPE}) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"
