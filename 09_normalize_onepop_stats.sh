#!/bin/bash

# This script normalizes single-population statistics (nSL, iHS, delihh, iHH12)
# for SELECTION simulations using pre-computed normalization bins.

# Usage: ./09_normalize_onepop_stats.sh <stat_type>
# <stat_type> can be: nsl, ihs, delihh, ihh12

# --- Script Argument Validation ---
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <nsl|ihs|delihh|ihh12>"
    exit 1
fi
STAT_TYPE="$1"

# --- Python Helper Script Name ---
PYTHON_NORM_SCRIPT_NAME="normalize_onepop_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/normalize_onepop_${STAT_TYPE}.log"

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Helper Functions (for host script) ---
# Moved log_message definition to the top
log_message() {
    local type="$1"
    local message="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${type}] - ${message}"
}

# Validate STAT_TYPE after log_message is defined
case "$STAT_TYPE" in
    nsl|ihs|delihh|ihh12)
        log_message "INFO" "Statistic type to normalize: ${STAT_TYPE}"
        ;;
    *)
        log_message "ERROR" "Invalid statistic type: ${STAT_TYPE}. Must be one of nsl, ihs, delihh, ihh12."
        exit 1
        ;;
esac

# --- Host Configuration ---
CONFIG_FILE="00config.json"

if ! command -v jq &> /dev/null; then
    log_message "WARNING_JQ" "jq command not found. Python fallback will be attempted."
    POP1_REF=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1]))['selected_pop'])" "${CONFIG_FILE}" 2>/dev/null)
    if [ $? -ne 0 ] || [ -z "$POP1_REF" ] || [ "$POP1_REF" = "null" ]; then
        log_message "ERROR_CONFIG" "Failed to read 'selected_pop' using Python fallback from ${CONFIG_FILE}."
        exit 1
    fi
    log_message "INFO" "Read config using Python fallback. jq is highly recommended."
else
    POP1_REF=$(jq -r '.selected_pop' "${CONFIG_FILE}")
fi

if [ -z "$POP1_REF" ] || [ "$POP1_REF" = "null" ]; then log_message "ERROR_CONFIG" "'selected_pop' not found or null in ${CONFIG_FILE}."; exit 1; fi

ONEPOP_STATS_SEL_DIR="one_pop_stats_sel" 
BIN_DIR="bin"                            
NORM_OUTPUT_DIR="norm"                   
RUNTIME_DIR="runtime"
INPUT_CSV_FOR_SIM_IDS="nsl.sel.runtime.csv" 

DOCKER_IMAGE_PYNORM="docker.io/tx56/deepsweep_simulator:latest" 
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
log_message "INFO" "Host Script (09_normalize_onepop_stats.sh for ${STAT_TYPE}) Started: $(date)"
log_message "INFO" "Reference Pop (pop1): ${POP1_REF}"
log_message "INFO" "Input stats from: ${ONEPOP_STATS_SEL_DIR}, Bin files from: ${BIN_DIR}, Output to: ${NORM_OUTPUT_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Config file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${NORM_OUTPUT_DIR}"
mkdir -p "${RUNTIME_DIR}" 

INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_FOR_SIM_IDS}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV for sim_ids '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV '${INPUT_CSV_FILE_HOST}' has no numeric sim_ids in 2nd col."; fi

REQUIRED_BIN_FILE="${HOST_CWD}/${BIN_DIR}/${STAT_TYPE}_bin.csv"
if [ ! -f "${REQUIRED_BIN_FILE}" ]; then
    log_message "ERROR" "Required normalization bin file '${REQUIRED_BIN_FILE}' not found. Ensure 08_create_norm_bins.sh ran successfully for this statistic."
    exit 1
fi
log_message "INFO" "Found required normalization bin file: ${REQUIRED_BIN_FILE}"


if ! docker image inspect "$DOCKER_IMAGE_PYNORM" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PYNORM} not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_PYNORM"; then log_message "ERROR" "Failed to pull Docker image ${DOCKER_IMAGE_PYNORM}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PYNORM} already exists locally."
fi
log_message "INFO" "Starting Docker container for normalizing ${STAT_TYPE}..."

docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_POP1_REF="${POP1_REF}" \
    -e CONTAINER_STAT_TYPE="${STAT_TYPE}" \
    -e CONTAINER_ONEPOP_STATS_SEL_DIR="${ONEPOP_STATS_SEL_DIR}" \
    -e CONTAINER_BIN_DIR="${BIN_DIR}" \
    -e CONTAINER_NORM_OUTPUT_DIR="${NORM_OUTPUT_DIR}" \
    -e CONTAINER_RUNTIME_DIR="${RUNTIME_DIR}" \
    -e CONTAINER_INPUT_CSV_FOR_SIM_IDS="${INPUT_CSV_FOR_SIM_IDS}" \
    -e PYTHON_NORM_SCRIPT_NAME="${PYTHON_NORM_SCRIPT_NAME}" \
    "$DOCKER_IMAGE_PYNORM" /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 

cleanup_and_exit() {
    log_container "Caught signal! Cleaning up temporary Python script..."
    rm -f "./${PYTHON_NORM_SCRIPT_NAME}"
    log_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM
log_container "----------------------------------------------------"
log_container "Container Script (Normalize One-Pop Stats for ${CONTAINER_STAT_TYPE}) Started: $(date)"
log_container "----------------------------------------------------"
log_container "Received STAT_TYPE: [${CONTAINER_STAT_TYPE}]"
log_container "Received POP1_REF: [${CONTAINER_POP1_REF}]"

cat > "./${PYTHON_NORM_SCRIPT_NAME}" <<'PYTHON_SCRIPT_EOF'
import sys
import os
import pandas as pd
import numpy as np 

def find_closest_bin_idx(daf_value, bin_daf_max_values):
    if pd.isna(daf_value):
        return -1 
    bin_daf_max_values_np = np.asarray(bin_daf_max_values)
    differences = np.abs(bin_daf_max_values_np - daf_value)
    closest_idx = np.argmin(differences)
    return closest_idx

def normalize_statistic(sim_id, pop1, stat_type, onepop_stats_dir, bin_dir, norm_output_dir):
    print(f"Python: Normalizing {stat_type} for sim_id {sim_id}, pop1 {pop1}")
    stat_col_name_in_file = "" # Original name in file
    stat_col_name_standard = "stat_value" # Standard name we'll use internally
    norm_stat_col_name = ""
    input_file_path = ""
    bin_file_path = os.path.join(bin_dir, f"{stat_type}_bin.csv")
    output_file_path = os.path.join(norm_output_dir, f"temp.{stat_type}.{sim_id}.tsv")
    
    # Define how to read each statistic file
    # cols_to_use_indices: 0-based indices of columns to ACTUALLY USE after reading
    # col_names_assigned: names to assign if header=None, or to check for if header=0
    # final_cols_map: mapping from original/assigned names to 'pos', 'daf', 'stat_value'
    
    read_params = {}

    if stat_type == "nsl":
        input_file_path = os.path.join(onepop_stats_dir, f"sel.{sim_id}_pop{pop1}.nsl.out")
        norm_stat_col_name = "norm_nsl"
        read_params = {
            "header": None, 
            "names": ['id_col','pos','daf','s1','s0','stat_value'], # nsl_value is the 6th col
            "usecols": ['pos','daf','stat_value'] # Select these after assigning names
        }
    elif stat_type == "ihs":
        input_file_path = os.path.join(onepop_stats_dir, f"sel.{sim_id}_0_{pop1}.ihs.out")
        norm_stat_col_name = "norm_ihs"
        read_params = { # This file has header: pos daf iHS delihh
            "header": 0, 
            "usecols": ['pos', 'daf', 'iHS'], 
            "rename_map": {'iHS': 'stat_value'}
        }
    elif stat_type == "delihh":
        input_file_path = os.path.join(onepop_stats_dir, f"sel.{sim_id}_0_{pop1}.ihs.out")
        norm_stat_col_name = "norm_delihh"
        read_params = { # This file has header: pos daf iHS delihh
            "header": 0,
            "usecols": ['pos', 'daf', 'delihh'],
            "rename_map": {'delihh': 'stat_value'}
        }
    elif stat_type == "ihh12":
        input_file_path = os.path.join(onepop_stats_dir, f"sel.{sim_id}_pop{pop1}.ihh12.out")
        norm_stat_col_name = "norm_ihh12"
        read_params = { # Header: id pos p1 ihh12
            "header": 0,
            "usecols": ['pos', 'p1', 'ihh12'],
            "rename_map": {'p1': 'daf', 'ihh12': 'stat_value'}
        }
    else:
        print(f"Python: Unknown statistic type: {stat_type}")
        return False

    if not os.path.exists(input_file_path):
        print(f"Python: Input statistic file not found: {input_file_path}")
        return False
    if not os.path.exists(bin_file_path):
        print(f"Python: Normalization bin file not found: {bin_file_path}")
        return False

    try:
        df_list = []
        # Chunked reading for potentially large files, though selscan outputs are often manageable
        for chunk in pd.read_csv(input_file_path, sep=r'\s+', header=read_params.get("header"), 
                                 names=read_params.get("names"), comment='#', chunksize=100000, low_memory=False):
            # Select and rename columns
            if "usecols" in read_params:
                chunk = chunk[read_params["usecols"]]
            if "rename_map" in read_params:
                chunk = chunk.rename(columns=read_params["rename_map"])
            df_list.append(chunk)
        
        if not df_list: # File was empty or only comments
            data_df = pd.DataFrame(columns=['pos', 'daf', 'stat_value'])
        else:
            data_df = pd.concat(df_list, ignore_index=True)

        # Ensure standard column names 'pos', 'daf', 'stat_value' exist
        if not all(col in data_df.columns for col in ['pos', 'daf', 'stat_value']):
            print(f"Python: ERROR - Standard columns ('pos', 'daf', 'stat_value') not found after processing {input_file_path}. Actual columns: {data_df.columns.tolist()}")
            return False

        data_df['daf'] = pd.to_numeric(data_df['daf'], errors='coerce')
        data_df['stat_value'] = pd.to_numeric(data_df['stat_value'], errors='coerce')
        data_df.dropna(subset=['daf', 'stat_value'], inplace=True)

    except Exception as e:
        print(f"Python: Error reading or processing input file {input_file_path}: {e}")
        return False

    if data_df.empty:
        print(f"Python: No valid data after reading/processing {input_file_path} for stat {stat_type}.")
        pd.DataFrame(columns=['pos', 'daf', norm_stat_col_name]).to_csv(output_file_path, sep="\t", index=False, header=True, na_rep="NA")
        return True

    try:
        norm_data_df = pd.read_csv(bin_file_path, header=0)
        norm_data_df.rename(columns={'bin_daf_max': 'bin_daf_max', 'mean_stat': 'mean', 'std_stat': 'std'}, inplace=True)
        norm_data_df['bin_daf_max'] = pd.to_numeric(norm_data_df['bin_daf_max'], errors='coerce')
        norm_data_df['mean'] = pd.to_numeric(norm_data_df['mean'], errors='coerce')
        norm_data_df['std'] = pd.to_numeric(norm_data_df['std'], errors='coerce')
        norm_data_df.dropna(inplace=True)
    except Exception as e:
        print(f"Python: Error reading or processing normalization bin file {bin_file_path}: {e}")
        return False

    if norm_data_df.empty:
        print(f"Python: Normalization bin file {bin_file_path} is empty or has no valid data.")
        return False

    normalized_values = []
    for index, row in data_df.iterrows():
        daf_val = row['daf']; stat_val = row['stat_value']
        if pd.isna(daf_val) or pd.isna(stat_val): normalized_values.append(np.nan); continue
        closest_bin_idx = find_closest_bin_idx(daf_val, norm_data_df['bin_daf_max'])
        if closest_bin_idx == -1 : normalized_values.append(np.nan); continue
        bin_info = norm_data_df.iloc[closest_bin_idx]
        bin_mean = bin_info['mean']; bin_std = bin_info['std']
        if pd.isna(bin_mean) or pd.isna(bin_std) or bin_std == 0: normalized_values.append(np.nan)
        else: norm_val = (stat_val - bin_mean) / bin_std; normalized_values.append(round(norm_val, 4))
    data_df[norm_stat_col_name] = normalized_values
    output_df = data_df[['pos', 'daf', norm_stat_col_name]]
    os.makedirs(norm_output_dir, exist_ok=True)
    output_df.to_csv(output_file_path, sep="\t", index=False, header=True, na_rep="NA")
    print(f"Python: Normalized {stat_type} data saved to {output_file_path}")
    return True

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: python <script_name>.py <sim_id> <pop1_ref> <stat_type> <onepop_stats_dir> <bin_dir> <norm_output_dir>")
        sys.exit(1)
    sim_id_arg, pop1_ref_arg, stat_type_arg, onepop_stats_dir_arg, bin_dir_arg, norm_output_dir_arg = sys.argv[1:7]
    if normalize_statistic(sim_id_arg, pop1_ref_arg, stat_type_arg, onepop_stats_dir_arg, bin_dir_arg, norm_output_dir_arg): sys.exit(0)
    else: sys.exit(1)
PYTHON_SCRIPT_EOF
chmod +x "./${PYTHON_NORM_SCRIPT_NAME}"
log_container "Python One-Pop Stats Normalization script created."

INPUT_CSV_FILE_IN_CONTAINER="./${CONTAINER_RUNTIME_DIR}/${CONTAINER_INPUT_CSV_FOR_SIM_IDS}"
if [ ! -f "${INPUT_CSV_FILE_IN_CONTAINER}" ]; then log_container "CRITICAL ERROR: Input CSV file '${INPUT_CSV_FILE_IN_CONTAINER}' not found. Exiting."; exit 1; fi
log_container "Reading all sim_ids from ${INPUT_CSV_FILE_IN_CONTAINER} for normalizing ${CONTAINER_STAT_TYPE}..."
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_IN_CONTAINER}" | sort -un)
if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_container "No valid sim_ids found in ${INPUT_CSV_FILE_IN_CONTAINER}. Nothing to process."
else
    log_container "Found unique sim_ids for normalization: ${sim_ids_to_run[*]}"
    overall_start_time=$(date +%s)
    for current_sim_id in "${sim_ids_to_run[@]}"; do
        log_container "--- Normalizing ${CONTAINER_STAT_TYPE} for sim_id: ${current_sim_id} ---"
        python3 "./${PYTHON_NORM_SCRIPT_NAME}" "${current_sim_id}" "${CONTAINER_POP1_REF}" "${CONTAINER_STAT_TYPE}" "./${CONTAINER_ONEPOP_STATS_SEL_DIR}" "./${CONTAINER_BIN_DIR}" "./${CONTAINER_NORM_OUTPUT_DIR}"
        python_exit_status=$?
        if [ $python_exit_status -eq 0 ]; then log_container "Python normalization for ${CONTAINER_STAT_TYPE}, sim_id ${current_sim_id} completed successfully.";
        else
            log_container "ERROR: Python normalization for ${CONTAINER_STAT_TYPE}, sim_id ${current_sim_id} FAILED with exit status ${python_exit_status}."
            echo "sim_id,${current_sim_id},pop1_ref,${CONTAINER_POP1_REF},stat,${CONTAINER_STAT_TYPE},norm_status,failed_python_exit_${python_exit_status}" >> "./${CONTAINER_RUNTIME_DIR}/norm_onepop.${CONTAINER_STAT_TYPE}.error.log"
        fi
    done
    overall_end_time=$(date +%s); overall_runtime=$((overall_end_time - overall_start_time))
    log_container "All sim_ids from CSV processed for ${CONTAINER_STAT_TYPE} normalization. Total time: ${overall_runtime}s"
    echo "stat_type,${CONTAINER_STAT_TYPE},total_sim_ids_processed,${#sim_ids_to_run[@]},overall_norm_runtime,${overall_runtime},seconds" >> "./${CONTAINER_RUNTIME_DIR}/norm_onepop_summary.runtime.csv"
fi
rm -f "./${PYTHON_NORM_SCRIPT_NAME}"
log_container "Python helper script removed."
log_container "Normalization of ${CONTAINER_STAT_TYPE} for selection simulations finished."
log_container "----------------------------------------------------"
log_container "Container Script (Normalize One-Pop Stats for ${CONTAINER_STAT_TYPE}) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
log_message "INFO" "Docker container (Normalize One-Pop Stats for ${STAT_TYPE}) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then log_message "INFO" "Script (Normalize One-Pop Stats for ${STAT_TYPE}) likely interrupted."; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then log_message "ERROR" "Docker container (Normalize One-Pop Stats for ${STAT_TYPE}) reported an error."; fi
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (09_normalize_onepop_stats.sh for ${STAT_TYPE}) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"