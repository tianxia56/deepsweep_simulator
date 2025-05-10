#!/bin/bash

# This script normalizes XPEHH scores for SELECTION simulations using pre-computed
# normalization bins and then calculates the maximum normalized XPEHH across pairs.

# --- Python Helper Script Name ---
PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME="norm_xpehh_max_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/normalize_xpehh_max.log"

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
    POP1_REF=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1]))['selected_pop'])" "${CONFIG_FILE}" 2>/dev/null)
    POP_IDS_STR_RAW=$(python3 -c "import sys, json; print(' '.join(map(str, json.load(open(sys.argv[1]))['pop_ids'])))" "${CONFIG_FILE}" 2>/dev/null)
    if [ $? -ne 0 ] || [ -z "$POP1_REF" ] || [ "$POP1_REF" = "null" ] || [ -z "$POP_IDS_STR_RAW" ]; then
        log_message "ERROR_CONFIG" "Failed to read 'selected_pop' or 'pop_ids' using Python fallback from ${CONFIG_FILE}."
        exit 1
    fi
    eval "POP_IDS_ARRAY=(${POP_IDS_STR_RAW})"
    log_message "INFO" "Read config using Python fallback. jq is highly recommended."
else
    POP1_REF=$(jq -r '.selected_pop' "${CONFIG_FILE}")
    POP_IDS_STR_JQ=$(jq -r '.pop_ids | map(tostring) | join(" ")' "${CONFIG_FILE}")
    read -r -a POP_IDS_ARRAY <<< "${POP_IDS_STR_JQ}"
fi

if [ -z "$POP1_REF" ] || [ "$POP1_REF" = "null" ]; then log_message "ERROR_CONFIG" "'selected_pop' not found or null in ${CONFIG_FILE}."; exit 1; fi
if [ ${#POP_IDS_ARRAY[@]} -eq 0 ]; then log_message "ERROR_CONFIG" "'pop_ids' not found or empty in ${CONFIG_FILE}."; exit 1; fi

# CSV file to get sim_ids from (output of XPEHH map/hap gen for selection sims)
INPUT_CSV_BASENAME="xpehh.sel.map_hap_gen.runtime.csv" 

# Directories
HAPBIN_DIR="hapbin"                     # Input XPEHH .out files (from neutral sims, processed by script 05)
BIN_DIR="bin"                           # Input normalization bin files (from script 08)
NORM_OUTPUT_DIR="norm"                  # Output for normalized stats & max_xpehh
RUNTIME_DIR="runtime"
DOCKER_IMAGE_PYXPEHH="docker.io/tx56/deepsweep_simulator:latest" # Assumed to have Python, pandas, numpy
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
log_message "INFO" "Host Script (10_normalize_xpehh_max.sh) Started: $(date)"
log_message "INFO" "Reference Pop (pop1): ${POP1_REF}, All Pop IDs for pairs: ${POP_IDS_ARRAY[*]}"
log_message "INFO" "Input XPEHH files from: ${HAPBIN_DIR} (prefix: sel)"
log_message "INFO" "Bin files from: ${BIN_DIR}"
log_message "INFO" "Output to: ${NORM_OUTPUT_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Config file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${NORM_OUTPUT_DIR}"
mkdir -p "${RUNTIME_DIR}" # Should exist

INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV for sim_ids '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
# xpehh.sel.map_hap_gen.runtime.csv format: sim_id,<sim_id_val>,sel_map_hap_gen_runtime,all_pops,...
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV '${INPUT_CSV_FILE_HOST}' has no numeric sim_ids in 2nd col."; fi

# Construct XPEHH pair IDs string (e.g., "1_vs_2,1_vs_3,1_vs_4") to pass to Python
XPEHH_PAIR_IDS_LIST=()
for pop2_iter in "${POP_IDS_ARRAY[@]}"; do
    if [ "${pop2_iter}" -ne "${POP1_REF}" ]; then
        XPEHH_PAIR_IDS_LIST+=("${POP1_REF}_vs_${pop2_iter}")
    fi
done
XPEHH_PAIR_IDS_STR_ARG=$(IFS=,; echo "${XPEHH_PAIR_IDS_LIST[*]}")
if [ -z "${XPEHH_PAIR_IDS_STR_ARG}" ]; then
    log_message "WARNING" "No XPEHH pairs to process based on POP1_REF and POP_IDS_ARRAY. Check config."
    # Script will still run but Python might not do much.
fi


if ! docker image inspect "$DOCKER_IMAGE_PYXPEHH" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PYXPEHH} not found. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_PYXPEHH"; then log_message "ERROR" "Failed to pull ${DOCKER_IMAGE_PYXPEHH}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PYXPEHH} already exists."
fi
log_message "INFO" "Starting Docker container for normalizing XPEHH and finding max..."

# --- Docker Execution ---
docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_XPEHH_PAIR_IDS_STR_ARG="${XPEHH_PAIR_IDS_STR_ARG}" \
    -e CONTAINER_HAPBIN_DIR="${HAPBIN_DIR}" \
    -e CONTAINER_BIN_DIR="${BIN_DIR}" \
    -e CONTAINER_NORM_OUTPUT_DIR="${NORM_OUTPUT_DIR}" \
    -e CONTAINER_RUNTIME_DIR="${RUNTIME_DIR}" \
    -e CONTAINER_INPUT_CSV_BASENAME="${INPUT_CSV_BASENAME}" \
    -e PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME="${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}" \
    "$DOCKER_IMAGE_PYXPEHH" /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 

cleanup_and_exit() {
    log_container "Caught signal! Cleaning up temporary Python script..."
    rm -f "./${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}"
    log_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM
log_container "----------------------------------------------------"
log_container "Container Script (Normalize XPEHH & Max) Started: $(date)"
log_container "----------------------------------------------------"
# (Echo received ENV VARS - condensed)
log_container "Received XPEHH_PAIR_IDS_STR_ARG: [${CONTAINER_XPEHH_PAIR_IDS_STR_ARG}]"


# Create the Python helper script inside the container
cat > "./${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}" <<'PYTHON_SCRIPT_EOF'
import sys
import os
import pandas as pd
import numpy as np
import glob # For finding temp normalized files

def find_closest_bin_idx(daf_value, bin_daf_max_values):
    if pd.isna(daf_value): return -1 
    bin_daf_max_values_np = np.asarray(bin_daf_max_values)
    differences = np.abs(bin_daf_max_values_np - daf_value)
    closest_idx = np.argmin(differences)
    return closest_idx

def normalize_xpehh_for_pair(sim_id, pair_id, hapbin_dir, bin_dir, norm_output_dir_temp):
    # Input XPEHH file (output of 05_hapbin_xpehh.sh)
    # Format: hapbin/sel.<sim_id>_<pair_id>.xpehh.out (pos daf xpehh with header)
    input_file = os.path.join(hapbin_dir, f"sel.{sim_id}_{pair_id}.xpehh.out")
    # Normalization bin file
    norm_bin_file = os.path.join(bin_dir, f"xpehh_{pair_id}_bin.csv")
    # Temporary output for this normalized pair
    temp_norm_output_file = os.path.join(norm_output_dir_temp, f"temp.xpehh.{sim_id}_{pair_id}.tsv")

    print(f"Python XPEHH Norm: Normalizing sim {sim_id}, pair {pair_id}")
    print(f"Python XPEHH Norm: Input XPEHH file: {input_file}")
    print(f"Python XPEHH Norm: Bin file: {norm_bin_file}")

    if not os.path.exists(input_file):
        print(f"Python XPEHH Norm: ERROR - Input XPEHH file not found: {input_file}")
        return False
    if not os.path.exists(norm_bin_file):
        print(f"Python XPEHH Norm: ERROR - Normalization bin file not found: {norm_bin_file}")
        return False

    try:
        data_df = pd.read_csv(input_file, sep=' ', header=0) # Expects pos, daf, xpehh
        if not all(col in data_df.columns for col in ['pos', 'daf', 'xpehh']):
            print(f"Python XPEHH Norm: ERROR - Required columns (pos, daf, xpehh) missing in {input_file}. Cols: {data_df.columns.tolist()}")
            return False
        
        data_df['daf'] = pd.to_numeric(data_df['daf'], errors='coerce')
        data_df['xpehh'] = pd.to_numeric(data_df['xpehh'], errors='coerce')
        data_df.dropna(subset=['daf', 'xpehh'], inplace=True)
    except Exception as e:
        print(f"Python XPEHH Norm: Error reading or processing {input_file}: {e}")
        return False

    if data_df.empty:
        print(f"Python XPEHH Norm: No valid data in {input_file} after cleaning.")
        # Still create an empty output file with header
        pd.DataFrame(columns=['pos', 'daf', 'xpehh', 'norm_xpehh']).to_csv(temp_norm_output_file, sep="\t", index=False, na_rep="NA")
        return True 

    try:
        norm_data_df = pd.read_csv(norm_bin_file, header=0)
        # Expected columns: bin_daf_max, mean_stat, std_stat
        if not all(col in norm_data_df.columns for col in ['bin_daf_max', 'mean_stat', 'std_stat']):
            print(f"Python XPEHH Norm: ERROR - Required columns missing in bin file {norm_bin_file}. Cols: {norm_data_df.columns.tolist()}")
            return False
        norm_data_df.rename(columns={'bin_daf_max': 'bin_daf_max', 'mean_stat': 'mean', 'std_stat': 'std'}, inplace=True)
        norm_data_df['bin_daf_max'] = pd.to_numeric(norm_data_df['bin_daf_max'], errors='coerce')
        norm_data_df['mean'] = pd.to_numeric(norm_data_df['mean'], errors='coerce')
        norm_data_df['std'] = pd.to_numeric(norm_data_df['std'], errors='coerce')
        norm_data_df.dropna(inplace=True)
    except Exception as e:
        print(f"Python XPEHH Norm: Error reading or processing bin file {norm_bin_file}: {e}")
        return False
    
    if norm_data_df.empty:
        print(f"Python XPEHH Norm: Bin file {norm_bin_file} is empty or has no valid data.")
        return False

    normalized_values = []
    for index, row in data_df.iterrows():
        daf_val = row['daf']; stat_val = row['xpehh']
        if pd.isna(daf_val) or pd.isna(stat_val): normalized_values.append(np.nan); continue
        
        closest_bin_idx = find_closest_bin_idx(daf_val, norm_data_df['bin_daf_max'])
        if closest_bin_idx == -1 : normalized_values.append(np.nan); continue
        
        bin_info = norm_data_df.iloc[closest_bin_idx]
        bin_mean = bin_info['mean']; bin_std = bin_info['std']
        
        if pd.isna(bin_mean) or pd.isna(bin_std) or bin_std == 0: normalized_values.append(np.nan)
        else: norm_val = (stat_val - bin_mean) / bin_std; normalized_values.append(round(norm_val, 4))
            
    data_df['norm_xpehh'] = normalized_values
    
    os.makedirs(norm_output_dir_temp, exist_ok=True)
    data_df.to_csv(temp_norm_output_file, sep="\t", index=False, header=True, na_rep="NA")
    print(f"Python XPEHH Norm: Normalized XPEHH for sim {sim_id}, pair {pair_id} saved to {temp_norm_output_file}")
    return True

def calculate_max_xpehh(sim_id, norm_output_dir_temp):
    # Find all temp normalized XPEHH files for this sim_id
    pattern = os.path.join(norm_output_dir_temp, f"temp.xpehh.{sim_id}_*.tsv")
    temp_norm_files = glob.glob(pattern)

    if not temp_norm_files:
        print(f"Python XPEHH Max: No temporary normalized XPEHH files found for sim_id {sim_id} with pattern {pattern}")
        return False

    print(f"Python XPEHH Max: Found files for sim_id {sim_id}: {temp_norm_files}")
    
    all_data_list = []
    for i, file_path in enumerate(temp_norm_files):
        try:
            df = pd.read_csv(file_path, sep="\t", usecols=['pos', 'norm_xpehh'])
            # Rename norm_xpehh to be unique for merging, e.g., norm_xpehh_pair1, norm_xpehh_pair2
            # Extract pair_id from filename for clarity if needed, or just use index
            # Filename: temp.xpehh.SIMID_POP1vsPOP2.tsv
            pair_id_from_filename = os.path.basename(file_path).replace(f"temp.xpehh.{sim_id}_", "").replace(".tsv","")
            df.rename(columns={'norm_xpehh': f'norm_xpehh_{pair_id_from_filename}'}, inplace=True)
            all_data_list.append(df)
        except Exception as e:
            print(f"Python XPEHH Max: Error reading {file_path}: {e}")
            continue # Skip this file

    if not all_data_list:
        print(f"Python XPEHH Max: No data could be read from temporary files for sim_id {sim_id}.")
        return False

    # Merge all dataframes by 'pos'
    # Start with the first dataframe, then iteratively merge others
    merged_df = all_data_list[0]
    for i in range(1, len(all_data_list)):
        merged_df = pd.merge(merged_df, all_data_list[i], on='pos', how='outer') # outer join to keep all positions

    # Columns to calculate max over will be all 'norm_xpehh_*' columns
    norm_xpehh_cols = [col for col in merged_df.columns if col.startswith('norm_xpehh_')]
    
    if not norm_xpehh_cols:
        print(f"Python XPEHH Max: No 'norm_xpehh_*' columns found after merge for sim_id {sim_id}.")
        return False

    merged_df['max_xpehh'] = merged_df[norm_xpehh_cols].max(axis=1, skipna=True)
    
    # Select final columns: pos, max_xpehh
    final_max_df = merged_df[['pos', 'max_xpehh']].copy()
    final_max_df.dropna(subset=['max_xpehh'], inplace=True) # Remove rows where max_xpehh is NaN (e.g. all inputs were NaN)

    output_file = os.path.join(norm_output_dir_temp, f"temp.max.xpehh.{sim_id}.tsv")
    final_max_df.to_csv(output_file, sep="\t", index=False, header=True, na_rep="NA")
    print(f"Python XPEHH Max: Max XPEHH for sim_id {sim_id} saved to {output_file}")
    return True

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: python <script_name>.py <sim_id> <comma_sep_pair_ids> <hapbin_dir> <bin_dir> <norm_output_dir> <runtime_dir>")
        sys.exit(1)
    
    sim_id_arg = sys.argv[1]
    pair_ids_comma_sep_arg = sys.argv[2]
    hapbin_dir_arg = sys.argv[3]
    bin_dir_arg = sys.argv[4]
    norm_output_dir_arg = sys.argv[5]
    # runtime_dir_arg = sys.argv[6] # For logging runtime, Bash handles this

    all_pair_ids = [p.strip() for p in pair_ids_comma_sep_arg.split(',')]
    
    overall_success = True

    # Step 1: Normalize XPEHH for each pair
    for pair_id_val in all_pair_ids:
        if not normalize_xpehh_for_pair(sim_id_arg, pair_id_val, hapbin_dir_arg, bin_dir_arg, norm_output_dir_arg):
            print(f"Python: Failed to normalize XPEHH for sim {sim_id_arg}, pair {pair_id_val}")
            overall_success = False # Mark as failed but continue other pairs for this sim_id

    # Step 2: Calculate Max XPEHH for this sim_id if all normalizations were attempted
    if not calculate_max_xpehh(sim_id_arg, norm_output_dir_arg):
        print(f"Python: Failed to calculate max XPEHH for sim {sim_id_arg}")
        overall_success = False
        
    if overall_success:
        sys.exit(0)
    else:
        sys.exit(1)
PYTHON_SCRIPT_EOF
chmod +x "./${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}"
log_container "Python XPEHH Normalize & Max script created."

INPUT_CSV_FILE_IN_CONTAINER="./${CONTAINER_RUNTIME_DIR}/${CONTAINER_INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_IN_CONTAINER}" ]; then
    log_container "CRITICAL ERROR: Input CSV '${INPUT_CSV_FILE_IN_CONTAINER}' not found. Exiting."
    exit 1
fi

log_container "Reading all sim_ids from ${INPUT_CSV_FILE_IN_CONTAINER} for XPEHH normalization and max calculation..."
# xpehh.sel.map_hap_gen.runtime.csv format: sim_id,<sim_id_val>,sel_map_hap_gen_runtime,all_pops,...
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_IN_CONTAINER}" | sort -un)

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_container "No valid sim_ids found in ${INPUT_CSV_FILE_IN_CONTAINER}. Nothing to process."
else
    log_container "Found unique sim_ids for XPEHH Norm & Max: ${sim_ids_to_run[*]}"
    for current_sim_id in "${sim_ids_to_run[@]}"; do
        log_container "--- Processing XPEHH Norm & Max for sim_id: ${current_sim_id} ---"
        
        overall_sim_start_time=$(date +%s)
        
        python3 "./${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}" \
            "${current_sim_id}" \
            "${CONTAINER_XPEHH_PAIR_IDS_STR_ARG}" \
            "./${CONTAINER_HAPBIN_DIR}" \
            "./${CONTAINER_BIN_DIR}" \
            "./${CONTAINER_NORM_OUTPUT_DIR}" \
            "./${CONTAINER_RUNTIME_DIR}" # Pass runtime_dir for consistency if Python needs it later
        
        python_exit_status=$?
        overall_sim_end_time=$(date +%s)
        overall_sim_runtime=$((overall_sim_end_time - overall_sim_start_time))

        if [ $python_exit_status -eq 0 ]; then
            log_container "Python script for XPEHH Norm & Max completed successfully for sim_id ${current_sim_id}. Runtime: ${overall_sim_runtime}s"
            echo "sim_id,${current_sim_id},xpehh_norm_max_runtime,${overall_sim_runtime},seconds,status,success" >> "./${CONTAINER_RUNTIME_DIR}/xpehh_norm_max.sel.runtime.csv"
        else
            log_container "ERROR: Python script for XPEHH Norm & Max FAILED for sim_id ${current_sim_id} with exit status ${python_exit_status}."
            echo "sim_id,${current_sim_id},xpehh_norm_max_runtime,${overall_sim_runtime},seconds,status,failed_python_exit_${python_exit_status}" >> "./${CONTAINER_RUNTIME_DIR}/xpehh_norm_max.sel.runtime.csv"
        fi
        log_container "--- Finished XPEHH Norm & Max for sim_id: ${current_sim_id} ---"
    done
    log_container "All sim_ids from CSV processed for XPEHH Norm & Max."
fi

rm -f "./${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}"
log_container "Python helper script removed."

log_container "XPEHH Normalization and Max calculation finished."
log_container "----------------------------------------------------"
log_container "Container Script (Normalize XPEHH & Max) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
log_message "INFO" "Docker container (Normalize XPEHH & Max) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then log_message "INFO" "Script (Normalize XPEHH & Max) likely interrupted."; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then log_message "ERROR" "Docker container (Normalize XPEHH & Max) reported an error."; fi
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (10_normalize_xpehh_max.sh) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"
