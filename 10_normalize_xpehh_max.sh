#!/bin/bash

# --- Bulletproof Bash and Conda Activation ---
# 1. Ensure the script is running with Bash, not sh/dash
if [ -z "$BASH_VERSION" ]; then
    # Re-execute the script with the same arguments using bash.
    echo "Re-executing with /bin/bash..." >&2
    exec /bin/bash "$0" "$@"
fi

# 2. Manually find and activate the Conda environment by manipulating the PATH.
# This is more reliable in non-interactive scripts than 'conda activate'.
ENV_NAME="deepsweep_simulator"
echo "Attempting to set PATH for Conda environment: ${ENV_NAME}"

# Find the full path to the Conda environment
CONDA_ENV_PATH=$(conda info --envs | grep "${ENV_NAME}" | awk '{print $NF}')

# Check if the environment path was found
if [ -z "${CONDA_ENV_PATH}" ]; then
    echo "ERROR: Could not find Conda environment path for '${ENV_NAME}'." >&2
    exit 1
fi

# Check if the environment's bin directory exists
if [ ! -d "${CONDA_ENV_PATH}/bin" ]; then
    echo "ERROR: bin directory not found in '${CONDA_ENV_PATH}'." >&2
    exit 1
fi

# Prepend the environment's bin directory to the PATH
export PATH="${CONDA_ENV_PATH}/bin:${PATH}"

echo "Successfully set PATH for Conda environment."
# --- End of Activation Block ---


# This script normalizes XPEHH scores for SELECTION simulations using pre-computed
# normalization bins and then calculates the maximum normalized XPEHH across pairs.
# This version runs all commands directly on the host system without Docker.

# --- Python Helper Script Name ---
PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME="norm_xpehh_max_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/normalize_xpehh_max.log"

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Helper Functions ---
log_message() {
    local type="$1"
    local message="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${type}] - ${message}"
}

# --- Initial Setup ---
log_message "INFO" "Host Script (10_normalize_xpehh_max.sh) Started: $(date)"

# --- Local Dependency Checks ---
log_message "INFO" "Checking for local dependencies..."
log_message "DEBUG" "Python interpreter being used is: $(which python3)"
if ! command -v python3 &> /dev/null; then
    log_message "ERROR" "'python3' command not found. Python 3 is required."
    exit 1
fi
if ! python3 -c "import pandas" &> /dev/null; then
    log_message "ERROR" "Python 'pandas' library not found. Please install it (e.g., 'pip install pandas')."
    exit 1
fi
if ! python3 -c "import numpy" &> /dev/null; then
    log_message "ERROR" "Python 'numpy' library not found. Please install it (e.g., 'pip install numpy')."
    exit 1
fi
log_message "INFO" "All local dependencies found."

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

INPUT_CSV_BASENAME="xpehh.sel.map_hap_gen.runtime.csv" 
HAPBIN_DIR="hapbin"
BIN_DIR="bin"
NORM_OUTPUT_DIR="norm"
RUNTIME_DIR="runtime"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
log_message "INFO" "Reference Pop (pop1): ${POP1_REF}, All Pop IDs for pairs: ${POP_IDS_ARRAY[*]}"
log_message "INFO" "Input XPEHH files from: ${HAPBIN_DIR} (prefix: sel)"
log_message "INFO" "Bin files from: ${BIN_DIR}"
log_message "INFO" "Output to: ${NORM_OUTPUT_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Config file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${NORM_OUTPUT_DIR}"
mkdir -p "${RUNTIME_DIR}"

INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV for sim_ids '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
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
fi

# --- Main Logic ---

# Cleanup function to remove the temporary python script
cleanup() {
    log_message "INFO" "Cleaning up temporary Python script..."
    rm -f "${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}"
}
trap cleanup INT TERM EXIT

# Create the Python helper script in the current directory
log_message "INFO" "Creating Python helper script: ${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}"
cat > "${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}" <<'PYTHON_SCRIPT_EOF'
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
    if len(sys.argv) != 6:
        print("Usage: python <script_name>.py <sim_id> <comma_sep_pair_ids> <hapbin_dir> <bin_dir> <norm_output_dir>")
        sys.exit(1)
    
    sim_id_arg = sys.argv[1]
    pair_ids_comma_sep_arg = sys.argv[2]
    hapbin_dir_arg = sys.argv[3]
    bin_dir_arg = sys.argv[4]
    norm_output_dir_arg = sys.argv[5]

    all_pair_ids = [p.strip() for p in pair_ids_comma_sep_arg.split(',') if p.strip()]
    
    if not all_pair_ids:
        print("Python: No valid pair IDs provided. Exiting.")
        sys.exit(0) # Not an error, just nothing to do.

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
chmod +x "${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}"

# Read sim_ids and start processing
log_message "INFO" "Reading all sim_ids from ${INPUT_CSV_FILE_HOST} for XPEHH normalization and max calculation..."
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_HOST}" | sort -un)

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_message "WARNING" "No valid sim_ids found in ${INPUT_CSV_FILE_HOST}. Nothing to process."
elif [ -z "${XPEHH_PAIR_IDS_STR_ARG}" ]; then
    log_message "WARNING" "No XPEHH pairs were defined to process. Skipping all simulations."
else
    log_message "INFO" "Found unique sim_ids for XPEHH Norm & Max: ${sim_ids_to_run[*]}"
    for current_sim_id in "${sim_ids_to_run[@]}"; do
        log_message "INFO" "--- Processing XPEHH Norm & Max for sim_id: ${current_sim_id} ---"
        
        overall_sim_start_time=$(date +%s)
        
        python3 "./${PYTHON_XPEHH_NORM_MAX_SCRIPT_NAME}" \
            "${current_sim_id}" \
            "${XPEHH_PAIR_IDS_STR_ARG}" \
            "${HAPBIN_DIR}" \
            "${BIN_DIR}" \
            "${NORM_OUTPUT_DIR}"
        
        python_exit_status=$?
        overall_sim_end_time=$(date +%s)
        overall_sim_runtime=$((overall_sim_end_time - overall_sim_start_time))

        if [ $python_exit_status -eq 0 ]; then
            log_message "INFO" "Python script for XPEHH Norm & Max completed successfully for sim_id ${current_sim_id}. Runtime: ${overall_sim_runtime}s"
            echo "sim_id,${current_sim_id},xpehh_norm_max_runtime,${overall_sim_runtime},seconds,status,success" >> "${RUNTIME_DIR}/xpehh_norm_max.sel.runtime.csv"
        else
            log_message "ERROR" "Python script for XPEHH Norm & Max FAILED for sim_id ${current_sim_id} with exit status ${python_exit_status}."
            echo "sim_id,${current_sim_id},xpehh_norm_max_runtime,${overall_sim_runtime},seconds,status,failed_python_exit_${python_exit_status}" >> "${RUNTIME_DIR}/xpehh_norm_max.sel.runtime.csv"
        fi
        log_message "INFO" "--- Finished XPEHH Norm & Max for sim_id: ${current_sim_id} ---"
    done
    log_message "INFO" "All sim_ids from CSV processed for XPEHH Norm & Max."
fi

log_message "INFO" "XPEHH Normalization and Max calculation finished."
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (10_normalize_xpehh_max.sh) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"