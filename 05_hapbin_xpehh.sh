#!/bin/bash

# This script processes simulations (neutral or selection) to:
# 1. Convert TPED to .hap and .map files (for xpehhbin)
# 2. Run xpehhbin for pop1 vs other populations
# 3. Add DAF and position info to xpehhbin outputs

# Usage: ./05_xpehh_processing.sh <neut|sel>

# --- Script Argument Validation ---
if [ "$#" -ne 1 ] || ([ "$1" != "neut" ] && [ "$1" != "sel" ]); then
    echo "Usage: $0 <neut|sel>"
    echo "  neut: Process neutral simulations"
    echo "  sel:  Process selection simulations"
    exit 1
fi
SIM_TYPE="$1" # "neut" or "sel"

# --- Python Helper Script Names ---
PYTHON_PREPROCESS_SCRIPT_NAME="xpehh_preprocess_core.py"
PYTHON_POSTPROCESS_SCRIPT_NAME="xpehh_postprocess_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/xpehh_processing_${SIM_TYPE}.log"

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
    log_message "WARNING_JQ" "jq command not found. Please install jq or ensure Python fallback works."
    POP1_TARGET=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1]))['selected_pop'])" "${CONFIG_FILE}" 2>/dev/null)
    PYTHON_EXIT_CODE_POP1=$?
    POP_IDS_STR_RAW=$(python3 -c "import sys, json; print(' '.join(map(str, json.load(open(sys.argv[1]))['pop_ids'])))" "${CONFIG_FILE}" 2>/dev/null)
    PYTHON_EXIT_CODE_POPIDS=$?
    if [ $PYTHON_EXIT_CODE_POP1 -ne 0 ] || [ -z "$POP1_TARGET" ] || [ "$POP1_TARGET" = "null" ]; then
        log_message "ERROR_CONFIG" "Failed to read 'selected_pop' using Python fallback from ${CONFIG_FILE}."
        exit 1
    fi
    if [ $PYTHON_EXIT_CODE_POPIDS -ne 0 ] || [ -z "$POP_IDS_STR_RAW" ]; then
        log_message "ERROR_CONFIG" "Failed to read 'pop_ids' using Python fallback from ${CONFIG_FILE}."
        exit 1
    fi
    eval "POP_IDS_ARRAY=(${POP_IDS_STR_RAW})"
    log_message "INFO" "Read config using Python fallback. jq is highly recommended."
else
    POP1_TARGET=$(jq -r '.selected_pop' "${CONFIG_FILE}")
    POP_IDS_STR_JQ=$(jq -r '.pop_ids | map(tostring) | join(" ")' "${CONFIG_FILE}")
    read -r -a POP_IDS_ARRAY <<< "${POP_IDS_STR_JQ}"
fi

if [ -z "$POP1_TARGET" ] || [ "$POP1_TARGET" = "null" ]; then
    log_message "ERROR_CONFIG" "'selected_pop' not found or null in ${CONFIG_FILE}."
    exit 1
fi
if [ ${#POP_IDS_ARRAY[@]} -eq 0 ]; then
    log_message "ERROR_CONFIG" "'pop_ids' not found, empty, or resulted in an empty array after parsing from ${CONFIG_FILE}."
    exit 1
fi
MAX_POP_ID=$(echo "${POP_IDS_ARRAY[@]}" | tr ' ' '\n' | sort -nr | head -n1)
if [ -z "$MAX_POP_ID" ]; then 
    log_message "ERROR_CONFIG" "Could not determine MAX_POP_ID from pop_ids: ${POP_IDS_ARRAY[*]}. Ensure pop_ids contains numbers."
    exit 1
fi

INPUT_SIM_DIR=""
INPUT_CSV_BASENAME=""
PATH_PREFIX_FOR_FILES="" 
if [ "${SIM_TYPE}" == "neut" ]; then
    INPUT_SIM_DIR="neutral_sims"
    INPUT_CSV_BASENAME="nsl.neut.runtime.csv"
    PATH_PREFIX_FOR_FILES="neut"
else 
    INPUT_SIM_DIR="selected_sims"
    INPUT_CSV_BASENAME="nsl.sel.runtime.csv"
    PATH_PREFIX_FOR_FILES="sel"
fi
HAPBIN_OUTPUT_DIR="hapbin" 
RUNTIME_DIR="runtime"
DOCKER_IMAGE_XPEHH="docker.io/tx56/deepsweep_simulator:latest" 
HOST_CWD=$(pwd)

log_message "INFO" "Host Script (05_xpehh_processing.sh for ${SIM_TYPE}) Started: $(date)"
log_message "INFO" "Reading configuration from ${CONFIG_FILE}"
log_message "INFO" "POP1_TARGET (Reference Population for XPEHH): ${POP1_TARGET}"
log_message "INFO" "POP_IDS_ARRAY: ${POP_IDS_ARRAY[*]}"
log_message "INFO" "MAX_POP_ID: ${MAX_POP_ID}"
log_message "INFO" "Processing SIM_TYPE: ${SIM_TYPE}"
log_message "INFO" "TPED Input Directory: ${HOST_CWD}/${INPUT_SIM_DIR}"
log_message "INFO" "Hapbin/XPEHH Output Directory: ${HOST_CWD}/${HAPBIN_OUTPUT_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Configuration file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${HAPBIN_OUTPUT_DIR}"; mkdir -p "${RUNTIME_DIR}"
INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV file '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV file '${INPUT_CSV_FILE_HOST}' has no numeric sim_ids in 2nd col."; fi
if ! docker image inspect "$DOCKER_IMAGE_XPEHH" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_XPEHH} not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_XPEHH"; then log_message "ERROR" "Failed to pull Docker image ${DOCKER_IMAGE_XPEHH}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_XPEHH} already exists locally."
fi
log_message "INFO" "Starting Docker container for XPEHH processing (${SIM_TYPE})..."

docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_POP1_TARGET="${POP1_TARGET}" \
    -e CONTAINER_POP_IDS_STR="${POP_IDS_ARRAY[*]}" \
    -e CONTAINER_MAX_POP_ID="${MAX_POP_ID}" \
    -e CONTAINER_PATH_PREFIX="${PATH_PREFIX_FOR_FILES}" \
    -e CONTAINER_INPUT_SIM_DIR="${INPUT_SIM_DIR}" \
    -e CONTAINER_HAPBIN_OUTPUT_DIR="${HAPBIN_OUTPUT_DIR}" \
    -e CONTAINER_RUNTIME_DIR="${RUNTIME_DIR}" \
    -e CONTAINER_INPUT_CSV_BASENAME="${INPUT_CSV_BASENAME}" \
    -e PYTHON_PREPROCESS_SCRIPT_NAME="${PYTHON_PREPROCESS_SCRIPT_NAME}" \
    -e PYTHON_POSTPROCESS_SCRIPT_NAME="${PYTHON_POSTPROCESS_SCRIPT_NAME}" \
    "$DOCKER_IMAGE_XPEHH" /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 
cleanup_and_exit() {
    log_container "Caught signal! Cleaning up temporary Python scripts..."
    rm -f "./${PYTHON_PREPROCESS_SCRIPT_NAME}" "./${PYTHON_POSTPROCESS_SCRIPT_NAME}"
    log_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM
log_container "----------------------------------------------------"
log_container "Container Script (XPEHH Processing for ${CONTAINER_PATH_PREFIX}) Started: $(date)"
log_container "----------------------------------------------------"
read -r -a CONTAINER_POP_IDS_ARRAY <<< "${CONTAINER_POP_IDS_STR}"

# --- Python Preprocessing Script ---
cat > "./${PYTHON_PREPROCESS_SCRIPT_NAME}" <<'PYTHON_PRE_EOF'
import sys
import os
def extract_and_clean_columns(input_file, output_file_map, output_file_hap):
    with open(input_file, 'r') as file, open(output_file_map, 'w') as map_file, open(output_file_hap, 'w') as hap_file:
        for line in file:
            columns = line.split()
            if len(columns) < 5: 
                continue
            map_file.write(' '.join(columns[:4]) + '\n')
            cleaned_columns = [col.strip() for col in columns[4:]]
            hap_file.write(' '.join(cleaned_columns) + '\n')
def create_map_file_final_format(original_map_file):
    temp_file = original_map_file + ".tmp"
    with open(original_map_file, 'r') as map_file, open(temp_file, 'w') as new_file:
        for line in map_file:
            columns = line.split()
            formatted_columns = []
            for col_idx, col_val in enumerate(columns):
                col_val_ascii = col_val.encode('ascii', 'ignore').decode('ascii')
                if 'e' in col_val_ascii or 'E' in col_val_ascii:
                    try:
                        formatted_columns.append(f"{float(col_val_ascii):.6f}")
                    except ValueError:
                        formatted_columns.append(col_val_ascii) 
                else:
                    formatted_columns.append(col_val_ascii)
            cleaned_line = ' '.join(formatted_columns).strip()
            new_file.write(cleaned_line + '\n')
    os.replace(temp_file, original_map_file)
def main_preprocess(sim_id, pop_id_current, pop1_ref, path_prefix, input_sim_dir_arg, hapbin_out_dir_arg):
    input_file = f"{input_sim_dir_arg}/{path_prefix}.hap.{sim_id}_0_{pop_id_current}.tped"
    if not os.path.exists(input_file):
        print(f"Python Pre: TPED file {input_file} does not exist for sim {sim_id}, pop {pop_id_current}. Skipping.")
        return False
    os.makedirs(hapbin_out_dir_arg, exist_ok=True)
    output_file_hap = f"{hapbin_out_dir_arg}/{path_prefix}.{sim_id}_0_{pop_id_current}.hap"
    output_file_map = f"{hapbin_out_dir_arg}/{path_prefix}.{sim_id}_0_{pop1_ref}.map"
    generate_this_map = False
    if not os.path.exists(output_file_map):
        generate_this_map = True
    temp_map_for_extraction = output_file_map + f".temp_extract_pop{pop_id_current}"
    if generate_this_map:
        extract_and_clean_columns(input_file, temp_map_for_extraction, output_file_hap)
        create_map_file_final_format(temp_map_for_extraction)
        if os.path.exists(temp_map_for_extraction):
             os.rename(temp_map_for_extraction, output_file_map)
             print(f"Python Pre: Created map {output_file_map} (from pop {pop_id_current}) and hap {output_file_hap}")
        else: 
             print(f"Python Pre: Error: temp map {temp_map_for_extraction} not created.")
             return False
    else:
        dummy_map = f"{hapbin_out_dir_arg}/dummy.{path_prefix}.{sim_id}.map.tmp"
        extract_and_clean_columns(input_file, dummy_map, output_file_hap)
        if os.path.exists(dummy_map): os.remove(dummy_map)
        print(f"Python Pre: Created hap {output_file_hap} (map {output_file_map} already exists)")
    return True
if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(f"Usage: python {sys.argv[0]} <sim_id> <pop_id_current> <pop1_ref> <path_prefix> <input_sim_dir> <hapbin_out_dir>")
        sys.exit(1)
    sim_id_arg, pop_id_current_arg, pop1_ref_arg, path_prefix_arg, input_sim_dir_arg, hapbin_out_dir_arg = sys.argv[1:7]
    if main_preprocess(sim_id_arg, pop_id_current_arg, pop1_ref_arg, path_prefix_arg, input_sim_dir_arg, hapbin_out_dir_arg): sys.exit(0)
    else: sys.exit(1)
PYTHON_PRE_EOF
chmod +x "./${PYTHON_PREPROCESS_SCRIPT_NAME}"
log_container "Python Preprocessing script created."

# --- Python Postprocessing Script (with dtype fix for merge) ---
cat > "./${PYTHON_POSTPROCESS_SCRIPT_NAME}" <<'PYTHON_POST_EOF'
import os
import pandas as pd
import sys

def add_id_pos_and_daf_column(sim_id_str, pop1_ref_str, path_prefix_str, input_sim_dir_str):
    tped_file_pop1 = f"{input_sim_dir_str}/{path_prefix_str}.hap.{sim_id_str}_0_{pop1_ref_str}.tped"
    if not os.path.exists(tped_file_pop1):
        print(f"Python Post: ERROR: TPED file for DAF calculation {tped_file_pop1} not found.")
        return None
    try:
        tped_data_ids_pos = pd.read_csv(tped_file_pop1, sep='\\s+', usecols=[1, 3], header=None, dtype=str)
        tped_data_ids_pos.columns = ['ID', 'pos']
    except Exception as e:
        print(f"Python Post: Error reading ID/pos columns from {tped_file_pop1}: {e}")
        return None
    try:
        with open(tped_file_pop1, 'r') as f:
            first_line = f.readline()
            num_cols_total = len(first_line.split())
        if num_cols_total < 5: 
            print(f"Python Post: Error: Not enough columns in {tped_file_pop1} for allele data.")
            return None
        allele_cols_indices = list(range(4, num_cols_total))
        pop_allele_data = pd.read_csv(tped_file_pop1, sep='\\s+', usecols=allele_cols_indices, header=None)
    except Exception as e:
        print(f"Python Post: Error reading allele data columns from {tped_file_pop1}: {e}")
        return None
    daf = pop_allele_data.apply(pd.to_numeric, errors='coerce').mean(axis=1)
    results_df = pd.DataFrame({'ID': tped_data_ids_pos['ID'], 'pos': tped_data_ids_pos['pos'], 'daf': daf})
    results_df['ID'] = results_df['ID'].astype(str) # Ensure ID is string
    return results_df

def main_postprocess(sim_id_str, pop1_ref_str, max_pop_id_str, path_prefix_str, hapbin_out_dir_str, input_sim_dir_str):
    print(f"Python Post: Postprocessing XPEHH for sim {sim_id_str}, pop1_ref {pop1_ref_str}")
    pop1_ref = int(pop1_ref_str)
    max_pop_id = int(max_pop_id_str)
    base_results_df = add_id_pos_and_daf_column(sim_id_str, pop1_ref_str, path_prefix_str, input_sim_dir_str)
    if base_results_df is None:
        print(f"Python Post: Failed to get base ID/pos/DAF data for sim {sim_id_str}. Aborting postprocessing for this sim.")
        return False
    
    base_results_df['ID'] = base_results_df['ID'].astype(str) # Ensure base ID is string before loop

    all_successful = True
    for pop2_val in range(1, max_pop_id + 1):
        if pop2_val == pop1_ref:
            continue
        xpehh_out_file = f"{hapbin_out_dir_str}/{path_prefix_str}.{sim_id_str}_{pop1_ref_str}_vs_{pop2_val}.xpehh.out"
        if not os.path.exists(xpehh_out_file):
            print(f"Python Post: XPEHH output file {xpehh_out_file} not found. Skipping.")
            continue
        try:
            # Read ID (col index 1) and XPEHH score (col index 6) explicitly as strings first
            xpehh_data_df_raw = pd.read_csv(xpehh_out_file, sep='\\s+', header=0, usecols=[1, 6], dtype=str)
            
            # Check actual column names after reading with usecols
            # Expected names based on xpehh.out header: 'ID' and 'XPEHH'
            if 'ID' not in xpehh_data_df_raw.columns or 'XPEHH' not in xpehh_data_df_raw.columns:
                print(f"Python Post: ERROR: Expected columns 'ID' and 'XPEHH' not found in {xpehh_out_file} after reading usecols=[1,6]. Columns are: {xpehh_data_df_raw.columns.tolist()}")
                all_successful = False
                continue

            # Create the DataFrame with correct dtypes for merge
            xpehh_data_df = pd.DataFrame({
                'ID': xpehh_data_df_raw['ID'].astype(str), # Ensure 'ID' is string
                'xpehh': pd.to_numeric(xpehh_data_df_raw['XPEHH'], errors='coerce')
            })

        except Exception as e:
            print(f"Python Post: Error reading or processing XPEHH data from {xpehh_out_file}: {e}")
            all_successful = False
            continue
            
        # Both 'ID' columns should now be string type
        merged_df = pd.merge(base_results_df, xpehh_data_df[['ID', 'xpehh']], on='ID', how='inner') 
        
        if merged_df.empty:
            print(f"Python Post: WARNING: Merge for {xpehh_out_file} resulted in an empty DataFrame. Check 'ID' consistency.")
            # Continue processing other files, but mark this pair as potentially problematic or log it.
            # all_successful = False # Uncomment if an empty merge should be considered a failure for the sim_id

        final_df = merged_df[['pos', 'daf', 'xpehh']]
        try:
            final_df.to_csv(xpehh_out_file, sep=' ', index=False, header=True) 
            print(f"Python Post: Updated {xpehh_out_file} with pos, daf, xpehh.")
        except Exception as e:
            print(f"Python Post: Error writing updated data to {xpehh_out_file}: {e}")
            all_successful = False
    return all_successful

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(f"Usage: python {sys.argv[0]} <sim_id> <pop1_ref> <max_pop_id> <path_prefix> <hapbin_out_dir> <input_sim_dir>")
        sys.exit(1)
    sim_id_arg, pop1_ref_arg, max_pop_id_arg, path_prefix_arg, hapbin_out_dir_arg, input_sim_dir_arg = sys.argv[1:7]
    if main_postprocess(sim_id_arg, pop1_ref_arg, max_pop_id_arg, path_prefix_arg, hapbin_out_dir_arg, input_sim_dir_arg): sys.exit(0)
    else: sys.exit(1)
PYTHON_POST_EOF
chmod +x "./${PYTHON_POSTPROCESS_SCRIPT_NAME}"
log_container "Python Postprocessing script created."

# (Rest of the container's main XPEHH processing loop remains the same)
INPUT_CSV_FILE_IN_CONTAINER="./${CONTAINER_RUNTIME_DIR}/${CONTAINER_INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_IN_CONTAINER}" ]; then log_container "CRITICAL ERROR: Input CSV file '${INPUT_CSV_FILE_IN_CONTAINER}' not found. Exiting."; exit 1; fi
log_container "Reading sim_ids from ${INPUT_CSV_FILE_IN_CONTAINER} for XPEHH processing..."
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_IN_CONTAINER}" | sort -un)
if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_container "No valid sim_ids found in ${INPUT_CSV_FILE_IN_CONTAINER}. Nothing to process."
else
    log_container "Found unique sim_ids for XPEHH: ${sim_ids_to_run[*]}"
    for current_sim_id in "${sim_ids_to_run[@]}"; do
        log_container "--- Starting XPEHH pipeline for sim_id: ${current_sim_id} ---"
        log_container "Step 1: Generating .map and .hap files for sim_id ${current_sim_id}..."
        map_hap_overall_start_time=$(date +%s)
        all_pops_preprocessed_successfully=true
        for pop_id_iter in "${CONTAINER_POP_IDS_ARRAY[@]}"; do
            log_container "Preprocessing TPED for sim ${current_sim_id}, pop ${pop_id_iter}..."
            python "./${PYTHON_PREPROCESS_SCRIPT_NAME}" "${current_sim_id}" "${pop_id_iter}" "${CONTAINER_POP1_TARGET}" "${CONTAINER_PATH_PREFIX}" "./${CONTAINER_INPUT_SIM_DIR}" "./${CONTAINER_HAPBIN_OUTPUT_DIR}"
            if [ $? -ne 0 ]; then log_container "ERROR: Python preprocessing failed for sim ${current_sim_id}, pop ${pop_id_iter}."; all_pops_preprocessed_successfully=false; fi
        done
        map_hap_overall_end_time=$(date +%s); map_hap_overall_runtime=$((map_hap_overall_end_time - map_hap_overall_start_time))
        echo "sim_id,${current_sim_id},${CONTAINER_PATH_PREFIX}_map_hap_gen_runtime,all_pops,${map_hap_overall_runtime},seconds" >> "./${CONTAINER_RUNTIME_DIR}/xpehh.${CONTAINER_PATH_PREFIX}.map_hap_gen.runtime.csv"
        if ! ${all_pops_preprocessed_successfully}; then log_container "ERROR: Not all populations preprocessed for sim ${current_sim_id}. Skipping xpehhbin and DAF."; continue; fi
        log_container "Step 1 completed for sim_id ${current_sim_id}."
        log_container "Step 2: Running xpehhbin for sim_id ${current_sim_id} (pop ${CONTAINER_POP1_TARGET} vs others)..."
        for pop2_iter in "${CONTAINER_POP_IDS_ARRAY[@]}"; do
            if [ "${pop2_iter}" -eq "${CONTAINER_POP1_TARGET}" ]; then continue; fi
            hapA_file="./${CONTAINER_HAPBIN_OUTPUT_DIR}/${CONTAINER_PATH_PREFIX}.${current_sim_id}_0_${CONTAINER_POP1_TARGET}.hap"
            hapB_file="./${CONTAINER_HAPBIN_OUTPUT_DIR}/${CONTAINER_PATH_PREFIX}.${current_sim_id}_0_${pop2_iter}.hap"
            map_file_xpehh="./${CONTAINER_HAPBIN_OUTPUT_DIR}/${CONTAINER_PATH_PREFIX}.${current_sim_id}_0_${CONTAINER_POP1_TARGET}.map" 
            xpehh_output_file="./${CONTAINER_HAPBIN_OUTPUT_DIR}/${CONTAINER_PATH_PREFIX}.${current_sim_id}_${CONTAINER_POP1_TARGET}_vs_${pop2_iter}.xpehh.out"
            if [ ! -f "${hapA_file}" ]; then log_container "ERROR: hapA file ${hapA_file} not found!"; continue; fi
            if [ ! -f "${hapB_file}" ]; then log_container "ERROR: hapB file ${hapB_file} not found!"; continue; fi
            if [ ! -f "${map_file_xpehh}" ]; then log_container "ERROR: map file ${map_file_xpehh} not found!"; continue; fi
            xpehh_cmd="xpehhbin --hapA ${hapA_file} --hapB ${hapB_file} --map ${map_file_xpehh} --out ${xpehh_output_file}"
            log_container "Executing: ${xpehh_cmd}"; xpehh_start_time=$(date +%s); ${xpehh_cmd}; xpehh_exit_status=$?; xpehh_end_time=$(date +%s); xpehh_runtime=$((xpehh_end_time - xpehh_start_time))
            if [ $xpehh_exit_status -eq 0 ] && [ -f "${xpehh_output_file}" ]; then
                log_container "xpehhbin for pop ${CONTAINER_POP1_TARGET} vs ${pop2_iter} completed. Runtime: ${xpehh_runtime}s"
                echo "sim_id,${current_sim_id},${CONTAINER_PATH_PREFIX}_xpehh_runtime,${CONTAINER_POP1_TARGET}vs${pop2_iter},${xpehh_runtime},seconds" >> "./${CONTAINER_RUNTIME_DIR}/xpehh.${CONTAINER_PATH_PREFIX}.pairwise.runtime.csv"
            else log_container "ERROR: xpehhbin failed for pop ${CONTAINER_POP1_TARGET} vs ${pop2_iter} (exit status ${xpehh_exit_status})."; fi
        done
        log_container "Step 2 completed for sim_id ${current_sim_id}."
        log_container "Step 3: Adding DAF/pos to XPEHH outputs for sim_id ${current_sim_id}..."
        python "./${PYTHON_POSTPROCESS_SCRIPT_NAME}" "${current_sim_id}" "${CONTAINER_POP1_TARGET}" "${CONTAINER_MAX_POP_ID}" "${CONTAINER_PATH_PREFIX}" "./${CONTAINER_HAPBIN_OUTPUT_DIR}" "./${CONTAINER_INPUT_SIM_DIR}"
        if [ $? -ne 0 ]; then log_container "ERROR: Python postprocessing (add DAF/pos) failed for sim ${current_sim_id}."; else log_container "DAF/pos addition completed for sim ${current_sim_id}."; fi
        log_container "Step 3 completed for sim_id ${current_sim_id}."
        log_container "--- Finished XPEHH pipeline for sim_id: ${current_sim_id} ---"
    done
    log_container "All sim_ids from CSV processed by XPEHH pipeline."
fi
rm -f "./${PYTHON_PREPROCESS_SCRIPT_NAME}" "./${PYTHON_POSTPROCESS_SCRIPT_NAME}"
log_container "Python helper scripts removed."
log_container "XPEHH processing for ${CONTAINER_PATH_PREFIX} simulations finished."
log_container "----------------------------------------------------"
log_container "Container Script (XPEHH Processing for ${CONTAINER_PATH_PREFIX}) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] - Docker container (XPEHH for ${SIM_TYPE}) finished with exit status: ${docker_exit_status}." | tee -a "${LOG_FILE}"
if [ ${docker_exit_status} -eq 130 ]; then echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] - Script (XPEHH for ${SIM_TYPE}) likely interrupted by user (Ctrl+C)." | tee -a "${LOG_FILE}"; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then echo "[$(date '+%Y-%m-%d %H:%M:%S')] [ERROR] - Docker container (XPEHH for ${SIM_TYPE}) reported an error." | tee -a "${LOG_FILE}"; fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] - ----------------------------------------------------" | tee -a "${LOG_FILE}"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] - Host Script (05_xpehh_processing.sh for ${SIM_TYPE}) Finished: $(date)" | tee -a "${LOG_FILE}"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] - ----------------------------------------------------" | tee -a "${LOG_FILE}"