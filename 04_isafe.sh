#!/bin/bash

# --- Python Helper Script Content ---
PYTHON_HELPER_SCRIPT_NAME="process_and_run_isafe_core.py" 

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/isafe_processing.log"

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Host Configuration ---
CONFIG_FILE="00config.json"

if ! command -v jq &> /dev/null
then
    echo "Host: CRITICAL ERROR: jq command could not be found. Please install jq."
    echo "Host: Attempting to use Python as a fallback to read config..."
    POP1_TARGET=$(python3 -c "import json; print(json.load(open('${CONFIG_FILE}'))['selected_pop'])" 2>/dev/null)
    if [ -z "$POP1_TARGET" ]; then
        echo "Host: CRITICAL ERROR: Failed to read 'selected_pop' for POP1_TARGET using Python fallback. Exiting."
        exit 1
    fi
    echo "Host: Warning: Read config using Python fallback. jq is recommended."
else
    POP1_TARGET=$(jq -r '.selected_pop' "${CONFIG_FILE}")
fi

if [ -z "$POP1_TARGET" ] || [ "$POP1_TARGET" = "null" ]; then
    echo "Host: CRITICAL ERROR: 'selected_pop' not found or null in ${CONFIG_FILE}, needed for POP1_TARGET. Exiting."
    exit 1
fi

# Directory and Path configurations
SELECTED_SIM_INPUT_DIR="selected_sims"     
ISAFE_AND_SELSCAN_SEL_OUTPUT_DIR="one_pop_stats_sel" 
RUNTIME_DIR="runtime"                       
INPUT_CSV_FOR_ISAFE_BASENAME="nsl.sel.runtime.csv" 

HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
echo "----------------------------------------------------"
echo "Host Script (04_isafe.sh - Linear) Started: $(date)"
echo "Host: Reading configuration from ${CONFIG_FILE}"
echo "Host: Target Population ID for TPED construction (from .selected_pop): ${POP1_TARGET}"
echo "----------------------------------------------------"
echo "Host: Script execution directory: ${HOST_CWD}"
echo "Host: TPED input directory: ${HOST_CWD}/${SELECTED_SIM_INPUT_DIR}"
echo "Host: iSAFE output directory: ${HOST_CWD}/${ISAFE_AND_SELSCAN_SEL_OUTPUT_DIR}"
echo "Host: Runtime stats directory: ${HOST_CWD}/${RUNTIME_DIR}"
INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_FOR_ISAFE_BASENAME}" 
echo "Host: Input CSV for sim_ids: ${INPUT_CSV_FILE_HOST}"

if [ ! -f "${CONFIG_FILE}" ]; then
    echo "Host: CRITICAL ERROR: Configuration file '${CONFIG_FILE}' NOT FOUND. Exiting."
    exit 1
fi
mkdir -p "${ISAFE_AND_SELSCAN_SEL_OUTPUT_DIR}" 
mkdir -p "${RUNTIME_DIR}" 

if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then
    echo "Host: CRITICAL ERROR: Input CSV file '${INPUT_CSV_FILE_HOST}' not found. This script expects it to be present. Exiting."
    exit 1
fi

numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then
    echo "Host: WARNING: Input CSV file '${INPUT_CSV_FILE_HOST}' exists but no rows with a numeric second field (sim_id) were found."
fi

# Check for local isafe executable
if ! command -v isafe &> /dev/null; then
    echo "Host: CRITICAL ERROR: 'isafe' command not found. Please install iSAFE and ensure it's in your PATH. Exiting."
    exit 1
fi
echo "Host: Local 'isafe' executable found."
echo "Host: Starting local iSAFE processing (linear)..."

# --- Create Python Helper Script ---
cat > "./${PYTHON_HELPER_SCRIPT_NAME}" <<'PYTHON_EOF'
import subprocess
import pandas as pd
import os
import random
import sys
import json

def is_numeric(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def process_file(input_file, output_file):
    print(f"Python: Processing {input_file} to {output_file}")
    with open(input_file, 'r') as file, open(output_file, 'w') as new_file:
        for line_num, line in enumerate(file):
            parts = line.strip().split() 
            if len(parts) < 4:
                print(f"Python: Warning: Line {line_num+1} in {input_file} has less than 4 columns: '{line.strip()}'")
                continue
            columns = parts[3:] 
            if not columns:
                print(f"Python: Warning: Line {line_num+1} in {input_file} has no allele data after first 3 columns: '{line.strip()}'")
                continue
            if is_numeric(columns[0]):
                columns[0] = str(int(float(columns[0]))) 
            new_file.write('\t'.join(columns) + '\n')

def extract_10_percent_pairs(tped_file):
    print(f"Python: Extracting 10% pairs from {tped_file}")
    with open(tped_file, 'r') as file:
        lines = file.readlines()
    selected_columns_for_all_snps = []
    for line in lines:
        columns = line.strip().split()[4:]  
        pairs = [columns[i:i+2] for i in range(0, len(columns), 2)]
        num_pairs = len(pairs)
        if num_pairs == 0:
            selected_columns_for_all_snps.append([])
            continue
        num_to_select = max(1, int(0.1 * num_pairs))
        if num_to_select > num_pairs : 
             num_to_select = num_pairs
        selected_pairs = random.sample(pairs, num_to_select)
        selected_columns_for_all_snps.append([item for sublist in selected_pairs for item in sublist])
    return selected_columns_for_all_snps

def add_extra_columns(hap_file, tped_file_for_extra_cols):
    print(f"Python: Adding extra columns to {hap_file} using {tped_file_for_extra_cols}")
    extra_columns_per_snp = extract_10_percent_pairs(tped_file_for_extra_cols)
    with open(hap_file, 'r') as hf:
        original_hap_lines = [line.strip() for line in hf.readlines()]
    if len(original_hap_lines) != len(extra_columns_per_snp):
        print(f"Python: Error: Mismatch in line count between {hap_file} ({len(original_hap_lines)}) and extracted columns from {tped_file_for_extra_cols} ({len(extra_columns_per_snp)}). Aborting add_extra_columns.")
        return False
    combined_lines = []
    for i, orig_line_data in enumerate(original_hap_lines):
        extra_cols_for_this_snp_str = "\t".join(extra_columns_per_snp[i])
        if extra_cols_for_this_snp_str:
             combined_lines.append(f"{orig_line_data}\t{extra_cols_for_this_snp_str}")
        else:
             combined_lines.append(orig_line_data)
    with open(hap_file, 'w') as hf_out:
        for line in combined_lines:
            hf_out.write(line + '\n')
    return True

def run_isafe_cmd(input_hap_file, output_prefix_isafe):
    command = f"isafe --input {input_hap_file} --output {output_prefix_isafe} --format hap"
    print(f"Python: Executing iSAFE: {command}")
    try:
        subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"Python: iSAFE completed successfully for {input_hap_file}")
    except subprocess.CalledProcessError as e:
        print(f"Python: Error running iSAFE for {input_hap_file}.")
        print(f"Python: Command: {e.cmd}")
        print(f"Python: Return code: {e.returncode}")
        print(f"Python: stdout: {e.stdout}")
        print(f"Python: stderr: {e.stderr}")
        return False
    return True

def main_isafe_processing(sim_id_str, tped_input_dir, isafe_target_output_dir, pop1_val):
    print(f"Python: main_isafe_processing called for sim_id: {sim_id_str}")
    tped_file_1 = f"{tped_input_dir}/sel.hap.{sim_id_str}_0_{pop1_val}.tped"
    if not os.path.exists(tped_file_1):
        print(f"Python: Skipping {sim_id_str} as TPED file {tped_file_1} does not exist.")
        return False
    
    output_hap_file = f"{isafe_target_output_dir}/{sim_id_str}.hap" 
    isafe_output_prefix = f"{isafe_target_output_dir}/{sim_id_str}"

    print(f"Python: TPED file: {tped_file_1}")
    print(f"Python: Output .hap file: {output_hap_file}")
    print(f"Python: iSAFE output prefix: {isafe_output_prefix}")

    process_file(tped_file_1, output_hap_file)
    if not add_extra_columns(output_hap_file, tped_file_1):
        print(f"Python: Failed to add extra columns for {sim_id_str}. Cleaning up {output_hap_file}.")
        if os.path.exists(output_hap_file): os.remove(output_hap_file)
        return False
    if not run_isafe_cmd(output_hap_file, isafe_output_prefix):
        print(f"Python: iSAFE run failed for {sim_id_str}. Intermediate .hap file {output_hap_file} may remain.")
        return False
    print(f"Python: Cleaning up intermediate file: {output_hap_file}")
    if os.path.exists(output_hap_file):
        os.remove(output_hap_file)
    return True

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python <script_name>.py <sim_id> <tped_input_dir> <isafe_target_output_dir> <pop1_val>")
        sys.exit(1)
    sim_id_arg = sys.argv[1]
    tped_dir_arg = sys.argv[2]
    isafe_out_dir_arg = sys.argv[3] 
    pop1_arg = sys.argv[4]
    if main_isafe_processing(sim_id_arg, tped_dir_arg, isafe_out_dir_arg, pop1_arg):
        sys.exit(0)
    else:
        sys.exit(1)
PYTHON_EOF
chmod +x "./${PYTHON_HELPER_SCRIPT_NAME}"
echo "Host: Python helper script created and made executable."

cleanup_host_script() {
    echo "Host: Caught signal! Cleaning up Python helper script..."
    rm -f "./${PYTHON_HELPER_SCRIPT_NAME}"
    echo "Host: Exiting due to signal."
    exit 130
}
trap cleanup_host_script INT TERM

# --- Main Execution (Local Linear Processing) ---
echo "Host: Reading all sim_ids from ${INPUT_CSV_FILE_HOST} for iSAFE processing..."

mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_HOST}" | sort -un)

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    echo "Host: No valid sim_ids found in ${INPUT_CSV_FILE_HOST}. Nothing to process for iSAFE."
else
    echo "Host: Found unique sim_ids to process with iSAFE (sorted): ${sim_ids_to_run[*]}"
    for sim_id_val in "${sim_ids_to_run[@]}"; do
        echo "Host: --- Starting iSAFE processing for sim_id: ${sim_id_val} ---"
        start_time_isafe=$(date +%s)
        
        python "./${PYTHON_HELPER_SCRIPT_NAME}" \
            "${sim_id_val}" \
            "./${SELECTED_SIM_INPUT_DIR}" \
            "./${ISAFE_AND_SELSCAN_SEL_OUTPUT_DIR}" \
            "${POP1_TARGET}"
        
        python_exit_status=$?
        end_time_isafe=$(date +%s)
        runtime_isafe=$((end_time_isafe - start_time_isafe))

        if [ $python_exit_status -eq 0 ]; then
            echo "Host: Python script completed successfully for sim_id ${sim_id_val}."
            echo "sim_id,${sim_id_val},pop_id,${POP1_TARGET},isafe_runtime,${runtime_isafe},seconds,status,success" >> "./${RUNTIME_DIR}/isafe.runtime.csv"
        else
            echo "Host: Python script FAILED for sim_id ${sim_id_val} with exit status ${python_exit_status}."
            echo "sim_id,${sim_id_val},pop_id,${POP1_TARGET},isafe_runtime,${runtime_isafe},seconds,status,failed_python_exit_${python_exit_status}" >> "./${RUNTIME_DIR}/isafe.runtime.csv"
        fi
        echo "Host: iSAFE processing for sim_id ${sim_id_val} took ${runtime_isafe}s."
        echo "Host: --- Finished iSAFE processing for sim_id: ${sim_id_val} ---"
    done
    echo "Host: All sim_ids from CSV processed by iSAFE."
fi

rm -f "./${PYTHON_HELPER_SCRIPT_NAME}"
echo "Host: Python helper script removed."

echo "Host: iSAFE processing for selection simulations finished."
echo "----------------------------------------------------"
echo "Host Script (04_isafe.sh - Linear) Finished: $(date)"
echo "----------------------------------------------------"
exit 0