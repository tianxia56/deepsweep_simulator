#!/bin/bash

# This script computes FST and ΔDAF for SELECTION simulations using Python.

# --- Python Helper Script Name ---
PYTHON_FST_DELDAF_SCRIPT_NAME="compute_fst_deldaf_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/fst_deldaf_processing_sel_py.log" # Indicate Python version in log

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

TPED_INPUT_DIR="selected_sims"
INPUT_CSV_BASENAME="nsl.sel.runtime.csv" 
PATH_PREFIX_FOR_FILES="sel"
OUTPUT_STATS_DIR="two_pop_stats" 
RUNTIME_DIR="runtime"
DOCKER_IMAGE_PY="docker.io/tx56/deepsweep_simulator:latest" # Assumed to have Python
HOST_CWD=$(pwd)

log_message "INFO" "Host Script (07_fst_deldaf_processing.sh for SELECTION with Python) Started: $(date)"
log_message "INFO" "Reading configuration from ${CONFIG_FILE}"
log_message "INFO" "Reference Pop (pop1): ${POP1_REF}, All Pop IDs: ${POP_IDS_ARRAY[*]}"
log_message "INFO" "TPED Input Dir: ${TPED_INPUT_DIR}, Output Stats Dir: ${OUTPUT_STATS_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Config file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${OUTPUT_STATS_DIR}"; mkdir -p "${RUNTIME_DIR}"
INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV '${INPUT_CSV_FILE_HOST}' has no numeric sim_ids."; fi

if ! docker image inspect "$DOCKER_IMAGE_PY" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PY} not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_PY"; then log_message "ERROR" "Failed to pull Docker image ${DOCKER_IMAGE_PY}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PY} already exists locally."
fi
log_message "INFO" "Starting Docker container for FST/DelDAF processing (SELECTION with Python)..."

docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_POP1_REF="${POP1_REF}" \
    -e CONTAINER_POP_IDS_STR="${POP_IDS_ARRAY[*]}" \
    -e CONTAINER_PATH_PREFIX="${PATH_PREFIX_FOR_FILES}" \
    -e CONTAINER_TPED_INPUT_DIR="${TPED_INPUT_DIR}" \
    -e CONTAINER_OUTPUT_STATS_DIR="${OUTPUT_STATS_DIR}" \
    -e CONTAINER_RUNTIME_DIR="${RUNTIME_DIR}" \
    -e CONTAINER_INPUT_CSV_BASENAME="${INPUT_CSV_BASENAME}" \
    -e PYTHON_FST_DELDAF_SCRIPT_NAME="${PYTHON_FST_DELDAF_SCRIPT_NAME}" \
    "$DOCKER_IMAGE_PY" /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 

cleanup_and_exit() {
    log_container "Caught signal! Cleaning up temporary Python script..."
    rm -f "./${PYTHON_FST_DELDAF_SCRIPT_NAME}"
    log_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM
log_container "----------------------------------------------------"
log_container "Container Script (FST/DelDAF with Python for ${CONTAINER_PATH_PREFIX}) Started: $(date)"
log_container "----------------------------------------------------"
read -r -a CONTAINER_POP_IDS_ARRAY_INTERNAL <<< "${CONTAINER_POP_IDS_STR}"

# Create the Python helper script inside the container
cat > "./${PYTHON_FST_DELDAF_SCRIPT_NAME}" <<'PYTHON_SCRIPT_EOF'
import sys
import os
import math # For isnan

def parse_tped_line(line_str):
    """Parses a single TPED line. Returns (chrom, snp_id, gen_dist, phys_pos, alleles_list) or None."""
    parts = line_str.strip().split()
    if len(parts) < 4: # Need at least chr, id, gen_dist, phys_pos
        return None
    chrom = parts[0]
    snp_id = parts[1]
    gen_dist = parts[2] # Usually 0 for simulations if not specified
    phys_pos = parts[3]
    alleles = parts[4:] # List of allele strings ('0' or '1')
    return chrom, snp_id, gen_dist, phys_pos, alleles

def calculate_daf_for_pop(tped_file_path):
    """Calculates DAF for each SNP in a TPED file for one population."""
    site_dafs = []
    site_positions = [] # To store corresponding physical positions
    site_snp_ids = []   # To store corresponding SNP IDs

    if not os.path.exists(tped_file_path):
        print(f"Python: ERROR - TPED file {tped_file_path} not found for DAF calculation.")
        return None, None, None

    print(f"Python: Reading TPED {tped_file_path} for DAF calculation...")
    with open(tped_file_path, 'r') as f:
        for line_num, line in enumerate(f):
            parsed_line = parse_tped_line(line)
            if parsed_line is None:
                print(f"Python: Warning - Skipping malformed line {line_num+1} in {tped_file_path}")
                continue
            
            _chrom, snp_id, _gen_dist, phys_pos, alleles = parsed_line
            site_positions.append(phys_pos)
            site_snp_ids.append(snp_id)

            if not alleles: # No allele data for this SNP
                site_dafs.append(float('nan')) # Or handle as error
                continue

            derived_allele_count = 0
            total_alleles = 0
            for allele in alleles:
                if allele == '1': # Assuming '1' is derived
                    derived_allele_count += 1
                # Consider only '0' and '1' as valid for DAF calculation
                if allele in ('0', '1'): 
                    total_alleles += 1
            
            if total_alleles == 0:
                site_dafs.append(float('nan')) # Or 0.0 if preferred for monomorphic ancestral
            else:
                site_dafs.append(derived_allele_count / total_alleles)
    return site_positions, site_snp_ids, site_dafs


def compute_fst_scalar(daf1, daf2):
    """Computes FST for a single SNP given DAFs for two populations."""
    if math.isnan(daf1) or math.isnan(daf2):
        return float('nan')
    if not (0 <= daf1 <= 1 and 0 <= daf2 <= 1): # Basic validation
        return float('nan')

    p_mean = (daf1 + daf2) / 2.0
    h_s = (daf1 * (1.0 - daf1) + daf2 * (1.0 - daf2)) / 2.0
    h_t = p_mean * (1.0 - p_mean)

    if h_t == 0:
        return 0.0 # Or float('nan') if undefined FST should be NaN
    
    fst = (h_t - h_s) / h_t
    return fst if not math.isnan(fst) else float('nan') # Ensure NaN propagation

def format_value_py(x):
    if math.isnan(x):
        return "NA"
    if abs(x) < 1e-6 and abs(x) > 0: # Effectively zero, but not exactly
        return f"{x:.4e}"
    if abs(x) < 0.001 and abs(x) > 0:
        return f"{x:.4e}"
    return f"{x:.4f}"

def main_fst_deldaf(sim_id, pop1_ref_str, all_pop_ids_str, path_prefix, tped_input_dir, output_stats_dir):
    pop1_ref = int(pop1_ref_str)
    all_pop_ids = [int(p) for p in all_pop_ids_str.split()]

    print(f"Python: Processing FST/DelDAF for sim_id={sim_id}, pop1_ref={pop1_ref}, path_prefix={path_prefix}")
    print(f"Python: All population IDs to consider: {all_pop_ids}")

    all_pops_daf_data = {} # Store DAFs for each pop_id: {pop_id: [dafs_for_snps]}
    positions = None
    snp_ids = None # Store SNP IDs from the reference population's TPED

    # Read DAFs for all populations
    for pop_id in all_pop_ids:
        tped_file = os.path.join(tped_input_dir, f"{path_prefix}.hap.{sim_id}_0_{pop_id}.tped")
        current_pos, current_snp_ids, current_dafs = calculate_daf_for_pop(tped_file)
        
        if current_dafs is None: # Error reading TPED
            print(f"Python: ERROR - Could not calculate DAF for pop {pop_id}, sim {sim_id}. Aborting for this sim_id.")
            return False 

        if positions is None:
            positions = current_pos
            snp_ids = current_snp_ids
        elif positions != current_pos or snp_ids != current_snp_ids:
            print(f"Python: ERROR - SNP positions/IDs mismatch between TPED files for sim {sim_id}. Pop {pop_id} vs reference.")
            return False
        
        all_pops_daf_data[pop_id] = current_dafs

    if positions is None:
        print(f"Python: ERROR - No position data loaded for sim {sim_id}. Cannot proceed.")
        return False

    num_snps = len(positions)
    results = [] # List of dictionaries, each for a SNP

    daf_pop1_vector = all_pops_daf_data.get(pop1_ref, [float('nan')] * num_snps)

    for i in range(num_snps):
        snp_result = {
            "sim_id": sim_id,
            "pos": positions[i], # SNP ID could be added: "snp_id": snp_ids[i]
            "mean_fst": float('nan'),
            "deldaf": float('nan'),
            "daf_pop1": daf_pop1_vector[i],
            "maf_pop1": float('nan')
        }

        # FST calculations for pop1_ref vs other pops
        fst_values_for_snp = []
        for pop_id_comp in all_pop_ids:
            if pop_id_comp == pop1_ref:
                continue
            
            daf_pop_comp_vector = all_pops_daf_data.get(pop_id_comp, [float('nan')] * num_snps)
            fst_val = compute_fst_scalar(daf_pop1_vector[i], daf_pop_comp_vector[i])
            snp_result[f"fst_{pop1_ref}_vs_{pop_id_comp}"] = fst_val
            if not math.isnan(fst_val):
                fst_values_for_snp.append(fst_val)
        
        if fst_values_for_snp: # If any FST values were calculated
            snp_result["mean_fst"] = sum(fst_values_for_snp) / len(fst_values_for_snp)

        # DelDAF calculation across all populations for this SNP
        current_snp_dafs = [all_pops_daf_data[pid][i] for pid in all_pop_ids if not math.isnan(all_pops_daf_data[pid][i])]
        if current_snp_dafs:
            snp_result["deldaf"] = max(current_snp_dafs) - min(current_snp_dafs)
        
        # MAF for pop1
        if not math.isnan(snp_result["daf_pop1"]):
            snp_result["maf_pop1"] = min(snp_result["daf_pop1"], 1.0 - snp_result["daf_pop1"])
            
        results.append(snp_result)

    # Prepare for output
    output_file_path = os.path.join(output_stats_dir, f"{path_prefix}.{sim_id}_0_{pop1_ref}_fst_deldaf.tsv")
    os.makedirs(output_stats_dir, exist_ok=True)

    header_cols = ["sim_id", "pos", "mean_fst", "deldaf", "daf_pop1", "maf_pop1"]
    # Add individual FST columns to header
    for pop_id_comp in all_pop_ids:
        if pop_id_comp != pop1_ref:
            header_cols.append(f"fst_{pop1_ref}_vs_{pop_id_comp}")
    
    with open(output_file_path, 'w') as outfile:
        outfile.write("\t".join(header_cols) + "\n")
        for snp_data in results:
            row_values = [str(snp_data.get(col, "NA")) for col in header_cols] # Get value or NA
            # Apply formatting to specific columns
            for idx, col_name in enumerate(header_cols):
                if col_name in ["mean_fst", "deldaf", "daf_pop1", "maf_pop1"] or col_name.startswith("fst_"):
                    try:
                        val_to_format = float(snp_data.get(col_name, float('nan')))
                        row_values[idx] = format_value_py(val_to_format)
                    except ValueError: # Already "NA" or other non-float string
                        pass 
            outfile.write("\t".join(row_values) + "\n")
            
    print(f"Python: Finished FST/DelDAF. Output: {output_file_path}")
    return True


if __name__ == "__main__":
    if len(sys.argv) != 7:
        print(f"Usage: python {sys.argv[0]} <sim_id> <pop1_ref_str> <all_pop_ids_str_space_sep> <path_prefix> <tped_input_dir> <output_stats_dir>")
        sys.exit(1)
    
    sim_id_arg = sys.argv[1]
    pop1_ref_arg = sys.argv[2]
    all_pop_ids_arg = sys.argv[3] # Space separated string of pop_ids
    path_prefix_arg = sys.argv[4]
    tped_input_dir_arg = sys.argv[5]
    output_stats_dir_arg = sys.argv[6]
    
    if main_fst_deldaf(sim_id_arg, pop1_ref_arg, all_pop_ids_arg, path_prefix_arg, tped_input_dir_arg, output_stats_dir_arg):
        sys.exit(0)
    else:
        sys.exit(1)
PYTHON_SCRIPT_EOF
chmod +x "./${PYTHON_FST_DELDAF_SCRIPT_NAME}"
log_container "Python FST/DelDAF script created."

INPUT_CSV_FILE_IN_CONTAINER="./${CONTAINER_RUNTIME_DIR}/${CONTAINER_INPUT_CSV_BASENAME}"
if [ ! -f "${INPUT_CSV_FILE_IN_CONTAINER}" ]; then
    log_container "CRITICAL ERROR: Input CSV file '${INPUT_CSV_FILE_IN_CONTAINER}' not found. Exiting."
    exit 1
fi

log_container "Reading all sim_ids from ${INPUT_CSV_FILE_IN_CONTAINER} for FST/DelDAF processing..."
mapfile -t sim_ids_to_run < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_IN_CONTAINER}" | sort -un)

if [ ${#sim_ids_to_run[@]} -eq 0 ]; then
    log_container "No valid sim_ids found in ${INPUT_CSV_FILE_IN_CONTAINER}. Nothing to process."
else
    log_container "Found unique sim_ids for FST/DelDAF: ${sim_ids_to_run[*]}"
    for current_sim_id in "${sim_ids_to_run[@]}"; do
        log_container "--- Starting FST/DelDAF processing for sim_id: ${current_sim_id} ---"
        
        fst_deldaf_start_time=$(date +%s)
        
        python3 "./${PYTHON_FST_DELDAF_SCRIPT_NAME}" \
            "${current_sim_id}" \
            "${CONTAINER_POP1_REF}" \
            "${CONTAINER_POP_IDS_STR}" \
            "${CONTAINER_PATH_PREFIX}" \
            "./${CONTAINER_TPED_INPUT_DIR}" \
            "./${CONTAINER_OUTPUT_STATS_DIR}"
        
        python_exit_status=$?
        fst_deldaf_end_time=$(date +%s)
        fst_deldaf_runtime=$((fst_deldaf_end_time - fst_deldaf_start_time))

        if [ $python_exit_status -eq 0 ]; then
            log_container "Python script for FST/DelDAF completed successfully for sim_id ${current_sim_id}. Runtime: ${fst_deldaf_runtime}s"
            echo "sim_id,${current_sim_id},pop1_ref,${CONTAINER_POP1_REF},fst_deldaf_py_runtime,${fst_deldaf_runtime},seconds,status,success" >> "./${CONTAINER_RUNTIME_DIR}/fst_deldaf.${CONTAINER_PATH_PREFIX}.runtime.csv"
        else
            log_container "ERROR: Python script for FST/DelDAF FAILED for sim_id ${current_sim_id} with exit status ${python_exit_status}."
            echo "sim_id,${current_sim_id},pop1_ref,${CONTAINER_POP1_REF},fst_deldaf_py_runtime,${fst_deldaf_runtime},seconds,status,failed_python_exit_${python_exit_status}" >> "./${CONTAINER_RUNTIME_DIR}/fst_deldaf.${CONTAINER_PATH_PREFIX}.runtime.csv"
        fi
        log_container "--- Finished FST/DelDAF processing for sim_id: ${current_sim_id} ---"
    done
    log_container "All sim_ids from CSV processed for FST/DelDAF."
fi

rm -f "./${PYTHON_FST_DELDAF_SCRIPT_NAME}"
log_container "Python FST/DelDAF helper script removed."

log_container "FST/DelDAF processing for ${CONTAINER_PATH_PREFIX} simulations finished."
log_container "----------------------------------------------------"
log_container "Container Script (FST/DelDAF with Python for ${CONTAINER_PATH_PREFIX}) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
log_message "INFO" "Docker container (FST/DelDAF for SELECTION with Python) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then log_message "INFO" "Script (FST/DelDAF for SELECTION with Python) likely interrupted."; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then log_message "ERROR" "Docker container (FST/DelDAF for SELECTION with Python) reported an error."; fi
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (07_fst_deldaf_processing.sh for SELECTION with Python) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"