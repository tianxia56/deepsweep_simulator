#!/bin/bash

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/selected_sims.log" 

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Host Configuration ---
CONFIG_FILE="00config.json"

# Read from JSON using jq
if ! command -v jq &> /dev/null
then
    echo "Host: CRITICAL ERROR: jq command could not be found. Please install jq."
    echo "Host: Attempting to use Python as a fallback to read config..."
    TOTAL_SELECTED_SIMULATIONS=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('selected_simulation_number'))" "${CONFIG_FILE}" 2>/dev/null)
    DEMOGRAPHIC_MODEL_BASENAME=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('demographic_model'))" "${CONFIG_FILE}" 2>/dev/null)
    SELECTIVE_SWEEP_PARAMS=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('selective_sweep'))" "${CONFIG_FILE}" 2>/dev/null)
    RECOMBINATION_MAP_FILE_PATH=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('recombination_map'))" "${CONFIG_FILE}" 2>/dev/null)
    if [ -z "$TOTAL_SELECTED_SIMULATIONS" ] || [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ -z "$SELECTIVE_SWEEP_PARAMS" ] || [ -z "$RECOMBINATION_MAP_FILE_PATH" ]; then
        echo "Host: CRITICAL ERROR: Failed to read config using Python fallback. Exiting."
        exit 1
    fi
    echo "Host: Warning: Read config using Python fallback. jq is recommended."
else
    TOTAL_SELECTED_SIMULATIONS=$(jq -r '.selected_simulation_number' "${CONFIG_FILE}")
    DEMOGRAPHIC_MODEL_BASENAME=$(jq -r '.demographic_model' "${CONFIG_FILE}")
    SELECTIVE_SWEEP_PARAMS=$(jq -r '.selective_sweep' "${CONFIG_FILE}")
    RECOMBINATION_MAP_FILE_PATH=$(jq -r '.recombination_map' "${CONFIG_FILE}")
fi

# Validate that values were read
if [ -z "$TOTAL_SELECTED_SIMULATIONS" ] || [ "$TOTAL_SELECTED_SIMULATIONS" = "null" ]; then echo "Host: CRITICAL ERROR: 'selected_simulation_number' not found or null."; exit 1; fi
if [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ "$DEMOGRAPHIC_MODEL_BASENAME" = "null" ]; then echo "Host: CRITICAL ERROR: 'demographic_model' not found or null."; exit 1; fi
if [ -z "$SELECTIVE_SWEEP_PARAMS" ] || [ "$SELECTIVE_SWEEP_PARAMS" = "null" ]; then echo "Host: CRITICAL ERROR: 'selective_sweep' not found or null."; exit 1; fi
if [ -z "$RECOMBINATION_MAP_FILE_PATH" ] || [ "$RECOMBINATION_MAP_FILE_PATH" = "null" ]; then echo "Host: CRITICAL ERROR: 'recombination_map' not found or null."; exit 1; fi


DEMOGRAPHIC_MODEL_DIR_REL_PATH="demographic_models"
SELECTED_OUTPUT_DIR_NAME="selected_sims"
RUNTIME_DIR_NAME="runtime"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
echo "----------------------------------------------------"
echo "Host Script (02_cosi_sel.sh) Started: $(date)"
echo "Host: Reading configuration from ${CONFIG_FILE}"
echo "Host: Total Selected Simulations: ${TOTAL_SELECTED_SIMULATIONS}"
echo "Host: Demographic Model Basename: ${DEMOGRAPHIC_MODEL_BASENAME}"
echo "----------------------------------------------------"
echo "Host: Script execution directory: ${HOST_CWD}"

# Extract base name without the .par extension for constructing temp filenames
DEMOGRAPHIC_MODEL_BASE_NAME_NO_EXT=$(basename "${DEMOGRAPHIC_MODEL_BASENAME%.par}")
BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH="${DEMOGRAPHIC_MODEL_DIR_REL_PATH}/${DEMOGRAPHIC_MODEL_BASENAME}"
echo "Host: Expected base demographic model file at: ${HOST_CWD}/${BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH}"

if [ ! -f "${CONFIG_FILE}" ]; then echo "Host: CRITICAL ERROR: Configuration file '${CONFIG_FILE}' NOT FOUND. Exiting."; exit 1; fi
if [ ! -f "${BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH}" ]; then echo "Host: CRITICAL ERROR: Base demographic model file '${BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH}' NOT FOUND. Exiting."; exit 1; fi

# Check if 'coalescent' command exists locally
if ! command -v coalescent &> /dev/null
then
    echo "Host: CRITICAL ERROR: 'coalescent' command could not be found. Please ensure cosi2 is installed and in your PATH. Exiting."
    exit 1
fi
echo "Host: All required input files and local 'coalescent' executable are FOUND."

echo_local() { echo "Host: $1"; }
trap 'cleanup_and_exit' INT TERM
current_coalescent_pid=""
cleanup_and_exit() { 
    echo_local "Received signal, attempting graceful shutdown..."
    if [ -n "$current_coalescent_pid" ] && kill -0 "$current_coalescent_pid" 2>/dev/null; then 
        echo_local "Killing running coalescent process (PID: $current_coalescent_pid)..."
        kill -TERM "$current_coalescent_pid"
        wait "$current_coalescent_pid" 2>/dev/null # Wait for it to actually die
    fi; 
    exit 130; 
}

BASE_DEMO_FILE_LOCAL="./${DEMOGRAPHIC_MODEL_DIR_REL_PATH}/${DEMOGRAPHIC_MODEL_BASENAME}"
DEMO_MODEL_DIR_LOCAL="./${DEMOGRAPHIC_MODEL_DIR_REL_PATH}"
COSI_SAMPLED_LOCI_FILE="./${RUNTIME_DIR_NAME}/cosi.sel.1.sampled_loci.csv"

# --- SIMULATION FUNCTION WITH RETRY/TIMEOUT LOGIC ---
run_selected_simulation() {
    local sim_id=$1 
    local output_suffix_base="hap.${sim_id}"
    
    local temp_par_filename="${DEMOGRAPHIC_MODEL_BASE_NAME_NO_EXT}-${sim_id}.par"
    local temp_par_filepath="${DEMO_MODEL_DIR_LOCAL}/${temp_par_filename}"

    echo_local "Starting selected simulation for ID ${sim_id}, base output suffix ${output_suffix_base}"
    mkdir -p "./${SELECTED_OUTPUT_DIR_NAME}" 
    mkdir -p "./${RUNTIME_DIR_NAME}"

    cp "${BASE_DEMO_FILE_LOCAL}" "${temp_par_filepath}"
    if [ $? -ne 0 ]; then echo_local "ERROR: Failed to copy base .par file to ${temp_par_filepath}."; return 1; fi

    printf "%s\n" "${SELECTIVE_SWEEP_PARAMS}" >> "${temp_par_filepath}"
    if [ $? -ne 0 ]; then echo_local "ERROR: Failed to append sweep params to ${temp_par_filepath}."; rm -f "${temp_par_filepath}"; return 1; fi

    local attempt=0
    local max_attempts=100
    local success=false

    while [ "$success" = false ] && [ $attempt -lt $max_attempts ]; do
        attempt=$((attempt + 1))
        echo_local "Attempt ${attempt}/${max_attempts} for sim ID ${sim_id}..."
        
        local start_time=$(date +%s)
        
        env COSI_SAVE_SAMPLED="${COSI_SAMPLED_LOCI_FILE}" COSI_NEWSIM=1 COSI_MAXATTEMPTS=1000000 coalescent \
            -p "${temp_par_filepath}" \
            -v \
            --drop-singletons .25 \
            --tped "./${SELECTED_OUTPUT_DIR_NAME}/sel.${output_suffix_base}" \
            -n 1 -M -r 0 &
        current_coalescent_pid=$!

        local timeout_seconds=35 
        local elapsed_seconds=0
        while kill -0 $current_coalescent_pid 2>/dev/null; do
            sleep 1
            elapsed_seconds=$((elapsed_seconds + 1))
            if [ $elapsed_seconds -ge $timeout_seconds ]; then
                echo_local "Timeout: Sim ID ${sim_id} (PID ${current_coalescent_pid}) exceeded ${timeout_seconds}s. Killing."
                kill -9 $current_coalescent_pid
                wait $current_coalescent_pid 2>/dev/null
                break
            fi
        done
        
        if wait $current_coalescent_pid; then
            echo_local "Sim ID ${sim_id} (attempt ${attempt}) completed successfully."
            success=true
        else
            local exit_code=$?
            echo_local "Sim ID ${sim_id} (attempt ${attempt}) failed (exit code ${exit_code}) or timed out. Retrying if attempts left..."
            current_coalescent_pid=""
            if [ $attempt -lt $max_attempts ]; then sleep 2; fi
        fi
    done

    if [ "$success" = true ]; then
        local end_time=$(date +%s)
        local runtime_seconds=$((end_time - start_time))
        echo "sim_id,${sim_id},sel_runtime,${runtime_seconds},seconds" >> "./${RUNTIME_DIR_NAME}/cosi.sel.runtime.csv"
        echo "${sim_id}" >> "${COSI_SAMPLED_LOCI_FILE}"
    fi

    rm -f "${temp_par_filepath}" # Clean up the temporary parameter file
    if [ "$success" = true ]; then return 0; else return 1; fi
}
# --- END OF RESTORED LOGIC ---

upper_limit_for_seq=$((${TOTAL_SELECTED_SIMULATIONS} - 1))
for i in $(seq 0 1 ${upper_limit_for_seq}); do 
    if ! run_selected_simulation "${i}"; then 
        echo_local "Selected sim ID ${i} failed critically."
        exit 1 # Exit if a simulation critically fails
    fi; 
done

echo "Host: All selected simulations completed."
exit 0