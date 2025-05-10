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
    TOTAL_SELECTED_SIMULATIONS=$(python3 -c "import json; print(json.load(open('${CONFIG_FILE}'))['selected_simulation_number'])" 2>/dev/null)
    DEMOGRAPHIC_MODEL_BASENAME=$(python3 -c "import json; print(json.load(open('${CONFIG_FILE}'))['demographic_model'])" 2>/dev/null)
    SELECTIVE_SWEEP_PARAMS=$(python3 -c "import json; print(json.load(open('${CONFIG_FILE}'))['selective_sweep'])" 2>/dev/null)
    if [ -z "$TOTAL_SELECTED_SIMULATIONS" ] || [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ -z "$SELECTIVE_SWEEP_PARAMS" ]; then
        echo "Host: CRITICAL ERROR: Failed to read config using Python fallback. Exiting."
        exit 1
    fi
    echo "Host: Warning: Read config using Python fallback. jq is recommended."
else
    # Use jq (preferred)
    TOTAL_SELECTED_SIMULATIONS=$(jq -r '.selected_simulation_number' "${CONFIG_FILE}")
    DEMOGRAPHIC_MODEL_BASENAME=$(jq -r '.demographic_model' "${CONFIG_FILE}")
    SELECTIVE_SWEEP_PARAMS=$(jq -r '.selective_sweep' "${CONFIG_FILE}")
fi

# Validate that values were read
if [ -z "$TOTAL_SELECTED_SIMULATIONS" ] || [ "$TOTAL_SELECTED_SIMULATIONS" = "null" ]; then
    echo "Host: CRITICAL ERROR: 'selected_simulation_number' not found or null in ${CONFIG_FILE}. Exiting."
    exit 1
fi
if [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ "$DEMOGRAPHIC_MODEL_BASENAME" = "null" ]; then
    echo "Host: CRITICAL ERROR: 'demographic_model' not found or null in ${CONFIG_FILE}. Exiting."
    exit 1
fi
if [ -z "$SELECTIVE_SWEEP_PARAMS" ] || [ "$SELECTIVE_SWEEP_PARAMS" = "null" ]; then
    echo "Host: CRITICAL ERROR: 'selective_sweep' not found or null in ${CONFIG_FILE}. Exiting."
    exit 1
fi

DEMOGRAPHIC_MODEL_DIR_REL_PATH="demographic_models" # Standard location
RECOMBINATION_MAP_FILE_AT_ROOT_REL_PATH="recom.recom" # Standard location
SELECTED_OUTPUT_DIR_NAME="selected_sims"  # Standard output dir name
RUNTIME_DIR_NAME="runtime" # Standard runtime dir name

DOCKER_IMAGE="docker.io/tx56/cosi"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
echo "----------------------------------------------------"
echo "Host Script (02_cosi_sel.sh) Started: $(date)"
echo "Host: Reading configuration from ${CONFIG_FILE}"
echo "Host: Total Selected Simulations: ${TOTAL_SELECTED_SIMULATIONS}"
echo "Host: Demographic Model Basename: ${DEMOGRAPHIC_MODEL_BASENAME}"
echo "Host: Selective Sweep Parameters: ${SELECTIVE_SWEEP_PARAMS}"
echo "----------------------------------------------------"
echo "Host: Script execution directory: ${HOST_CWD}"
BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH="${DEMOGRAPHIC_MODEL_DIR_REL_PATH}/${DEMOGRAPHIC_MODEL_BASENAME}"
echo "Host: Expected base demographic model file at: ${HOST_CWD}/${BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH}"
echo "Host: Expected recombination map file at: ${HOST_CWD}/${RECOMBINATION_MAP_FILE_AT_ROOT_REL_PATH}"

if [ ! -f "${CONFIG_FILE}" ]; then
    echo "Host: CRITICAL ERROR: Configuration file '${CONFIG_FILE}' NOT FOUND. Exiting."
    exit 1
fi
if [ ! -f "${BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH}" ]; then
    echo "Host: CRITICAL ERROR: Base demographic model file '${BASE_DEMOGRAPHIC_MODEL_FILE_REL_PATH}' (derived from config) NOT FOUND. Exiting."
    exit 1
fi
if [ ! -f "${RECOMBINATION_MAP_FILE_AT_ROOT_REL_PATH}" ]; then
    echo "Host: CRITICAL ERROR: Recombination map file '${RECOMBINATION_MAP_FILE_AT_ROOT_REL_PATH}' NOT FOUND. Exiting."
    exit 1
fi
echo "Host: All required input files FOUND."

if ! docker image inspect "$DOCKER_IMAGE" &> /dev/null; then
    echo "Host: Docker image $DOCKER_IMAGE not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE"; then
        echo "Host: Failed to pull Docker image $DOCKER_IMAGE. Exiting."
        exit 1
    fi
else
    echo "Host: Docker image $DOCKER_IMAGE already exists locally."
fi
echo "Host: Starting Docker container for selected simulations..."

# --- Docker Execution ---
docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_BASE_DEMO_MODEL_DIR_REL_PATH="${DEMOGRAPHIC_MODEL_DIR_REL_PATH}" \
    -e CONTAINER_BASE_DEMO_MODEL_BASENAME="${DEMOGRAPHIC_MODEL_BASENAME}" \
    -e CONTAINER_SELECTIVE_SWEEP_PARAMS="${SELECTIVE_SWEEP_PARAMS}" \
    -e CONTAINER_TOTAL_SELECTED_SIMS="${TOTAL_SELECTED_SIMULATIONS}" \
    -e CONTAINER_SELECTED_OUTPUT_DIR_NAME="${SELECTED_OUTPUT_DIR_NAME}" \
    -e CONTAINER_RUNTIME_DIR_NAME="${RUNTIME_DIR_NAME}" \
    "$DOCKER_IMAGE" /bin/bash <<'EOF_INNER'

# --- Container Initialization & Sanity Checks ---
echo_container() { echo "Container: $1"; }

current_coalescent_pid=""
cleanup_and_exit() {
    echo_container "Caught signal! Cleaning up..."
    # Note: Temp par file cleanup on signal is tricky here as filename is sim_id specific
    if [ -n "$current_coalescent_pid" ] && kill -0 "$current_coalescent_pid" 2>/dev/null; then
        echo_container "Terminating coalescent process $current_coalescent_pid..."
        kill -TERM "$current_coalescent_pid"; sleep 0.2
        if kill -0 "$current_coalescent_pid" 2>/dev/null; then kill -KILL "$current_coalescent_pid"; fi
    fi
    echo_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM

echo_container "----------------------------------------------------"
echo_container "Container Script (02_cosi_sel) Started: $(date)"
echo_container "----------------------------------------------------"
echo_container "CWD is $(pwd)" 
echo_container "Received BASE_DEMO_MODEL_DIR_REL_PATH: [${CONTAINER_BASE_DEMO_MODEL_DIR_REL_PATH}]"
echo_container "Received BASE_DEMO_MODEL_BASENAME: [${CONTAINER_BASE_DEMO_MODEL_BASENAME}]"
echo_container "Received SELECTIVE_SWEEP_PARAMS: [${CONTAINER_SELECTIVE_SWEEP_PARAMS}]"
echo_container "Received TOTAL_SELECTED_SIMS: [${CONTAINER_TOTAL_SELECTED_SIMS}]"
echo_container "Received SELECTED_OUTPUT_DIR_NAME: [${CONTAINER_SELECTED_OUTPUT_DIR_NAME}]"
echo_container "Received RUNTIME_DIR_NAME: [${CONTAINER_RUNTIME_DIR_NAME}]"

BASE_DEMO_FILE_IN_CONTAINER="./${CONTAINER_BASE_DEMO_MODEL_DIR_REL_PATH}/${CONTAINER_BASE_DEMO_MODEL_BASENAME}"
DEMO_MODEL_DIR_IN_CONTAINER="./${CONTAINER_BASE_DEMO_MODEL_DIR_REL_PATH}" # e.g., ./demographic_models

echo_container "Checking Base Demo model: [${BASE_DEMO_FILE_IN_CONTAINER}]"
if [ ! -f "${BASE_DEMO_FILE_IN_CONTAINER}" ]; then
    echo_container "CRITICAL ERROR: Base Demo model NOT FOUND at ${BASE_DEMO_FILE_IN_CONTAINER}. Exiting."
    exit 1
fi
echo_container "Base Demo model FOUND."

EXPECTED_RECOM_MAP_BY_PAR="./recom.recom" 
echo_container "Checking Recom map (as expected by base .par file): [${EXPECTED_RECOM_MAP_BY_PAR}]"
if [ ! -f "${EXPECTED_RECOM_MAP_BY_PAR}" ]; then
    echo_container "CRITICAL ERROR: Recom map NOT FOUND at '${EXPECTED_RECOM_MAP_BY_PAR}' (expected by .par file). Exiting."
    exit 1
fi
echo_container "Recom map FOUND at '${EXPECTED_RECOM_MAP_BY_PAR}'."

# Fixed filename for COSI_SAVE_SAMPLED output, as per original script's intent
COSI_SAMPLED_LOCI_FILE="./${CONTAINER_RUNTIME_DIR_NAME}/cosi.sel.1.sampled_loci.csv"

# --- Simulation Function ---
run_selected_simulation() {
    local sim_id=$1 
    # Output suffix for TPED files: sel.hap.0, sel.hap.1 etc.
    # Coalescent will append _0_X.tped if it outputs multiple populations.
    local output_suffix_base="hap.${sim_id}"
    
    local temp_par_filename="${CONTAINER_BASE_DEMO_MODEL_BASENAME}-${sim_id}.par" # e.g. jv_default...par-0.par
    local temp_par_filepath="${DEMO_MODEL_DIR_IN_CONTAINER}/${temp_par_filename}" # e.g. ./demographic_models/jv_default...par-0.par

    echo_container "Starting selected simulation for ID ${sim_id}, base output suffix ${output_suffix_base}"
    echo_container "Base .par: ${BASE_DEMO_FILE_IN_CONTAINER}"
    echo_container "Temporary .par: ${temp_par_filepath}"

    mkdir -p "./${CONTAINER_SELECTED_OUTPUT_DIR_NAME}" 
    mkdir -p "./${CONTAINER_RUNTIME_DIR_NAME}"

    cp "${BASE_DEMO_FILE_IN_CONTAINER}" "${temp_par_filepath}"
    if [ $? -ne 0 ]; then
        echo_container "ERROR: Failed to copy base .par file to ${temp_par_filepath}. Skipping sim ${sim_id}."
        return 1
    fi

    # Append sweep parameters. Ensure no unwanted shell expansion if params contain $ or `
    # Using printf is safer for arbitrary strings.
    printf "%s\n" "${CONTAINER_SELECTIVE_SWEEP_PARAMS}" >> "${temp_par_filepath}"
    if [ $? -ne 0 ]; then
        echo_container "ERROR: Failed to append sweep params to ${temp_par_filepath}. Skipping sim ${sim_id}."
        rm "${temp_par_filepath}" 
        return 1
    fi
    echo_container "Content of temp .par file (${temp_par_filepath}) after adding sweep (last 5 lines):"
    tail -n 5 "${temp_par_filepath}" 

    local attempt=0
    local max_attempts=3
    local success=false

    while [ "$success" = false ] && [ $attempt -lt $max_attempts ]; do
        attempt=$((attempt + 1))
        echo_container "Attempt ${attempt}/${max_attempts} for sim ID ${sim_id}..."
        
        local start_time=$(date +%s)
        
        env COSI_SAVE_SAMPLED="${COSI_SAMPLED_LOCI_FILE}" COSI_NEWSIM=1 COSI_MAXATTEMPTS=1000000 coalescent \
            -p "${temp_par_filepath}" \
            -v \
            --drop-singletons .25 \
            --tped "./${CONTAINER_SELECTED_OUTPUT_DIR_NAME}/sel.${output_suffix_base}" \
            -n 1 -M -r 0 &
        current_coalescent_pid=$!

        local timeout_seconds=35 
        local elapsed_seconds=0
        while kill -0 $current_coalescent_pid 2>/dev/null; do
            sleep 1
            elapsed_seconds=$((elapsed_seconds + 1))
            if [ $elapsed_seconds -ge $timeout_seconds ]; then
                echo_container "Timeout: Sim ID ${sim_id} (PID ${current_coalescent_pid}) exceeded ${timeout_seconds}s. Killing."
                kill -9 $current_coalescent_pid
                wait $current_coalescent_pid 2>/dev/null
                break
            fi
        done
        
        if wait $current_coalescent_pid; then
            echo_container "Sim ID ${sim_id} (attempt ${attempt}) completed successfully."
            success=true
        else
            local exit_code=$?
            echo_container "Sim ID ${sim_id} (attempt ${attempt}) failed (exit code ${exit_code}) or timed out. Retrying if attempts left..."
            current_coalescent_pid=""
            if [ $attempt -lt $max_attempts ]; then sleep 2; fi
        fi
    done

    if [ "$success" = true ]; then
        local end_time=$(date +%s)
        local runtime_seconds=$((end_time - start_time))
        echo "sim_id,${sim_id},sel_runtime,${runtime_seconds},seconds" >> "./${CONTAINER_RUNTIME_DIR_NAME}/cosi.sel.runtime.csv"
        echo "${sim_id}" >> "${COSI_SAMPLED_LOCI_FILE}"
    fi

    echo_container "Removing temporary .par file: ${temp_par_filepath}"
    rm "${temp_par_filepath}"
    if [ $? -ne 0 ]; then
        echo_container "WARNING: Failed to remove temporary .par file ${temp_par_filepath}."
    fi

    if [ "$success" = true ]; then
        return 0
    else
        echo_container "Sim ID ${sim_id} failed after ${max_attempts} attempts."
        return 1
    fi
}

# --- Main Simulation Loop ---
upper_limit_for_seq=$((${CONTAINER_TOTAL_SELECTED_SIMS} - 1)) # If 2 sims, loop 0 to 1

if [ "${upper_limit_for_seq}" -lt 0 ]; then
    echo_container "TOTAL_SELECTED_SIMS is ${CONTAINER_TOTAL_SELECTED_SIMS}, resulting in no simulations to run."
else
    echo_container "Starting selected simulation loop from ID 0 to ${upper_limit_for_seq}..."
    for i in $(seq 0 1 ${upper_limit_for_seq}); do 
        if ! run_selected_simulation "${i}"; then
            echo_container "Selected simulation for ID ${i} failed critically. Continuing with next..."
        fi
    done
fi

echo_container "Container Script (02_cosi_sel) Finished: $(date)"
echo_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
echo "Host: Docker container (Selected Sims) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then
    echo "Host: Script (Selected Sims) likely interrupted by user (Ctrl+C)."
elif [ ${docker_exit_status} -ne 0 ]; then
    echo "Host: Docker container (Selected Sims) reported an error."
fi
echo "----------------------------------------------------"
echo "Host Script (02_cosi_sel.sh) Finished: $(date)"
echo "----------------------------------------------------"