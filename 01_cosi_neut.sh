#!/bin/bash

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/neutral_sims.log"

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Host Configuration ---
CONFIG_FILE="00config.json"

# Read from JSON using jq
# Check if jq is available
if ! command -v jq &> /dev/null
then
    echo "Host: CRITICAL ERROR: jq command could not be found. Please install jq."
    echo "Host: Attempting to use Python as a fallback to read config..."
    # Python fallbacks (less ideal for multiple values)
    TOTAL_NEUTRAL_SIMULATIONS=$(python3 -c "import json; print(json.load(open('${CONFIG_FILE}'))['neutral_simulation_number'])" 2>/dev/null)
    DEMOGRAPHIC_MODEL_BASENAME=$(python3 -c "import json; print(json.load(open('${CONFIG_FILE}'))['demographic_model'])" 2>/dev/null)
    if [ -z "$TOTAL_NEUTRAL_SIMULATIONS" ] || [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ]; then
        echo "Host: CRITICAL ERROR: Failed to read config using Python fallback. Exiting."
        exit 1
    fi
    echo "Host: Warning: Read config using Python fallback. jq is recommended."
else
    # Use jq (preferred)
    TOTAL_NEUTRAL_SIMULATIONS=$(jq -r '.neutral_simulation_number' "${CONFIG_FILE}")
    DEMOGRAPHIC_MODEL_BASENAME=$(jq -r '.demographic_model' "${CONFIG_FILE}")
fi

# Validate that values were read
if [ -z "$TOTAL_NEUTRAL_SIMULATIONS" ] || [ "$TOTAL_NEUTRAL_SIMULATIONS" = "null" ]; then
    echo "Host: CRITICAL ERROR: 'neutral_simulation_number' not found or null in ${CONFIG_FILE}. Exiting."
    exit 1
fi
if [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ "$DEMOGRAPHIC_MODEL_BASENAME" = "null" ]; then
    echo "Host: CRITICAL ERROR: 'demographic_model' not found or null in ${CONFIG_FILE}. Exiting."
    exit 1
fi


DEMOGRAPHIC_MODEL_DIR_REL_PATH="demographic_models" # Standard location
RECOMBINATION_MAP_FILE_AT_ROOT_REL_PATH="recom.recom" # Standard location
NEUTRAL_OUTPUT_DIR_NAME="neutral_sims" # Standard output dir name
RUNTIME_DIR_NAME="runtime" # Standard runtime dir name

DOCKER_IMAGE="docker.io/tx56/cosi"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
echo "----------------------------------------------------"
echo "Host Script (01_cosi_neut.sh) Started: $(date)"
echo "Host: Reading configuration from ${CONFIG_FILE}"
echo "Host: Total Neutral Simulations: ${TOTAL_NEUTRAL_SIMULATIONS}"
echo "Host: Demographic Model Basename: ${DEMOGRAPHIC_MODEL_BASENAME}"
echo "----------------------------------------------------"
echo "Host: Script execution directory: ${HOST_CWD}"
DEMOGRAPHIC_MODEL_FILE_REL_PATH="${DEMOGRAPHIC_MODEL_DIR_REL_PATH}/${DEMOGRAPHIC_MODEL_BASENAME}"
echo "Host: Expected demographic model file at: ${HOST_CWD}/${DEMOGRAPHIC_MODEL_FILE_REL_PATH}"
echo "Host: Expected recombination map file at: ${HOST_CWD}/${RECOMBINATION_MAP_FILE_AT_ROOT_REL_PATH}"

if [ ! -f "${CONFIG_FILE}" ]; then
    echo "Host: CRITICAL ERROR: Configuration file '${CONFIG_FILE}' NOT FOUND. Exiting."
    exit 1
fi
if [ ! -f "${DEMOGRAPHIC_MODEL_FILE_REL_PATH}" ]; then
    echo "Host: CRITICAL ERROR: Demographic model file '${DEMOGRAPHIC_MODEL_FILE_REL_PATH}' (derived from config) NOT FOUND. Exiting."
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
echo "Host: Starting Docker container for neutral simulations..."

# --- Docker Execution ---
docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_DEMO_MODEL_REL_PATH="${DEMOGRAPHIC_MODEL_FILE_REL_PATH}" \
    -e CONTAINER_TOTAL_NEUTRAL_SIMS="${TOTAL_NEUTRAL_SIMULATIONS}" \
    -e CONTAINER_NEUTRAL_OUTPUT_DIR_NAME="${NEUTRAL_OUTPUT_DIR_NAME}" \
    -e CONTAINER_RUNTIME_DIR_NAME="${RUNTIME_DIR_NAME}" \
    "$DOCKER_IMAGE" /bin/bash <<'EOF_INNER'

# --- Container Initialization & Sanity Checks ---
echo_container() { echo "Container: $1"; }

current_coalescent_pid=""
cleanup_and_exit() {
    echo_container "Caught signal! Cleaning up..."
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
echo_container "Container Script (01_cosi_neut) Started: $(date)"
echo_container "----------------------------------------------------"
echo_container "CWD is $(pwd)"
echo_container "Received DEMO_MODEL_REL_PATH: [${CONTAINER_DEMO_MODEL_REL_PATH}]"
echo_container "Received TOTAL_NEUTRAL_SIMS: [${CONTAINER_TOTAL_NEUTRAL_SIMS}]"
echo_container "Received NEUTRAL_OUTPUT_DIR_NAME: [${CONTAINER_NEUTRAL_OUTPUT_DIR_NAME}]"
echo_container "Received RUNTIME_DIR_NAME: [${CONTAINER_RUNTIME_DIR_NAME}]"


DEMO_FILE_IN_CONTAINER="./${CONTAINER_DEMO_MODEL_REL_PATH}" # Path relative to /app_data

echo_container "Checking Demo model: [${DEMO_FILE_IN_CONTAINER}]"
if [ ! -f "${DEMO_FILE_IN_CONTAINER}" ]; then
    echo_container "CRITICAL ERROR: Demo model NOT FOUND at ${DEMO_FILE_IN_CONTAINER}. Exiting."
    exit 1
fi
echo_container "Demo model FOUND."

EXPECTED_RECOM_MAP_BY_PAR="./recom.recom" # As .par file refers to ../recom.recom from its subdir
echo_container "Checking Recom map (as expected by .par file from its location): [${EXPECTED_RECOM_MAP_BY_PAR}]"
if [ ! -f "${EXPECTED_RECOM_MAP_BY_PAR}" ]; then
    echo_container "CRITICAL ERROR: Recom map NOT FOUND at '${EXPECTED_RECOM_MAP_BY_PAR}' (expected by .par file). Exiting."
    exit 1
fi
echo_container "Recom map FOUND at '${EXPECTED_RECOM_MAP_BY_PAR}'."

# --- Simulation Function ---
run_simulation() {
    local sim_id=$1
    # Output suffix for TPED files: neut.hap.0, neut.hap.1 etc.
    # If coalescent creates sub-population files like neut.hap.0_0_1.tped,
    # this output_suffix is for the "base" part, coalescent handles the rest.
    local output_suffix_base="hap.${sim_id}" 

    echo_container "Starting simulation for ID ${sim_id}, base output suffix ${output_suffix_base}"
    mkdir -p "./${CONTAINER_NEUTRAL_OUTPUT_DIR_NAME}"
    mkdir -p "./${CONTAINER_RUNTIME_DIR_NAME}"

    local attempt=0
    local max_attempts=3
    local success=false

    while [ "$success" = false ] && [ $attempt -lt $max_attempts ]; do
        attempt=$((attempt + 1))
        echo_container "Attempt ${attempt}/${max_attempts} for sim ID ${sim_id}..."

        local start_time=$(date +%s)
        # The --tped argument forms the *prefix* for output files.
        # If coalescent is set up to output multiple populations, it will append to this.
        # E.g., --tped ./neutral_sims/neut.hap.0 might result in ./neutral_sims/neut.hap.0_0_1.tped etc.
        env COSI_NEWSIM=1 COSI_MAXATTEMPTS=1000000 coalescent \
            -p "${DEMO_FILE_IN_CONTAINER}" \
            -v \
            --drop-singletons .25 \
            --tped "./${CONTAINER_NEUTRAL_OUTPUT_DIR_NAME}/neut.${output_suffix_base}" \
            -n 1 -M -r 0 &
            # No explicit -g for recom map; relying on the .par file
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
        # This CSV records one entry per main simulation ID run (e.g., 0, 1)
        echo "sim_id,${sim_id},neut_runtime,${runtime_seconds},seconds" >> "./${CONTAINER_RUNTIME_DIR_NAME}/cosi.neut.runtime.csv"
        return 0
    else
        echo_container "Sim ID ${sim_id} failed after ${max_attempts} attempts."
        return 1
    fi
}

# --- Main Simulation Loop ---
upper_limit_for_seq=$((${CONTAINER_TOTAL_NEUTRAL_SIMS} - 1)) # If 2 sims, loop 0 to 1

if [ "${upper_limit_for_seq}" -lt 0 ]; then
    echo_container "TOTAL_NEUTRAL_SIMS is ${CONTAINER_TOTAL_NEUTRAL_SIMS}, resulting in no simulations to run."
else
    echo_container "Starting simulation loop from ID 0 to ${upper_limit_for_seq}..."
    for i in $(seq 0 1 ${upper_limit_for_seq}); do
        if ! run_simulation "${i}"; then
            echo_container "Simulation for ID ${i} failed critically. Continuing with next..."
            # Optionally: exit 1 to stop all further processing if one sim fails
        fi
    done
fi

echo_container "Container Script (01_cosi_neut) Finished: $(date)"
echo_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
echo "Host: Docker container finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then
    echo "Host: Script likely interrupted by user (Ctrl+C)."
elif [ ${docker_exit_status} -ne 0 ]; then
    echo "Host: Docker container reported an error."
fi
echo "----------------------------------------------------"
echo "Host Script (01_cosi_neut.sh) Finished: $(date)"
echo "----------------------------------------------------"