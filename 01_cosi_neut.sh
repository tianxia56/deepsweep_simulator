#!/bin/bash

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/neutral_sims.log"

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Host Configuration ---
CONFIG_FILE="00config.json"

# Read from JSON using jq
if ! command -v jq &> /dev/null
then
    echo "Host: CRITICAL ERROR: jq command could not be found. Please install jq."
    echo "Host: Attempting to use Python as a fallback to read config..."
    TOTAL_NEUTRAL_SIMULATIONS=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('neutral_simulation_number'))" "${CONFIG_FILE}" 2>/dev/null)
    DEMOGRAPHIC_MODEL_BASENAME=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('demographic_model'))" "${CONFIG_FILE}" 2>/dev/null)
    RECOMBINATION_MAP_FILE_PATH=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('recombination_map'))" "${CONFIG_FILE}" 2>/dev/null)
    if [ -z "$TOTAL_NEUTRAL_SIMULATIONS" ] || [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ -z "$RECOMBINATION_MAP_FILE_PATH" ]; then
        echo "Host: CRITICAL ERROR: Failed to read config using Python fallback. Exiting."
        exit 1
    fi
    echo "Host: Warning: Read config using Python fallback. jq is recommended."
else
    TOTAL_NEUTRAL_SIMULATIONS=$(jq -r '.neutral_simulation_number' "${CONFIG_FILE}")
    DEMOGRAPHIC_MODEL_BASENAME=$(jq -r '.demographic_model' "${CONFIG_FILE}")
    RECOMBINATION_MAP_FILE_PATH=$(jq -r '.recombination_map' "${CONFIG_FILE}")
fi

# Validate that values were read
if [ -z "$TOTAL_NEUTRAL_SIMULATIONS" ] || [ "$TOTAL_NEUTRAL_SIMULATIONS" = "null" ]; then echo "Host: CRITICAL ERROR: 'neutral_simulation_number' not found or null in ${CONFIG_FILE}."; exit 1; fi
if [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ "$DEMOGRAPHIC_MODEL_BASENAME" = "null" ]; then echo "Host: CRITICAL ERROR: 'demographic_model' not found or null in ${CONFIG_FILE}."; exit 1; fi
if [ -z "$RECOMBINATION_MAP_FILE_PATH" ] || [ "$RECOMBINATION_MAP_FILE_PATH" = "null" ]; then echo "Host: CRITICAL ERROR: 'recombination_map' not found or null in ${CONFIG_FILE}."; exit 1; fi


DEMOGRAPHIC_MODEL_DIR_REL_PATH="demographic_models"
NEUTRAL_OUTPUT_DIR_NAME="neutral_sims"
RUNTIME_DIR_NAME="runtime"
DOCKER_IMAGE="quay.io/ilya_broad/dockstore-tool-cosi2:latest"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
echo "----------------------------------------------------"
echo "Host Script (01_cosi_neut.sh) Started: $(date)"
echo "Host: Reading configuration from ${CONFIG_FILE}"
echo "Host: Total Neutral Simulations: ${TOTAL_NEUTRAL_SIMULATIONS}"
echo "Host: Demographic Model Basename: ${DEMOGRAPHIC_MODEL_BASENAME}"
echo "----------------------------------------------------"
echo "Host: Script execution directory: ${HOST_CWD}"
DEMOGRAPHIC_MODEL_FILE_REL_PATH="${DEMOGRAPHIC_MODEL_DIR_REL_PATH}/$(basename "${DEMOGRAPHIC_MODEL_BASENAME}")"
echo "Host: Expected demographic model file at: ${HOST_CWD}/${DEMOGRAPHIC_MODEL_FILE_REL_PATH}"
if [ ! -f "${CONFIG_FILE}" ]; then echo "Host: CRITICAL ERROR: Configuration file '${CONFIG_FILE}' NOT FOUND. Exiting."; exit 1; fi
if [ ! -f "${DEMOGRAPHIC_MODEL_FILE_REL_PATH}" ]; then echo "Host: CRITICAL ERROR: Demographic model file '${DEMOGRAPHIC_MODEL_FILE_REL_PATH}' NOT FOUND. Exiting."; exit 1; fi
echo "Host: All required input files that are checked are FOUND."

if ! docker image inspect "$DOCKER_IMAGE" &> /dev/null; then echo "Host: Pulling $DOCKER_IMAGE..."; docker pull "$DOCKER_IMAGE"; fi
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
echo_container() { echo "Container: $1"; }
trap 'cleanup_and_exit' INT TERM
current_coalescent_pid=""
cleanup_and_exit() { if [ -n "$current_coalescent_pid" ] && kill -0 "$current_coalescent_pid" 2>/dev/null; then kill -TERM "$current_coalescent_pid"; fi; exit 130; }

DEMO_FILE_IN_CONTAINER="./${CONTAINER_DEMO_MODEL_REL_PATH}"
# Recom map check inside container removed as requested. cosi2 will handle it.

# --- SIMULATION FUNCTION WITH RETRY/TIMEOUT LOGIC RESTORED ---
run_simulation() {
    local sim_id=$1
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
        env COSI_NEWSIM=1 COSI_MAXATTEMPTS=1000000 coalescent \
            -p "${DEMO_FILE_IN_CONTAINER}" \
            -v \
            --drop-singletons .25 \
            --tped "./${CONTAINER_NEUTRAL_OUTPUT_DIR_NAME}/neut.${output_suffix_base}" \
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
        echo "sim_id,${sim_id},neut_runtime,${runtime_seconds},seconds" >> "./${CONTAINER_RUNTIME_DIR_NAME}/cosi.neut.runtime.csv"
        return 0
    else
        echo_container "Sim ID ${sim_id} failed after ${max_attempts} attempts."
        return 1
    fi
}
# --- END OF RESTORED LOGIC ---

upper_limit_for_seq=$((${CONTAINER_TOTAL_NEUTRAL_SIMS} - 1))
for i in $(seq 0 1 ${upper_limit_for_seq}); do if ! run_simulation "${i}"; then echo_container "Sim ID ${i} failed critically."; fi; done
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
echo "Host: Docker container finished with exit status: ${docker_exit_status}."
exit ${docker_exit_status}