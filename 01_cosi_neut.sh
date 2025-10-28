#!/bin/bash

# This script generates neutral simulated genotypes using cosi2
# based on a specified demographic model, running locally within the
# 'deepsweep_simulator' Conda environment.
# It is designed to replace a previous Docker-based execution flow.

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/neutral_sims.log"

mkdir -p "${LOG_DIR}" || { echo "Host: CRITICAL ERROR: Failed to create log directory '${LOG_DIR}'. Exiting."; exit 1; }
# Redirect all stdout and stderr to a log file, and also to stdout
exec &> >(tee -a "${LOG_FILE}")

# Exit immediately if a command exits with a non-zero status.
set -e
set -o pipefail # Added for robustness in pipelines

echo "----------------------------------------------------"
echo "Host Script (01_cosi_neut.sh) Started: $(date)"
echo "Host: Running in local Conda environment (no Docker)."
echo "----------------------------------------------------"

# --- Host Configuration ---
CONFIG_FILE="00config.json"

# Check for jq and read from JSON. Fallback to Python if jq is not found.
echo "Host: Reading configuration from ${CONFIG_FILE}..."
if ! command -v jq &> /dev/null
then
    echo "Host: Warning: 'jq' command not found. Using Python as a fallback to read config. Please consider installing 'jq' for better performance and reliability."
    TOTAL_NEUTRAL_SIMULATIONS=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('neutral_simulation_number'))" "${CONFIG_FILE}" 2>/dev/null)
    DEMOGRAPHIC_MODEL_BASENAME=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('demographic_model'))" "${CONFIG_FILE}" 2>/dev/null)
    RECOMBINATION_MAP_FILE_PATH=$(python3 -c "import sys, json; print(json.load(open(sys.argv[1])).get('recombination_map'))" "${CONFIG_FILE}" 2>/dev/null)
    if [ -z "$TOTAL_NEUTRAL_SIMULATIONS" ] || [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ -z "$RECOMBINATION_MAP_FILE_PATH" ]; then
        echo "Host: CRITICAL ERROR: Failed to read config using Python fallback. Check '${CONFIG_FILE}' and Python installation. Exiting."
        exit 1
    fi
else
    TOTAL_NEUTRAL_SIMULATIONS=$(jq -r '.neutral_simulation_number' "${CONFIG_FILE}")
    DEMOGRAPHIC_MODEL_BASENAME=$(jq -r '.demographic_model' "${CONFIG_FILE}")
    RECOMBINATION_MAP_FILE_PATH=$(jq -r '.recombination_map' "${CONFIG_FILE}")
fi

# Validate that values were read and are not null
if [ -z "$TOTAL_NEUTRAL_SIMULATIONS" ] || [ "$TOTAL_NEUTRAL_SIMULATIONS" = "null" ]; then echo "Host: CRITICAL ERROR: 'neutral_simulation_number' not found or null in ${CONFIG_FILE}. Exiting."; exit 1; fi
if [ -z "$DEMOGRAPHIC_MODEL_BASENAME" ] || [ "$DEMOGRAPHIC_MODEL_BASENAME" = "null" ]; then echo "Host: CRITICAL ERROR: 'demographic_model' not found or null in ${CONFIG_FILE}. Exiting."; exit 1; fi
if [ -z "$RECOMBINATION_MAP_FILE_PATH" ] || [ "$RECOMBINATION_MAP_FILE_PATH" = "null" ]; then echo "Host: CRITICAL ERROR: 'recombination_map' not found or null in ${CONFIG_FILE}. Exiting."; exit 1; fi

DEMOGRAPHIC_MODEL_DIR_REL_PATH="demographic_models"
NEUTRAL_OUTPUT_DIR_NAME="neutral_sims"
RUNTIME_DIR_NAME="runtime"
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
echo "Host: Total Neutral Simulations: ${TOTAL_NEUTRAL_SIMULATIONS}"
echo "Host: Demographic Model Basename: ${DEMOGRAPHIC_MODEL_BASENAME}"
echo "Host: Script execution directory: ${HOST_CWD}"

# Construct the full path to the demographic model file
# CORRECTED LINE: Removed the extra .par as DEMOGRAPHIC_MODEL_BASENAME already includes it.
DEMOGRAPHIC_MODEL_FILE_PATH="${DEMOGRAPHIC_MODEL_DIR_REL_PATH}/${DEMOGRAPHIC_MODEL_BASENAME}"
echo "Host: Expected demographic model file at: ${HOST_CWD}/${DEMOGRAPHIC_MODEL_FILE_PATH}"

# Validate existence of config and demographic model files
if [ ! -f "${CONFIG_FILE}" ]; then echo "Host: CRITICAL ERROR: Configuration file '${CONFIG_FILE}' NOT FOUND. Exiting."; exit 1; fi
if [ ! -f "${DEMOGRAPHIC_MODEL_FILE_PATH}" ]; then echo "Host: CRITICAL ERROR: Demographic model file '${DEMOGRAPHIC_MODEL_FILE_PATH}' NOT FOUND. Exiting."; exit 1; fi

# Check if 'coalescent' command is available in the PATH
if ! command -v coalescent &> /dev/null; then
    echo "Host: CRITICAL ERROR: 'coalescent' command not found in PATH. Is 'deepsweep_simulator' Conda environment activated and cosi2 installed correctly? Exiting."
    exit 1
fi
echo "Host: All required input files and 'coalescent' executable FOUND."

# Create output directories
mkdir -p "${NEUTRAL_OUTPUT_DIR_NAME}" || { echo "Host: CRITICAL ERROR: Failed to create neutral output directory '${NEUTRAL_OUTPUT_DIR_NAME}'. Exiting."; exit 1; }
mkdir -p "${RUNTIME_DIR_NAME}" || { echo "Host: CRITICAL ERROR: Failed to create runtime directory '${RUNTIME_DIR_NAME}'. Exiting."; exit 1; }
echo "Host: Output directories created."

# Define a function for consistent logging from the "local" execution context
echo_local() { echo "Local: $1"; }

# Trap for graceful exit on SIGINT/SIGTERM
current_coalescent_pid=""
cleanup_and_exit() {
    echo_local "Received signal, cleaning up and exiting..."
    if [ -n "$current_coalescent_pid" ] && kill -0 "$current_coalescent_pid" 2>/dev/null; then
        echo_local "Terminating active 'coalescent' process (PID: ${current_coalescent_pid})..."
        kill -TERM "$current_coalescent_pid"
        wait "$current_coalescent_pid" 2>/dev/null
    fi
    exit 130 # Standard exit code for script termination by signal
}
trap 'cleanup_and_exit' INT TERM

# --- SIMULATION FUNCTION WITH RETRY/TIMEOUT LOGIC ---
run_simulation() {
    local sim_id=$1
    local output_suffix_base="hap.${sim_id}"
    local output_tped_file="./${NEUTRAL_OUTPUT_DIR_NAME}/neut.${output_suffix_base}"

    echo_local "Starting simulation for ID ${sim_id}, base output suffix ${output_suffix_base}"

    local attempt=0
    local max_attempts=100
    local success=false

    while [ "$success" = false ] && [ $attempt -lt $max_attempts ]; do
        attempt=$((attempt + 1))
        echo_local "Attempt ${attempt}/${max_attempts} for sim ID ${sim_id}..."

        local start_time=$(date +%s)
        # Run cosi2 locally
        env COSI_NEWSIM=1 COSI_MAXATTEMPTS=1000000 coalescent \
            -p "${DEMOGRAPHIC_MODEL_FILE_PATH}" \
            -v \
            --drop-singletons .25 \
            --tped "${output_tped_file}" \
            -n 1 -M -r 0 &
        current_coalescent_pid=$!

        local timeout_seconds=35 # Max time for a single simulation run
        local elapsed_seconds=0
        local kill_signal_sent=false
        while kill -0 $current_coalescent_pid 2>/dev/null; do
            sleep 1
            elapsed_seconds=$((elapsed_seconds + 1))
            if [ $elapsed_seconds -ge $timeout_seconds ]; then
                echo_local "Timeout: Sim ID ${sim_id} (PID ${current_coalescent_pid}) exceeded ${timeout_seconds}s. Killing."
                kill -9 $current_coalescent_pid # Force kill
                wait $current_coalescent_pid 2>/dev/null # Wait for it to actually terminate
                kill_signal_sent=true
                break # Exit the inner while loop
            fi
        done

        if [ "$kill_signal_sent" = true ]; then
            echo_local "Sim ID ${sim_id} (attempt ${attempt}) timed out. Retrying if attempts left..."
            current_coalescent_pid=""
            if [ $attempt -lt $max_attempts ]; then sleep 2; fi # Short pause before retry
            continue # Go to next attempt
        fi

        # If not killed by timeout, check exit status
        if wait "$current_coalescent_pid"; then
            echo_local "Sim ID ${sim_id} (attempt ${attempt}) completed successfully."
            success=true
        else
            local exit_code=$?
            echo_local "Sim ID ${sim_id} (attempt ${attempt}) failed (exit code ${exit_code}). Retrying if attempts left..."
            current_coalescent_pid=""
            if [ $attempt -lt $max_attempts ]; then sleep 2; fi # Short pause before retry
        fi
    done

    if [ "$success" = true ]; then
        local end_time=$(date +%s)
        local runtime_seconds=$((end_time - start_time))
        echo "sim_id,${sim_id},neut_runtime,${runtime_seconds},seconds" >> "./${RUNTIME_DIR_NAME}/cosi.neut.runtime.csv"
        return 0
    else
        echo_local "Sim ID ${sim_id} failed after ${max_attempts} attempts."
        return 1
    fi
}

# --- Main Simulation Loop ---
echo "Host: Starting neutral simulations (Total: ${TOTAL_NEUTRAL_SIMULATIONS})..."
upper_limit_for_seq=$((${TOTAL_NEUTRAL_SIMULATIONS} - 1))
sim_failed_count=0
for i in $(seq 0 1 ${upper_limit_for_seq}); do
    if ! run_simulation "${i}"; then
        echo "Host: CRITICAL WARNING: Simulation ID ${i} failed after all retries."
        sim_failed_count=$((sim_failed_count + 1))
    fi
done

echo "Host: All neutral simulations attempted."
if [ "$sim_failed_count" -eq 0 ]; then
    echo "Host: All simulations completed successfully."
    echo "Host Script (01_cosi_neut.sh) Finished Successfully: $(date)"
    exit 0
else
    echo "Host: ERROR: ${sim_failed_count} simulation(s) failed."
    echo "Host Script (01_cosi_neut.sh) Finished with Errors: $(date)"
    exit 1
fi