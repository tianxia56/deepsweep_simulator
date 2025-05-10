#!/bin/bash

# This script creates bin files for various statistics from NEUTRAL simulations,
# which are used for normalization purposes later.
# It processes one-population stats (nSL, iHS, delihh, iHH12) and two-population XPEHH stats.

# --- Python Helper Script Names ---
PYTHON_ONEPOP_BINS_SCRIPT_NAME="create_onepop_norm_bins_core.py"
PYTHON_XPEHH_BINS_SCRIPT_NAME="create_xpehh_norm_bins_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/create_norm_bins.log" # Single log for this step

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

NEUTRAL_SIM_RUNTIME_CSV="cosi.neut.runtime.csv" 
ONEPOP_STATS_NEUT_DIR="one_pop_stats_neut" 
HAPBIN_DIR="hapbin"                      
BIN_OUTPUT_DIR="bin"                     
RUNTIME_DIR="runtime"
DOCKER_IMAGE_PYBINS="docker.io/tx56/deepsweep_simulator:latest"
HOST_CWD=$(pwd)

log_message "INFO" "Host Script (08_create_norm_bins.sh) Started: $(date)"
log_message "INFO" "Reading configuration from ${CONFIG_FILE}"
log_message "INFO" "Reference Pop (pop1): ${POP1_REF}, All Pop IDs: ${POP_IDS_ARRAY[*]}"
log_message "INFO" "Input one-pop stats dir: ${ONEPOP_STATS_NEUT_DIR}"
log_message "INFO" "Input XPEHH files from: ${HAPBIN_DIR} (for neutral)"
log_message "INFO" "Output bin files to: ${BIN_OUTPUT_DIR}"

if [ ! -f "${CONFIG_FILE}" ]; then log_message "ERROR" "Config file '${CONFIG_FILE}' NOT FOUND."; exit 1; fi
mkdir -p "${BIN_OUTPUT_DIR}"; mkdir -p "${RUNTIME_DIR}"
INPUT_CSV_FILE_HOST="${HOST_CWD}/${RUNTIME_DIR}/${NEUTRAL_SIM_RUNTIME_CSV}"
if [ ! -f "${INPUT_CSV_FILE_HOST}" ]; then log_message "ERROR" "Input CSV '${INPUT_CSV_FILE_HOST}' not found."; exit 1; fi
numeric_data_rows=$(awk -F, '$2 ~ /^[0-9]+$/ {count++} END {print count+0}' "${INPUT_CSV_FILE_HOST}")
if [ "${numeric_data_rows}" -eq 0 ]; then log_message "WARNING" "Input CSV '${INPUT_CSV_FILE_HOST}' has no numeric sim_ids in 2nd col."; fi

if ! docker image inspect "$DOCKER_IMAGE_PYBINS" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PYBINS} not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_PYBINS"; then log_message "ERROR" "Failed to pull Docker image ${DOCKER_IMAGE_PYBINS}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_PYBINS} already exists locally."
fi
log_message "INFO" "Starting Docker container for creating normalization bins..."

XPEHH_PAIR_IDS_LIST=()
for pop2_iter in "${POP_IDS_ARRAY[@]}"; do
    if [ "${pop2_iter}" -ne "${POP1_REF}" ]; then
        XPEHH_PAIR_IDS_LIST+=("${POP1_REF}_vs_${pop2_iter}")
    fi
done
XPEHH_PAIR_IDS_STR=$(IFS=,; echo "${XPEHH_PAIR_IDS_LIST[*]}")

docker run --rm -i --init \
    -u $(id -u):$(id -g) \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e CONTAINER_POP1_REF="${POP1_REF}" \
    -e CONTAINER_XPEHH_PAIR_IDS_STR="${XPEHH_PAIR_IDS_STR}" \
    -e CONTAINER_ONEPOP_STATS_NEUT_DIR="${ONEPOP_STATS_NEUT_DIR}" \
    -e CONTAINER_HAPBIN_DIR="${HAPBIN_DIR}" \
    -e CONTAINER_BIN_OUTPUT_DIR="${BIN_OUTPUT_DIR}" \
    -e CONTAINER_RUNTIME_DIR="${RUNTIME_DIR}" \
    -e CONTAINER_NEUTRAL_SIM_RUNTIME_CSV="${NEUTRAL_SIM_RUNTIME_CSV}" \
    -e PYTHON_ONEPOP_BINS_SCRIPT_NAME="${PYTHON_ONEPOP_BINS_SCRIPT_NAME}" \
    -e PYTHON_XPEHH_BINS_SCRIPT_NAME="${PYTHON_XPEHH_BINS_SCRIPT_NAME}" \
    "$DOCKER_IMAGE_PYBINS" /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 
cleanup_and_exit() {
    log_container "Caught signal! Cleaning up temporary Python scripts..."
    rm -f "./${PYTHON_ONEPOP_BINS_SCRIPT_NAME}" "./${PYTHON_XPEHH_BINS_SCRIPT_NAME}"
    log_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM
log_container "----------------------------------------------------"
log_container "Container Script (Create Norm Bins) Started: $(date)"
log_container "----------------------------------------------------"
log_container "Received POP1_REF: [${CONTAINER_POP1_REF}]"
log_container "Received XPEHH_PAIR_IDS_STR: [${CONTAINER_XPEHH_PAIR_IDS_STR}]"

# --- Create Python script for One-Population Stat Bins ---
cat > "./${PYTHON_ONEPOP_BINS_SCRIPT_NAME}" <<'PYTHON_ONEPOP_EOF'
import os
import pandas as pd
import numpy as np
import sys

def process_nsl(file_path):
    col_names_for_nsl = ['locusID', 'pos', 'daf', 'sl1', 'sl0', 'nsl_value']
    try:
        df = pd.read_csv(file_path, sep=r'\s+', header=None, names=col_names_for_nsl, comment='#')
        if not all(col in df.columns for col in ['pos', 'daf', 'nsl_value']):
             print(f"Python OnePop: ERROR - After assigning names, required columns not present for NSL file {file_path}. Cols: {df.columns.tolist()}")
             return pd.DataFrame(columns=['pos', 'daf', 'nsl'])
        df_processed = df[['pos', 'daf', 'nsl_value']].copy()
        df_processed.rename(columns={'nsl_value': 'nsl'}, inplace=True)
        df_processed['daf'] = pd.to_numeric(df_processed['daf'], errors='coerce')
        df_processed['nsl'] = pd.to_numeric(df_processed['nsl'], errors='coerce')
        return df_processed[['pos', 'daf', 'nsl']]
    except Exception as e:
        print(f"Python OnePop: ERROR processing NSL file {file_path}: {e}")
        return pd.DataFrame(columns=['pos', 'daf', 'nsl'])

def process_ihs(file_path): # Reads final .ihs.out from step 06 which has header
    try:
        df = pd.read_csv(file_path, sep=' ', header=0) 
        if not all(col in df.columns for col in ['pos', 'daf', 'iHS', 'delihh']):
            print(f"Python OnePop: ERROR - Required columns missing in IHS file {file_path}. Cols: {df.columns.tolist()}")
            return pd.DataFrame(columns=['pos', 'daf', 'iHS']), pd.DataFrame(columns=['pos', 'daf', 'delihh'])
        df['daf'] = pd.to_numeric(df['daf'], errors='coerce')
        df['iHS'] = pd.to_numeric(df['iHS'], errors='coerce')
        df['delihh'] = pd.to_numeric(df['delihh'], errors='coerce')
        return df[['pos', 'daf', 'iHS']], df[['pos', 'daf', 'delihh']]
    except Exception as e:
        print(f"Python OnePop: ERROR processing IHS file {file_path}: {e}")
        return pd.DataFrame(columns=['pos', 'daf', 'iHS']), pd.DataFrame(columns=['pos', 'daf', 'delihh'])

def process_ihh12(file_path):
    # selscan --ihh12 output. Assumed header: id pos p1 ihh12 (or similar)
    try:
        df = pd.read_csv(file_path, sep=r'\s+', header=0, comment='#') 

        pos_col_name, daf_col_name, ihh12_col_name = None, None, None
        if 'pos' in df.columns: pos_col_name = 'pos'
        elif 'physPos' in df.columns: pos_col_name = 'physPos'
        else: # Fallback if no known position column name
            if df.shape[1] >= 2: pos_col_name = df.columns[1]

        if 'p1' in df.columns: daf_col_name = 'p1' 
        elif 'freq_derived' in df.columns: daf_col_name = 'freq_derived'
        elif 'daf' in df.columns: daf_col_name = 'daf'
        else: # Fallback
            if df.shape[1] >= 3: daf_col_name = df.columns[2]
            
        if 'ihh12' in df.columns: ihh12_col_name = 'ihh12' 
        elif 'iHH12' in df.columns: ihh12_col_name = 'iHH12'
        else: # Fallback
             if df.shape[1] >= 4: ihh12_col_name = df.columns[3]

        if not all([pos_col_name, daf_col_name, ihh12_col_name]) or \
           pos_col_name not in df.columns or \
           daf_col_name not in df.columns or \
           ihh12_col_name not in df.columns:
            print(f"Python OnePop: ERROR - Could not identify all required columns (pos, daf, ihh12 equivalent) in iHH12 file {file_path}. Found columns: {df.columns.tolist()}")
            return pd.DataFrame(columns=['pos', 'daf', 'ihh12'])

        df_processed = df[[pos_col_name, daf_col_name, ihh12_col_name]].copy()
        df_processed.rename(columns={
            pos_col_name: 'pos', 
            daf_col_name: 'daf', 
            ihh12_col_name: 'ihh12'
        }, inplace=True)
        
        df_processed['daf'] = pd.to_numeric(df_processed['daf'], errors='coerce')
        df_processed['ihh12'] = pd.to_numeric(df_processed['ihh12'], errors='coerce')
        return df_processed[['pos', 'daf', 'ihh12']]
    except Exception as e:
        print(f"Python OnePop: ERROR processing iHH12 file {file_path}: {e}")
        return pd.DataFrame(columns=['pos', 'daf', 'ihh12'])

def bin_data_stats(df_full, value_col_name, num_bins=20):
    if df_full.empty or value_col_name not in df_full.columns or 'daf' not in df_full.columns:
        print(f"Python OnePop: Dataframe empty or required columns ('daf', '{value_col_name}') missing for binning.")
        return pd.DataFrame(columns=['bin_daf_max', 'mean_stat', 'std_stat'])
    df = df_full[['daf', value_col_name]].copy()
    df.dropna(subset=['daf', value_col_name], inplace=True) 
    if df.empty:
        print(f"Python OnePop: Dataframe empty after dropping NaNs for {value_col_name} binning.")
        return pd.DataFrame(columns=['bin_daf_max', 'mean_stat', 'std_stat'])
    
    df = df.sort_values(by='daf').reset_index(drop=True)
    
    try:
        df['bin'] = pd.qcut(df['daf'], q=num_bins, labels=False, duplicates='drop')
        binned_stats = df.groupby('bin', observed=True).agg(
            bin_daf_max=('daf', 'max'),      
            mean_stat=(value_col_name, 'mean'),
            std_stat=(value_col_name, 'std')
        ).reset_index(drop=True) 
    except ValueError as e: 
        print(f"Python OnePop: qcut failed for {value_col_name} (e.g. too few unique DAF values): {e}. Falling back to linspace.")
        min_daf, max_daf = df['daf'].min(), df['daf'].max()
        if min_daf == max_daf: 
            print(f"Python OnePop: All DAF values are identical ({min_daf}) for {value_col_name}. Creating a single bin.")
            mean_val = df[value_col_name].mean()
            std_val = df[value_col_name].std()
            binned_stats = pd.DataFrame({
                'bin_daf_max': [max_daf],
                'mean_stat': [mean_val],
                'std_stat': [std_val if not pd.isna(std_val) else 0]
            })
        else:
            bin_edges = np.linspace(min_daf, max_daf + 1e-9, num_bins + 1)
            df['bin_cat'] = pd.cut(df['daf'], bins=bin_edges, include_lowest=True, right=True)
            binned_stats = df.groupby('bin_cat', observed=False).agg(
                daf_for_max_edge_calc=('daf', 'max'),
                mean_stat=(value_col_name, 'mean'),
                std_stat=(value_col_name, 'std')
            ).reset_index()
            binned_stats['bin_daf_max'] = binned_stats['bin_cat'].apply(lambda x: x.right if pd.api.types.is_interval(x) else np.nan)
            binned_stats = binned_stats[['bin_daf_max', 'mean_stat', 'std_stat']]
            
    binned_stats['std_stat'].fillna(0, inplace=True)
    return binned_stats.sort_values(by='bin_daf_max').reset_index(drop=True)

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python <script_name>.py <sim_ids_comma_sep> <pop1_ref> <onepop_stats_neut_dir> <bin_output_dir>")
        sys.exit(1)
    sim_ids_str, pop1_ref, onepop_stats_neut_dir, bin_output_dir = sys.argv[1:5]
    sim_ids = [s.strip() for s in sim_ids_str.split(',')]
    print(f"Python OnePop: Processing sim_ids: {sim_ids} for pop1_ref: {pop1_ref}")
    all_nsl_data, all_ihs_data, all_delihh_data, all_ihh12_data = [], [], [], []
    for sim_id in sim_ids:
        print(f"Python OnePop: --- Reading data for sim_id {sim_id} ---")
        nsl_file = os.path.join(onepop_stats_neut_dir, f'neut.{sim_id}_pop{pop1_ref}.nsl.out')
        ihs_file_final = os.path.join(onepop_stats_neut_dir, f'neut.{sim_id}_0_{pop1_ref}.ihs.out')
        ihh12_file = os.path.join(onepop_stats_neut_dir, f'neut.{sim_id}_pop{pop1_ref}.ihh12.out')
        if os.path.exists(nsl_file): print(f"Python OnePop: Processing nSL file: {nsl_file}"); all_nsl_data.append(process_nsl(nsl_file))
        else: print(f"Python OnePop: WARNING - NSL file not found: {nsl_file}")
        if os.path.exists(ihs_file_final): print(f"Python OnePop: Processing IHS file: {ihs_file_final}"); ihs_df, delihh_df = process_ihs(ihs_file_final); all_ihs_data.append(ihs_df); all_delihh_data.append(delihh_df)
        else: print(f"Python OnePop: WARNING - IHS file not found: {ihs_file_final}")
        if os.path.exists(ihh12_file): print(f"Python OnePop: Processing iHH12 file: {ihh12_file}"); all_ihh12_data.append(process_ihh12(ihh12_file))
        else: print(f"Python OnePop: WARNING - iHH12 file not found: {ihh12_file}")
    
    nsl_data_full = pd.concat(all_nsl_data, ignore_index=True) if all_nsl_data else pd.DataFrame(columns=['pos', 'daf', 'nsl'])
    ihs_data_full = pd.concat(all_ihs_data, ignore_index=True) if all_ihs_data else pd.DataFrame(columns=['pos', 'daf', 'iHS'])
    delihh_data_full = pd.concat(all_delihh_data, ignore_index=True) if all_delihh_data else pd.DataFrame(columns=['pos', 'daf', 'delihh'])
    ihh12_data_full = pd.concat(all_ihh12_data, ignore_index=True) if all_ihh12_data else pd.DataFrame(columns=['pos', 'daf', 'ihh12'])
    
    os.makedirs(bin_output_dir, exist_ok=True)
    if not nsl_data_full.empty: nsl_binned = bin_data_stats(nsl_data_full, 'nsl'); nsl_binned.to_csv(os.path.join(bin_output_dir, 'nsl_bin.csv'), index=False); print(f"Python OnePop: nSL binned data saved.")
    else: print("Python OnePop: No nSL data to bin.")
    if not ihs_data_full.empty: ihs_binned = bin_data_stats(ihs_data_full, 'iHS'); ihs_binned.to_csv(os.path.join(bin_output_dir, 'ihs_bin.csv'), index=False); print(f"Python OnePop: iHS binned data saved.")
    else: print("Python OnePop: No iHS data to bin.")
    if not delihh_data_full.empty: delihh_binned = bin_data_stats(delihh_data_full, 'delihh'); delihh_binned.to_csv(os.path.join(bin_output_dir, 'delihh_bin.csv'), index=False); print(f"Python OnePop: delihh binned data saved.")
    else: print("Python OnePop: No delihh data to bin.")
    if not ihh12_data_full.empty: ihh12_binned = bin_data_stats(ihh12_data_full, 'ihh12'); ihh12_binned.to_csv(os.path.join(bin_output_dir, 'ihh12_bin.csv'), index=False); print(f"Python OnePop: iHH12 binned data saved.")
    else: print("Python OnePop: No iHH12 data to bin.")
    print(f"Python OnePop: Binned data generation complete. Output in '{bin_output_dir}' directory.")
PYTHON_ONEPOP_EOF
chmod +x "./${PYTHON_ONEPOP_BINS_SCRIPT_NAME}"
log_container "Python One-Population Binning script created."

# --- Create Python script for XPEHH Stat Bins ---
# (PYTHON_XPEHH_BINS_SCRIPT_NAME heredoc remains the same as the last correct version)
cat > "./${PYTHON_XPEHH_BINS_SCRIPT_NAME}" <<'PYTHON_XPEHH_EOF'
import os
import numpy as np
import pandas as pd
import sys
def extract_columns_from_xpehh(file_path):
    try:
        df = pd.read_csv(file_path, sep=' ', header=0) 
        if not all(col in df.columns for col in ['daf', 'xpehh']):
            print(f"Python XPEHH: ERROR - Required columns 'daf', 'xpehh' missing in {file_path}. Cols: {df.columns.tolist()}")
            return pd.DataFrame(columns=['daf', 'xpehh']) 
        return df[['daf', 'xpehh']]
    except Exception as e:
        print(f"Python XPEHH: ERROR processing XPEHH file {file_path}: {e}")
        return pd.DataFrame(columns=['daf', 'xpehh'])
def create_bins_and_stats_xpehh(scores_series, dafs_series, num_bins=20):
    dafs = pd.Series(dafs_series); scores = pd.Series(scores_series)
    dafs = pd.to_numeric(dafs, errors='coerce'); scores = pd.to_numeric(scores, errors='coerce')
    valid_indices = ~dafs.isna() & ~scores.isna()
    dafs = dafs[valid_indices].reset_index(drop=True); scores = scores[valid_indices].reset_index(drop=True)
    if dafs.empty or scores.empty or len(dafs) < num_bins : 
        print(f"Python XPEHH: Not enough valid DAF/score pairs (found {len(dafs)}, need at least {num_bins}) for {num_bins}-binning after NaN removal.")
        return pd.DataFrame(columns=['bin_daf_max', 'mean_stat', 'std_stat'])
    temp_df_for_binning = pd.DataFrame({'daf': dafs, 'score': scores})
    temp_df_for_binning.sort_values(by='daf', inplace=True); bin_labels_for_stats = None
    try:
        temp_df_for_binning['bin_qcut'] = pd.qcut(temp_df_for_binning['daf'], q=num_bins, labels=False, duplicates='drop')
        bin_labels_for_stats = 'bin_qcut'
        print(f"Python XPEHH: Successfully used qcut for binning. Number of bins: {temp_df_for_binning['bin_qcut'].nunique()}")
    except ValueError:
        print(f"Python XPEHH: qcut failed. Falling back to linspace.")
        min_daf, max_daf = temp_df_for_binning['daf'].min(), temp_df_for_binning['daf'].max()
        if min_daf == max_daf:
            print(f"Python XPEHH: All DAF values are identical ({min_daf}) after qcut fallback. Creating a single bin.")
            mean_val = temp_df_for_binning['score'].mean(); std_val = temp_df_for_binning['score'].std()
            return pd.DataFrame({'bin_daf_max': [max_daf], 'mean_stat': [mean_val], 'std_stat': [std_val if not pd.isna(std_val) else 0]})
        bin_edges = np.linspace(min_daf, max_daf + 1e-9, num_bins + 1) 
        temp_df_for_binning['bin_linspace'] = pd.cut(temp_df_for_binning['daf'], bins=bin_edges, include_lowest=True, right=True)
        bin_labels_for_stats = 'bin_linspace'; print(f"Python XPEHH: Used linspace for binning.")
    binned_stats = temp_df_for_binning.groupby(bin_labels_for_stats, observed=False).agg(
        daf_for_max_edge_calc=('daf', 'max'), mean_stat=('score', 'mean'), std_stat=('score', 'std')).reset_index()
    if bin_labels_for_stats == 'bin_qcut': binned_stats.rename(columns={'daf_for_max_edge_calc': 'bin_daf_max', bin_labels_for_stats: 'bin_group_id'}, inplace=True)
    elif bin_labels_for_stats == 'bin_linspace':
        binned_stats['bin_daf_max'] = binned_stats[bin_labels_for_stats].apply(lambda x: x.right if pd.api.types.is_interval(x) else np.nan)
        binned_stats.rename(columns={bin_labels_for_stats: 'bin_interval'}, inplace=True)
    binned_stats['std_stat'].fillna(0, inplace=True)
    binned_stats = binned_stats[['bin_daf_max', 'mean_stat', 'std_stat']].sort_values(by='bin_daf_max').reset_index(drop=True)
    print(f"Python XPEHH: Final number of bins generated: {binned_stats.shape[0]}")
    return binned_stats
if __name__ == "__main__":
    if len(sys.argv) < 4: print("Usage: python <script_name>.py <sim_ids_comma_sep> <pair_ids_comma_sep> <hapbin_dir> <bin_output_dir>"); sys.exit(1)
    sim_ids_str, pair_ids_str, hapbin_dir, bin_output_dir = sys.argv[1:5]
    sim_ids = [s.strip() for s in sim_ids_str.split(',')]; pair_ids = [p.strip() for p in pair_ids_str.split(',')]
    print(f"Python XPEHH: Processing sim_ids: {sim_ids} for XPEHH pairs: {pair_ids}"); os.makedirs(bin_output_dir, exist_ok=True)
    for pair_id in pair_ids:
        print(f"Python XPEHH: --- Processing XPEHH pair: {pair_id} ---"); combined_xpehh_data_for_pair = []
        for sim_id in sim_ids:
            xpehh_file_path = os.path.join(hapbin_dir, f'neut.{sim_id}_{pair_id}.xpehh.out')
            if not os.path.exists(xpehh_file_path): print(f"Python XPEHH: WARNING - File not found: {xpehh_file_path}"); continue
            print(f"Python XPEHH: Reading {xpehh_file_path}"); xpehh_df_one_sim = extract_columns_from_xpehh(xpehh_file_path)
            if not xpehh_df_one_sim.empty: combined_xpehh_data_for_pair.append(xpehh_df_one_sim)
            else: print(f"Python XPEHH: WARNING - No data extracted from {xpehh_file_path}")
        if not combined_xpehh_data_for_pair: print(f"Python XPEHH: No XPEHH data for pair {pair_id}. Skipping."); continue
        combined_xpehh_df_all_sims = pd.concat(combined_xpehh_data_for_pair, ignore_index=True)
        if combined_xpehh_df_all_sims.empty or 'daf' not in combined_xpehh_df_all_sims or 'xpehh' not in combined_xpehh_df_all_sims:
            print(f"Python XPEHH: ERROR - DataFrame for pair {pair_id} is empty or missing 'daf'/'xpehh'."); continue
        binned_xpehh_df = create_bins_and_stats_xpehh(combined_xpehh_df_all_sims['xpehh'], combined_xpehh_df_all_sims['daf'])
        if not binned_xpehh_df.empty:
            output_bin_file = os.path.join(bin_output_dir, f'xpehh_{pair_id}_bin.csv')
            binned_xpehh_df.to_csv(output_bin_file, index=False); print(f"Python XPEHH: XPEHH binned data for pair {pair_id} saved to {output_bin_file}")
        else: print(f"Python XPEHH: No binned data generated for XPEHH pair {pair_id}.")
    print("Python XPEHH: XPEHH bin file generation complete.")
PYTHON_XPEHH_EOF
chmod +x "./${PYTHON_XPEHH_BINS_SCRIPT_NAME}"
log_container "Python XPEHH Binning script created."

# --- Main Execution in Container ---
INPUT_CSV_FILE_IN_CONTAINER="./${CONTAINER_RUNTIME_DIR}/${CONTAINER_NEUTRAL_SIM_RUNTIME_CSV}"
if [ ! -f "${INPUT_CSV_FILE_IN_CONTAINER}" ]; then
    log_container "CRITICAL ERROR: Input CSV '${INPUT_CSV_FILE_IN_CONTAINER}' not found. Exiting."
    exit 1
fi
log_container "Reading all neutral sim_ids from ${INPUT_CSV_FILE_IN_CONTAINER} for bin creation..."
mapfile -t neutral_sim_ids_array < <(awk -F, '$2 ~ /^[0-9]+$/ {print $2}' "${INPUT_CSV_FILE_IN_CONTAINER}" | sort -un)
NEUTRAL_SIM_IDS_COMMA_SEP=$(IFS=,; echo "${neutral_sim_ids_array[*]}")
if [ -z "${NEUTRAL_SIM_IDS_COMMA_SEP}" ]; then
    log_container "No neutral sim_ids found to process. Exiting bin creation."
else
    log_container "Found neutral sim_ids for binning: ${NEUTRAL_SIM_IDS_COMMA_SEP}"
    log_container "--- Starting One-Population Neutral Stats Binning ---"
    python3 "./${PYTHON_ONEPOP_BINS_SCRIPT_NAME}" \
        "${NEUTRAL_SIM_IDS_COMMA_SEP}" \
        "${CONTAINER_POP1_REF}" \
        "./${CONTAINER_ONEPOP_STATS_NEUT_DIR}" \
        "./${CONTAINER_BIN_OUTPUT_DIR}"
    if [ $? -eq 0 ]; then log_container "One-population neutral stats binning completed successfully."; else log_container "ERROR: Python script for one-population neutral stats binning FAILED."; fi
    log_container "--- Finished One-Population Neutral Stats Binning ---"
    if [ -z "${CONTAINER_XPEHH_PAIR_IDS_STR}" ]; then
        log_container "No XPEHH pairs defined. Skipping XPEHH binning."
    else
        log_container "--- Starting XPEHH Neutral Stats Binning for pairs: ${CONTAINER_XPEHH_PAIR_IDS_STR} ---"
        python3 "./${PYTHON_XPEHH_BINS_SCRIPT_NAME}" \
            "${NEUTRAL_SIM_IDS_COMMA_SEP}" \
            "${CONTAINER_XPEHH_PAIR_IDS_STR}" \
            "./${CONTAINER_HAPBIN_DIR}" \
            "./${CONTAINER_BIN_OUTPUT_DIR}"
        if [ $? -eq 0 ]; then log_container "XPEHH neutral stats binning completed successfully."; else log_container "ERROR: Python script for XPEHH neutral stats binning FAILED."; fi
        log_container "--- Finished XPEHH Neutral Stats Binning ---"
    fi
fi
rm -f "./${PYTHON_ONEPOP_BINS_SCRIPT_NAME}" "./${PYTHON_XPEHH_BINS_SCRIPT_NAME}"
log_container "Python helper scripts removed."
log_container "Normalization bin creation process finished."
log_container "----------------------------------------------------"
log_container "Container Script (Create Norm Bins) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$?
log_message "INFO" "Docker container (Create Norm Bins) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then log_message "INFO" "Script (Create Norm Bins) likely interrupted."; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then log_message "ERROR" "Docker container (Create Norm Bins) reported an error."; fi
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (08_create_norm_bins.sh) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"