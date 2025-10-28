#!/bin/bash

# --- Bulletproof Bash and Conda Activation ---
# 1. Ensure the script is running with Bash, not sh/dash
if [ -z "$BASH_VERSION" ]; then
    # Re-execute the script with the same arguments using bash.
    echo "Re-executing with /bin/bash..." >&2
    exec /bin/bash "$0" "$@"
fi

# 2. Manually find and activate the Conda environment by manipulating the PATH.
ENV_NAME="deepsweep_simulator"
echo "Attempting to set PATH for Conda environment: ${ENV_NAME}" >&2
CONDA_ENV_PATH=$(conda info --envs | grep -E "^${ENV_NAME}\s" | awk '{print $NF}')
if [ -z "${CONDA_ENV_PATH}" ]; then
    echo "ERROR: Could not find Conda environment path for '${ENV_NAME}'." >&2
    exit 1
fi
if [ ! -d "${CONDA_ENV_PATH}/bin" ]; then
    echo "ERROR: bin directory not found in '${CONDA_ENV_PATH}'." >&2
    exit 1
fi
export PATH="${CONDA_ENV_PATH}/bin:${PATH}"
echo "Successfully set PATH for Conda environment." >&2
# --- End of Activation Block ---

# This script collates all statistics, processes sweep parameters,
# finalizes output files by zipping them, and cleans up intermediate files.
# This version runs all commands directly on the host system.

set -e
set -o pipefail

# --- Python Helper Script Name ---
PYTHON_FINALIZE_SCRIPT_NAME="collate_finalize_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/collate_finalize.log"

mkdir -p "${LOG_DIR}"
exec &> >(tee -a "${LOG_FILE}")

# --- Helper Functions ---
log_message() {
    local type="$1"
    local message="$2"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] [${type}] - ${message}"
}

# --- Initial Setup ---
log_message "INFO" "Host Script (11_collate_and_finalize.sh) Started: $(date)"

# --- Local Dependency Checks ---
log_message "INFO" "Checking for local dependencies..."
log_message "DEBUG" "Python interpreter being used is: $(which python3)"
if ! command -v python3 &> /dev/null; then
    log_message "ERROR" "'python3' command not found. Python 3 is required."
    exit 1
fi
if ! python3 -c "import pandas" &> /dev/null; then
    log_message "ERROR" "Python 'pandas' library not found. The Conda env may not be activated correctly."
    exit 1
fi
if ! python3 -c "import numpy" &> /dev/null; then
    log_message "ERROR" "Python 'numpy' library not found. The Conda env may not be activated correctly."
    exit 1
fi
log_message "INFO" "All local dependencies found."

# --- Host Configuration & Pre-checks ---
CONFIG_FILE="00config.json" 
HOST_CWD=$(pwd)

log_message "INFO" "Configuration file to be used by Python script: ${CONFIG_FILE}"
for req_dir in runtime selected_sims one_pop_stats_sel two_pop_stats norm bin; do
    if [ ! -d "${HOST_CWD}/${req_dir}" ]; then
        log_message "WARNING" "Required input directory '${req_dir}' not found. Previous steps might not have completed fully."
    fi
done
if [ ! -f "${HOST_CWD}/runtime/nsl.sel.runtime.csv" ]; then
    log_message "ERROR" "Critical input file 'runtime/nsl.sel.runtime.csv' not found. Cannot determine sim_ids. Aborting."
    exit 1
fi
if [ ! -f "${CONFIG_FILE}" ]; then 
    log_message "ERROR" "Configuration file '${CONFIG_FILE}' NOT FOUND on host. Aborting."
    exit 1
fi

# --- Main Logic ---

# Cleanup function to remove the temporary python script
cleanup() {
    log_message "INFO" "Cleaning up temporary Python script..."
    rm -f "${PYTHON_FINALIZE_SCRIPT_NAME}"
}
trap cleanup INT TERM EXIT

# Create the Python helper script in the current directory
log_message "INFO" "Creating Python helper script: ${PYTHON_FINALIZE_SCRIPT_NAME}"
cat > "${PYTHON_FINALIZE_SCRIPT_NAME}" <<'PYTHON_SCRIPT_EOF'
import json
import os
import pandas as pd
from datetime import datetime
import zipfile
import sys 
import numpy as np 
import glob 

config_file_name = "00config.json" 

def load_config(cfg_file):
    try:
        with open(cfg_file, 'r') as f: config = json.load(f)
        print(f"Python: Successfully loaded config file '{cfg_file}'.")
        return config
    except FileNotFoundError: print(f"Python: Error - Configuration file '{cfg_file}' not found."); sys.exit(1)
    except Exception as e: print(f"Python: Error loading config '{cfg_file}': {e}"); sys.exit(1)

def collate_stats_for_sim(sim_id, demographic_model_name, sim_serial_num, onepop_stats_sel_dir_loc, two_pop_stats_dir_loc, norm_dir_loc, output_dir_loc):
    output_file = os.path.join(output_dir_loc, f"{demographic_model_name}_batch{sim_serial_num}_cms_stats_{sim_id}.tsv")
    print(f"Python Collate: Initial collation for simulation ID: {sim_id}")
    try:
        pop1_ref = str(config_data_global.get('selected_pop'))
        
        fst_deldaf_file = os.path.join(two_pop_stats_dir_loc, f"sel.{sim_id}_0_{pop1_ref}_fst_deldaf.tsv")
        
        output_data = None 
        base_data_loaded = False
        if os.path.exists(fst_deldaf_file):
            try:
                output_data = pd.read_csv(fst_deldaf_file, sep='\t')
                if 'daf_pop1' in output_data.columns:
                    output_data.rename(columns={'daf_pop1': 'daf'}, inplace=True)
                if 'maf_pop1' in output_data.columns:
                    output_data.rename(columns={'maf_pop1': 'maf'}, inplace=True)
                if 'pos' in output_data.columns:
                    output_data['pos'] = pd.to_numeric(output_data['pos'], errors='coerce')
                    base_data_loaded = True
            except Exception as e: print(f"  Warning: Error reading FST/DelDAF file {fst_deldaf_file}: {e}")

        if not base_data_loaded:
             isafe_check_file = os.path.join(onepop_stats_sel_dir_loc, f"{sim_id}.iSAFE.out") 
             if os.path.exists(isafe_check_file):
                 try:
                     temp_isafe = pd.read_csv(isafe_check_file, sep='\t')
                     if 'POS' in temp_isafe.columns:
                         output_data = temp_isafe[['POS']].rename(columns={'POS': 'pos'}).copy()
                         output_data['pos'] = pd.to_numeric(output_data['pos'], errors='coerce')
                         base_data_loaded = True
                 except Exception as e: print(f"  Error reading fallback iSAFE file {isafe_check_file}: {e}"); return None
             else: print(f"  Error: Base FST/DelDAF and fallback iSAFE not found. Skipping sim {sim_id}."); return None
        
        if output_data is None: print(f"  Error: Base data is missing. Skipping sim {sim_id}."); return None
        output_data.dropna(subset=['pos'], inplace=True)

        isafe_file = os.path.join(onepop_stats_sel_dir_loc, f"{sim_id}.iSAFE.out") 
        if os.path.exists(isafe_file):
            try:
                isafe_data = pd.read_csv(isafe_file, sep='\t')
                if 'POS' in isafe_data.columns and 'iSAFE' in isafe_data.columns:
                    isafe_data.rename(columns={'POS': 'pos'}, inplace=True)
                    isafe_data['pos'] = pd.to_numeric(isafe_data['pos'], errors='coerce')
                    isafe_data['iSAFE'] = pd.to_numeric(isafe_data['iSAFE'], errors='coerce').round(4)
                    output_data = pd.merge(output_data, isafe_data[['pos', 'iSAFE']], on='pos', how='left')
            except Exception as e: print(f"  Warning: Error processing iSAFE file {isafe_file}: {e}")
        
        stats_to_merge = {
            "norm_ihs": os.path.join(norm_dir_loc, f"temp.ihs.{sim_id}.tsv"),
            "norm_nsl": os.path.join(norm_dir_loc, f"temp.nsl.{sim_id}.tsv"),
            "norm_ihh12": os.path.join(norm_dir_loc, f"temp.ihh12.{sim_id}.tsv"),
            "norm_delihh": os.path.join(norm_dir_loc, f"temp.delihh.{sim_id}.tsv"),
            "max_xpehh": os.path.join(norm_dir_loc, f"temp.max.xpehh.{sim_id}.tsv"), 
        }
        for col_name, file_path in stats_to_merge.items():
            if os.path.exists(file_path):
                try:
                    norm_data = pd.read_csv(file_path, sep='\t')
                    norm_data['pos'] = pd.to_numeric(norm_data['pos'], errors='coerce')
                    if 'pos' in norm_data.columns and col_name in norm_data.columns:
                         norm_data[col_name] = pd.to_numeric(norm_data[col_name], errors='coerce')
                         output_data = pd.merge(output_data, norm_data[['pos', col_name]], on='pos', how='left')
                except Exception as e: print(f"  Warning: Error reading or merging {file_path}: {e}")
            else:
                output_data[col_name] = pd.NA
        
        output_data['sim_batch_no'] = int(sim_serial_num) 
        output_data['sim_id'] = int(sim_id)
        
        os.makedirs(output_dir_loc, exist_ok=True)
        output_data.to_csv(output_file, sep='\t', index=False, na_rep='NA')
        print(f"  Initial collated stats saved to: {output_file}")
        return output_file 
    except Exception as e: print(f"  Error processing stats for sim_id {sim_id}: {e}"); import traceback; traceback.print_exc(); return None

def parse_sweep_parameters_from_cosi(runtime_dir_loc):
    print("\nPython: Parsing cosi.sel.*.sampled_loci.csv files...")
    all_sweep_params = []
    cosi_sel_pattern = os.path.join(runtime_dir_loc, "cosi.sel.*.sampled_loci.csv")
    files_to_parse = glob.glob(cosi_sel_pattern)
    if not files_to_parse:
        print(f"  No cosi.sel.*.sampled_loci.csv files found in {runtime_dir_loc}.")
        return pd.DataFrame(columns=['sim_id', 'deri_gen', 'sel_gen', 's'])
    for cosi_file_path in files_to_parse:
        try:
            with open(cosi_file_path, 'r') as f_cosi:
                lines = [line.strip() for line in f_cosi if line.strip()]
        except Exception: continue
        line_idx = 0
        while line_idx < (len(lines) - 1):
            current_line = lines[line_idx]; parts = current_line.split()
            event_type = ""
            if parts and (parts[0] == "sweep_mult" or parts[0] == "sweep_mult_standing"):
                event_type = parts[0]
            if event_type:
                next_line_str = lines[line_idx + 1]
                sim_id_str_from_file = next_line_str.strip().split()[0] if next_line_str.strip() else ""
                if sim_id_str_from_file.isdigit():
                    sim_id = int(sim_id_str_from_file)
                    try:
                        deri_gen, sel_gen, s_val = pd.NA, pd.NA, np.nan
                        if event_type == "sweep_mult" and len(parts) >= 5:
                            common_gen_val = int(round(float(parts[3]))); deri_gen = common_gen_val; sel_gen = common_gen_val; s_val = float(parts[4]) 
                        elif event_type == "sweep_mult_standing" and len(parts) >= 9:
                            deri_gen = int(round(float(parts[3]))); s_val = float(parts[4]); sel_gen = int(round(float(parts[8])))
                        if pd.notna(s_val):
                            all_sweep_params.append({'sim_id': sim_id, 'deri_gen': deri_gen, 'sel_gen': sel_gen, 's': s_val})
                        line_idx += 2; continue
                    except Exception: line_idx += 2; continue
                else: line_idx += 1; continue
            else: line_idx += 1
    if not all_sweep_params:
        return pd.DataFrame(columns=['sim_id', 'deri_gen', 'sel_gen', 's'])
    sweep_df = pd.DataFrame(all_sweep_params).drop_duplicates(subset=['sim_id'], keep='last')
    sweep_df['sim_id'] = sweep_df['sim_id'].astype(int)
    for col in ['deri_gen', 'sel_gen']: sweep_df[col] = pd.to_numeric(sweep_df[col], errors='coerce').astype('Int64')
    sweep_df['s'] = pd.to_numeric(sweep_df['s'], errors='coerce')
    print(f"  Parsed sweep parameters for {len(sweep_df)} unique sim_ids.")
    return sweep_df

def finalize_collated_files(successfully_collated_files, sweep_params_df, pos_sel_target, par_inputs_file_path):
    print("\nPython: Finalizing collated files with sweep parameters...")
    if os.path.exists(par_inputs_file_path):
        try: os.remove(par_inputs_file_path)
        except OSError: pass
    
    par_inputs_header_written = False

    for file_path in successfully_collated_files: 
        try:
            sim_id = int(os.path.basename(file_path).split('_cms_stats_')[-1].split('.tsv')[0])
            output_data = pd.read_csv(file_path, sep='\t')
            output_data['sim_id'] = output_data['sim_id'].astype(int)
            
            output_data['deri_gen'] = pd.NA; output_data['sel_gen'] = pd.NA; output_data['s'] = np.nan; output_data['selpos'] = 0

            output_data['pos'] = pd.to_numeric(output_data['pos'], errors='coerce')
            pos_sel_target_numeric = pd.to_numeric(pos_sel_target, errors='coerce')
            
            if not pd.isna(pos_sel_target_numeric):
                target_row_mask = (output_data['pos'] == pos_sel_target_numeric)
                if target_row_mask.any():
                    output_data.loc[target_row_mask, 'selpos'] = 1
                    params_row = sweep_params_df[sweep_params_df['sim_id'] == sim_id]
                    if not params_row.empty:
                        params = params_row.iloc[0]
                        output_data.loc[target_row_mask, 'deri_gen'] = params.get('deri_gen', pd.NA)
                        output_data.loc[target_row_mask, 'sel_gen'] = params.get('sel_gen', pd.NA)
                        output_data.loc[target_row_mask, 's'] = params.get('s', np.nan)
            
            for col in ['deri_gen', 'sel_gen']: output_data[col] = output_data[col].astype('Int64')
            output_data['selpos'] = output_data['selpos'].astype(int)
            
            # --- FINAL, DOUBLE-CHECKED, CORRECTED COLUMN ORDERING LOGIC ---
            desired_order = [
                'sim_batch_no', 'sim_id', 'pos',
                'mean_fst', 'deldaf', 'daf', 'maf',
                'iSAFE', 'norm_ihs', 'norm_nsl', 'norm_ihh12', 'norm_delihh', 'max_xpehh',
                'deri_gen', 'sel_gen', 's', 'selpos'
            ]
            
            final_col_order = [col for col in desired_order if col in output_data.columns]
            
            # BEFORE extracting the par_inputs slice, reorder the main DataFrame
            output_data_cleaned = output_data[final_col_order].copy()

            # NOW extract the par_inputs slice from the CLEANED DataFrame
            if not pd.isna(pos_sel_target_numeric):
                target_row_mask_cleaned = (output_data_cleaned['pos'] == pos_sel_target_numeric)
                if target_row_mask_cleaned.any():
                    pos_sel_extract = output_data_cleaned[target_row_mask_cleaned]
                    write_header = not par_inputs_header_written and (not os.path.exists(par_inputs_file_path) or os.path.getsize(par_inputs_file_path) == 0)
                    pos_sel_extract.to_csv(par_inputs_file_path, mode='a', header=write_header, index=False, sep='\t', na_rep='NA')
                    if write_header: par_inputs_header_written = True
            
            # Save the cleaned DataFrame to the final output file
            output_data_cleaned.to_csv(file_path, sep='\t', index=False, na_rep='NA')
            # --- END OF CORRECTION ---

        except Exception as e: print(f"  Error finalizing {file_path}: {e}")

def zip_and_cleanup(files_to_cleanup, zip_filename_base, output_dir_loc):
    current_date = datetime.now().strftime("%Y-%m-%d")
    zipfile_name = os.path.join(output_dir_loc, f"{zip_filename_base}_{current_date}.zip")
    print(f"\nPython: Creating zip file: {zipfile_name}")
    if not files_to_cleanup: print("  Warning: No files found to zip."); return
    try:
        with zipfile.ZipFile(zipfile_name, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            for file_path in files_to_cleanup:
                if os.path.exists(file_path):
                    zipf.write(file_path, os.path.basename(file_path))
        print(f"  Successfully created zip file with {len(files_to_cleanup)} files.")
        
        print("\nPython: Cleaning up individual TSV files...")
        for file_path in files_to_cleanup:
            if os.path.exists(file_path):
                try: os.remove(file_path)
                except Exception as e: print(f"  Error removing file {file_path}: {e}")
        print(f"  Removed {len(files_to_cleanup)} files.")
    except Exception as e: print(f"  Error during zip/cleanup: {e}")

config_data_global = {} 

if __name__ == "__main__":
    script_start_time = datetime.now()
    print(f"Python Finalize: Script starting at {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    config_data_global = load_config(config_file_name)
    demographic_model = config_data_global.get('demographic_model')
    sim_serial = config_data_global.get('simulation_serial_number')
    pos_sel = config_data_global.get('pos_sel_position')
    
    runtime_dir = "runtime"; output_dir = "output"; onepop_dir = "one_pop_stats_sel"; twopop_dir = "two_pop_stats"; norm_dir = "norm"
    sim_id_csv = os.path.join(runtime_dir, "nsl.sel.runtime.csv")
    
    try:
        sim_ids_df = pd.read_csv(sim_id_csv, header=None, usecols=[1])
        sim_ids = sim_ids_df.iloc[:, 0].dropna().astype(int).unique().tolist()
        if not sim_ids: print(f"Error: No sim IDs in '{sim_id_csv}'."); sys.exit(1)
        print(f"Found {len(sim_ids)} unique sim IDs.")
    except Exception as e: print(f"Error reading sim IDs: {e}"); sys.exit(1)
    
    os.makedirs(output_dir, exist_ok=True)
    
    print("\n--- Step 1: Initial Collation ---")
    collated_files = [collate_stats_for_sim(sid, demographic_model, sim_serial, onepop_dir, twopop_dir, norm_dir, output_dir) for sid in sim_ids]
    collated_files = [f for f in collated_files if f]
    if not collated_files: print("CRITICAL: No files collated. Exiting."); sys.exit(1)
    
    print("\n--- Step 2: Parsing Sweep Parameters ---")
    sweep_params = parse_sweep_parameters_from_cosi(runtime_dir)
    
    print("\n--- Step 3: Finalizing Files & Extracting Metadata ---")
    par_inputs_file = os.path.join(output_dir, f"{demographic_model}_batch{sim_serial}_par_inputs_{datetime.now().strftime('%Y-%m-%d')}.tsv")
    finalize_collated_files(collated_files, sweep_params, pos_sel, par_inputs_file)
    
    print("\n--- Step 4 & 5: Zipping and Cleaning Up ---")
    stats_zip_base = f"{demographic_model}_batch{sim_serial}_cms_stats_all"
    zip_and_cleanup(collated_files, stats_zip_base, output_dir)
    
    if os.path.exists(par_inputs_file) and os.path.getsize(par_inputs_file) > 0:
        par_inputs_base = os.path.basename(par_inputs_file).replace(".tsv", "")
        zip_and_cleanup([par_inputs_file], par_inputs_base, output_dir)
    
    script_end_time = datetime.now()
    print(f"\nPython Finalize: Script finished at {script_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total execution time: {script_end_time - script_start_time}")
PYTHON_SCRIPT_EOF
chmod +x "${PYTHON_FINALIZE_SCRIPT_NAME}"

# Execute the Python script
log_message "INFO" "Executing Python script for final collation and processing..."
python3 "./${PYTHON_FINALIZE_SCRIPT_NAME}"
python_exit_status=$?

if [ $python_exit_status -eq 0 ]; then
    log_message "INFO" "Python Collate & Finalize script completed successfully."
else
    log_message "ERROR" "Python Collate & Finalize script FAILED with exit status ${python_exit_status}."
fi

log_message "INFO" "Collation and finalization process finished."
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (11_collate_and_finalize.sh) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"