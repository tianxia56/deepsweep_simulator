#!/bin/bash

# This script collates all statistics, processes sweep parameters,
# finalizes output files by zipping them, and cleans up intermediate files.

# --- Python Helper Script Name ---
PYTHON_FINALIZE_SCRIPT_NAME="collate_finalize_core.py"

# --- Log Configuration ---
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/collate_finalize.log"

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

DOCKER_IMAGE_FINALIZE="docker.io/tx56/deepsweep_simulator:latest" 
HOST_CWD=$(pwd)

# --- Host-Side Pre-checks ---
log_message "INFO" "Host Script (11_collate_and_finalize.sh) Started: $(date)"
log_message "INFO" "Configuration file to be used by Python script inside Docker: ${CONFIG_FILE}"
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

if ! docker image inspect "$DOCKER_IMAGE_FINALIZE" &> /dev/null; then
    log_message "INFO" "Docker image ${DOCKER_IMAGE_FINALIZE} not found locally. Pulling..."
    if ! docker pull "$DOCKER_IMAGE_FINALIZE"; then log_message "ERROR" "Failed to pull Docker image ${DOCKER_IMAGE_FINALIZE}."; exit 1; fi
else
    log_message "INFO" "Docker image ${DOCKER_IMAGE_FINALIZE} already exists locally."
fi

log_message "DEBUG" "DOCKER_IMAGE_FINALIZE value is: [${DOCKER_IMAGE_FINALIZE}]"
log_message "DEBUG" "HOST_CWD value is: [${HOST_CWD}]"
log_message "DEBUG" "PYTHON_FINALIZE_SCRIPT_NAME value is: [${PYTHON_FINALIZE_SCRIPT_NAME}]"
log_message "DEBUG" "CONFIG_FILE value is: [${CONFIG_FILE}]"
log_message "INFO" "Starting Docker container for final collation and processing..."

# --- Docker Execution (Modified for clarity) ---
docker run \
    --rm \
    -i \
    --init \
    -u "$(id -u):$(id -g)" \
    -v "${HOST_CWD}:/app_data" \
    -w "/app_data" \
    -e PYTHON_FINALIZE_SCRIPT_NAME="${PYTHON_FINALIZE_SCRIPT_NAME}" \
    -e CONFIG_FILE_PATH_IN_CONTAINER="${CONFIG_FILE}" \
    "${DOCKER_IMAGE_FINALIZE}" \
    /bin/bash <<'EOF_INNER'

# --- Container Initialization ---
# (Python script and container logic from the last *complete* version I sent you goes here)
# This includes the cat > ... PYTHON_SCRIPT_EOF ... PYTHON_SCRIPT_EOF block
# and the subsequent container bash logic.
echo_container() { echo "Container: $1"; }
log_container() { echo_container "$1"; } 

cleanup_and_exit() {
    log_container "Caught signal! Cleaning up temporary Python script..."
    rm -f "./${PYTHON_FINALIZE_SCRIPT_NAME}"
    log_container "Exiting due to signal."
    exit 130
}
trap cleanup_and_exit INT TERM
log_container "----------------------------------------------------"
log_container "Container Script (Collate & Finalize) Started: $(date)"
log_container "----------------------------------------------------"
log_container "Config file to be used by Python: [${CONFIG_FILE_PATH_IN_CONTAINER}]" 
log_container "Python helper script will be: ./${PYTHON_FINALIZE_SCRIPT_NAME}"

# Create the Python helper script inside the container
cat > "./${PYTHON_FINALIZE_SCRIPT_NAME}" <<'PYTHON_SCRIPT_EOF'
import json
import os
import pandas as pd
from datetime import datetime
import zipfile
import warnings
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
    except json.JSONDecodeError: print(f"Python: Error - Configuration file '{cfg_file}' contains invalid JSON."); sys.exit(1)
    except Exception as e: print(f"Python: Error loading config '{cfg_file}': {e}"); sys.exit(1)

def get_config_value(config_data, key, default_val=None, is_critical=True):
    val = config_data.get(key, default_val)
    if is_critical and val is None: print(f"Python: Error - Critical key '{key}' missing or null in configuration."); sys.exit(1)
    return val

def collate_stats_for_sim(sim_id, demographic_model_name, sim_serial_num, runtime_dir_loc, onepop_stats_sel_dir_loc, two_pop_stats_dir_loc, norm_dir_loc, output_dir_loc):
    output_file = os.path.join(output_dir_loc, f"{demographic_model_name}_batch{sim_serial_num}_cms_stats_{sim_id}.tsv")
    print(f"Python Collate: Initial collation for simulation ID: {sim_id}")
    try:
        pop1_ref = str(config_data_global.get('selected_pop', 'ERROR_POP1_MISSING'))
        if pop1_ref == 'ERROR_POP1_MISSING': print(f"Python Collate: Error - 'selected_pop' missing from config."); return None
        
        fst_deldaf_file = os.path.join(two_pop_stats_dir_loc, f"sel.{sim_id}_0_{pop1_ref}_fst_deldaf.tsv")
        
        output_data = None 
        base_data_loaded = False
        if os.path.exists(fst_deldaf_file):
            try:
                output_data = pd.read_csv(fst_deldaf_file, sep='\t', dtype={'pos': str}) 
                if 'pos' in output_data.columns:
                    output_data['pos'] = pd.to_numeric(output_data['pos'], errors='coerce')
                    base_data_loaded = True
                else: output_data = None 
            except pd.errors.EmptyDataError: print(f"  Warning: FST/DelDAF file {fst_deldaf_file} is empty.")
            except Exception as e: print(f"  Warning: Error reading FST/DelDAF file {fst_deldaf_file}: {e}")

        if not base_data_loaded:
             print(f"  Info: FST/DelDAF file {fst_deldaf_file} not loaded or invalid. Trying iSAFE for base 'pos'.");
             isafe_check_file = os.path.join(onepop_stats_sel_dir_loc, f"{sim_id}.iSAFE.out") 
             if os.path.exists(isafe_check_file):
                 try:
                     temp_isafe = pd.read_csv(isafe_check_file, sep='\t', dtype={'POS': str}) 
                     if 'POS' in temp_isafe.columns:
                         output_data = temp_isafe[['POS']].rename(columns={'POS': 'pos'}).copy()
                         output_data['pos'] = pd.to_numeric(output_data['pos'], errors='coerce')
                         print(f"  Info: Using 'pos' column from {isafe_check_file} as base for sim_id {sim_id}.")
                         base_data_loaded = True
                     else: print(f"  Error: Fallback iSAFE file {isafe_check_file} also missing 'POS'. Cannot proceed for sim_id {sim_id}."); return None
                 except Exception as e: print(f"  Error reading fallback iSAFE file {isafe_check_file}: {e}"); return None
             else: print(f"  Error: Base FST/DelDAF file and fallback iSAFE not found. Skipping sim_id {sim_id}."); return None
        
        if output_data is None or 'pos' not in output_data.columns: print(f"  Error: 'pos' missing in base data for sim_id {sim_id}. Skipping."); return None
        
        output_data['pos'] = pd.to_numeric(output_data['pos'], errors='coerce'); output_data.dropna(subset=['pos'], inplace=True)

        isafe_file = os.path.join(onepop_stats_sel_dir_loc, f"{sim_id}.iSAFE.out") 
        if os.path.exists(isafe_file):
            try:
                isafe_data = pd.read_csv(isafe_file, sep='\t')
                if 'POS' in isafe_data.columns and 'iSAFE' in isafe_data.columns:
                    isafe_data.rename(columns={'POS': 'pos'}, inplace=True)
                    isafe_data['pos'] = pd.to_numeric(isafe_data['pos'], errors='coerce')
                    isafe_data['iSAFE'] = pd.to_numeric(isafe_data['iSAFE'], errors='coerce').round(4)
                    output_data = pd.merge(output_data, isafe_data[['pos', 'iSAFE']], on='pos', how='left')
                elif 'POS' not in isafe_data.columns: print(f"  Warning: 'POS' column missing in {isafe_file}")
                elif 'iSAFE' not in isafe_data.columns: print(f"  Warning: 'iSAFE' column missing in {isafe_file}")
            except Exception as e: print(f"  Warning: Error processing iSAFE file {isafe_file}: {e}")
        else: 
            print(f"  Info: File not found {isafe_file}. 'iSAFE' column will be NA.")
            if 'iSAFE' not in output_data.columns: output_data['iSAFE'] = pd.NA

        stats_to_merge = {
            "norm_ihs": os.path.join(norm_dir_loc, f"temp.ihs.{sim_id}.tsv"),
            "norm_nsl": os.path.join(norm_dir_loc, f"temp.nsl.{sim_id}.tsv"),
            "norm_ihh12": os.path.join(norm_dir_loc, f"temp.ihh12.{sim_id}.tsv"),
            "norm_delihh": os.path.join(norm_dir_loc, f"temp.delihh.{sim_id}.tsv"),
            "max_xpehh": os.path.join(norm_dir_loc, f"temp.max.xpehh.{sim_id}.tsv"), 
        }
        for col_name_target, file_path in stats_to_merge.items():
            col_name_in_file = col_name_target 
            if os.path.exists(file_path):
                try:
                    norm_data = pd.read_csv(file_path, sep='\t', dtype={'pos':str})
                    norm_data['pos'] = pd.to_numeric(norm_data['pos'], errors='coerce')
                    if 'pos' in norm_data.columns and col_name_in_file in norm_data.columns:
                         norm_data[col_name_in_file] = pd.to_numeric(norm_data[col_name_in_file], errors='coerce')
                         output_data = pd.merge(output_data, norm_data[['pos', col_name_in_file]], on='pos', how='left')
                         if col_name_in_file != col_name_target: output_data.rename(columns={col_name_in_file: col_name_target}, inplace=True)
                    elif 'pos' not in norm_data.columns: print(f"  Warning: 'pos' column missing in {file_path}")
                    else: print(f"  Warning: Expected column '{col_name_in_file}' not found in {file_path}")
                except Exception as e: print(f"  Warning: Error reading or merging {file_path}: {e}")
            else:
                print(f"  Info: File not found {file_path}. '{col_name_target}' column will be NA.")
                if col_name_target not in output_data.columns: output_data[col_name_target] = pd.NA
        
        output_data['sim_batch_no'] = int(sim_serial_num) 
        output_data['sim_id'] = int(sim_id) 
        
        output_data['deri_gen'] = pd.NA; output_data['sel_gen'] = pd.NA
        output_data['s'] = np.nan; output_data['selpos'] = 0 
        
        pop1_ref_str = str(config_data_global.get('selected_pop', ''))
        cols_order = ['sim_batch_no', 'sim_id', 'pos']
        stat_cols_ordered = []
        if 'mean_fst' in output_data.columns: stat_cols_ordered.append('mean_fst')
        if 'deldaf' in output_data.columns: stat_cols_ordered.append('deldaf')
        if 'daf_pop1' in output_data.columns: stat_cols_ordered.append('daf_pop1')
        if 'maf_pop1' in output_data.columns: stat_cols_ordered.append('maf_pop1')
        for pop_id_iter in config_data_global.get('pop_ids', []):
            if str(pop_id_iter) != pop1_ref_str:
                 fst_pair_col = f"fst_{pop1_ref_str}_vs_{pop_id_iter}"
                 if fst_pair_col in output_data.columns and fst_pair_col not in stat_cols_ordered: stat_cols_ordered.append(fst_pair_col)
        other_stats_ordered = ['iSAFE', 'norm_ihs', 'norm_nsl', 'norm_ihh12', 'norm_delihh', 'max_xpehh']
        for stat_col in other_stats_ordered:
            if stat_col in output_data.columns and stat_col not in stat_cols_ordered: stat_cols_ordered.append(stat_col)
        param_cols = ['deri_gen', 'sel_gen', 's', 'selpos']
        
        final_col_order = cols_order
        for col_group in [stat_cols_ordered, param_cols]: 
            for col in col_group:
                if col not in output_data.columns: 
                    output_data[col] = pd.NA if col in ['deri_gen', 'sel_gen'] else (0.0 if col == 's' else (0 if col == 'selpos' else pd.NA) )
                if col not in final_col_order: final_col_order.append(col) 

        remaining_cols = [c for c in output_data.columns if c not in final_col_order]
        final_col_order.extend(sorted(remaining_cols)) 
        final_col_order_unique = []
        for col in final_col_order:
            if col not in final_col_order_unique: final_col_order_unique.append(col)
        output_data = output_data[final_col_order_unique]

        # Ensure output directory exists (Python equivalent of mkdir -p)
        os.makedirs(output_dir_loc, exist_ok=True)
        output_data.to_csv(output_file, sep='\t', index=False, na_rep='NA')
        print(f"  Initial collated stats saved to: {output_file}")
        return output_file 
    except pd.errors.EmptyDataError: print(f"  Error: Base input file for sim_id {sim_id} is empty."); return None
    except Exception as e: print(f"  Error processing stats for sim_id {sim_id}: {e}"); import traceback; traceback.print_exc(); return None

def parse_sweep_parameters_from_cosi(runtime_dir_loc):
    """
    Parses cosi.sel.*.sampled_loci.csv files to extract sweep parameters (deri_gen, sel_gen, s)
    and the sim_id they apply to.
    A sweep parameter line is immediately followed by a line containing only the sim_id.
    Returns a DataFrame with columns: ['sim_id', 'deri_gen', 'sel_gen', 's']
    """
    print("\nPython: Parsing cosi.sel.*.sampled_loci.csv files for sweep parameters...")
    all_sweep_params = []
    
    cosi_sel_pattern = os.path.join(runtime_dir_loc, "cosi.sel.*.sampled_loci.csv")
    files_to_parse = glob.glob(cosi_sel_pattern)

    if not files_to_parse:
        print(f"  No cosi.sel.*.sampled_loci.csv files found in {runtime_dir_loc}.")
        return pd.DataFrame(columns=['sim_id', 'deri_gen', 'sel_gen', 's'])

    for cosi_file_path in files_to_parse:
        print(f"  Scanning file: {os.path.basename(cosi_file_path)}")
        try:
            with open(cosi_file_path, 'r') as f_cosi:
                lines = [line.strip() for line in f_cosi if line.strip()]
            if not lines:
                print(f"    File {os.path.basename(cosi_file_path)} is empty.")
                continue
        except Exception as e:
            print(f"  Warning: Error reading {cosi_file_path}: {e}")
            continue

        line_idx = 0
        while line_idx < (len(lines) - 1) : # Need at least two lines: one for sweep, one for sim_id
            current_line = lines[line_idx]
            parts = current_line.split()
            
            deri_gen, sel_gen, s_val = pd.NA, pd.NA, np.nan
            event_type = ""
            is_sweep_param_line = False

            if parts and (parts[0] == "sweep_mult" or parts[0] == "sweep_mult_standing"):
                event_type = parts[0]
                is_sweep_param_line = True
            
            if is_sweep_param_line:
                # The next line MUST be the sim_id
                next_line_str = lines[line_idx + 1]
                sim_id_str_from_file = next_line_str.strip().split()[0] if next_line_str.strip() else ""
                sim_id = None

                if sim_id_str_from_file.isdigit():
                    sim_id = int(sim_id_str_from_file)
                    # print(f"    Potential sweep line: '{current_line}'") # Debug
                    # print(f"      Followed by sim_id: {sim_id}") # Debug
                    try:
                        if event_type == "sweep_mult" and len(parts) >= 5: # sweep_mult "sweep" pop Tgen s
                            common_gen_val = int(round(float(parts[3]))) 
                            deri_gen = common_gen_val; sel_gen = common_gen_val
                            s_val = float(parts[4]) 
                        elif event_type == "sweep_mult_standing" and len(parts) >= 9: # sweep_mult_standing "sweep" pop deri_gen s ... sel_gen
                            deri_gen = int(round(float(parts[3]))) 
                            s_val = float(parts[4])                
                            sel_gen = int(round(float(parts[8]))) 
                        else:
                            print(f"    Warning: SimID {sim_id if sim_id is not None else 'Unknown'}: Line '{current_line}' looks like sweep but has insufficient fields for type '{event_type}'.")
                            line_idx += 1 # Advance past current line, next iter will check next_line_str
                            continue 
                        
                        if pd.notna(s_val) and pd.notna(deri_gen) and pd.notna(sel_gen):
                            all_sweep_params.append({'sim_id': sim_id, 'deri_gen': deri_gen, 'sel_gen': sel_gen, 's': s_val})
                            print(f"    Parsed for sim_id={sim_id}: type={event_type}, deri={deri_gen}, sel={sel_gen}, s={s_val:.4e}")
                        
                        line_idx += 2 # Successfully processed sweep line and its sim_id line
                        continue # Move to the line after sim_id for next potential sweep
                            
                    except Exception as e: 
                        print(f"    Error parsing numeric sweep parameters for SimID {sim_id if sim_id is not None else 'Unknown'} from line '{current_line}': {e}")
                        line_idx += 2 # Skip this pair of lines on error
                        continue
                else:
                    # Current line looked like a sweep, but next line wasn't a sim_id.
                    # This means the current line was NOT the one associated with a sim_id.
                    print(f"    Info: Line '{current_line}' looks like sweep, but next line '{next_line_str}' is not a sim_id. Continuing search.")
                    line_idx += 1 # Advance past current line only, next iter will evaluate next_line_str
                    continue
            else:
                # Current line is not a sweep parameter line
                line_idx += 1
    
    if not all_sweep_params:
        print("  Info: No sweep parameters successfully parsed from any cosi.sel.*.sampled_loci.csv files.")
        return pd.DataFrame(columns=['sim_id', 'deri_gen', 'sel_gen', 's'])
    
    sweep_df = pd.DataFrame(all_sweep_params).drop_duplicates(subset=['sim_id'], keep='last')
    sweep_df['sim_id'] = sweep_df['sim_id'].astype(int)
    sweep_df['deri_gen'] = pd.to_numeric(sweep_df['deri_gen'], errors='coerce').astype('Int64')
    sweep_df['sel_gen'] = pd.to_numeric(sweep_df['sel_gen'], errors='coerce').astype('Int64')
    sweep_df['s'] = pd.to_numeric(sweep_df['s'], errors='coerce')
    print(f"  Parsed sweep parameters for {len(sweep_df)} unique sim_ids from sampled_loci files.")
    return sweep_df

def finalize_collated_files_and_extract_metadata(successfully_collated_sim_files, sweep_params_df, output_dir_loc, demographic_model_name, sim_serial_num, pos_sel_target, par_inputs_file_path):
    print("\nPython: Finalizing collated files with sweep parameters and extracting metadata...")
    par_inputs_header_written = False
    if os.path.exists(par_inputs_file_path):
        try: os.remove(par_inputs_file_path); print(f"  Cleared existing parameter file: {par_inputs_file_path}")
        except OSError as e: print(f"  Warning: Could not remove {par_inputs_file_path}. Error: {e}")

    for collated_file_path in successfully_collated_sim_files: 
        try:
            sim_id_from_filename = int(os.path.basename(collated_file_path).split('_cms_stats_')[-1].split('.tsv')[0])
            print(f"  Finalizing file: {collated_file_path} for sim_id {sim_id_from_filename}")
            
            output_data = pd.read_csv(collated_file_path, sep='\t')
            # Ensure sim_id column is int for merging with sweep_params_df
            if 'sim_id' not in output_data.columns: output_data['sim_id'] = sim_id_from_filename 
            output_data['sim_id'] = output_data['sim_id'].astype(int)

            current_sweep_params_row = sweep_params_df[sweep_params_df['sim_id'] == sim_id_from_filename]
            
            # Initialize these columns if they were not added by collate_stats_for_sim
            for col_name_to_init in ['deri_gen', 'sel_gen', 's', 'selpos']:
                if col_name_to_init not in output_data.columns:
                    output_data[col_name_to_init] = pd.NA if col_name_to_init in ['deri_gen', 'sel_gen'] else (0.0 if col_name_to_init == 's' else 0)
                else: # If exists, ensure correct initial type for rows not matching pos_sel_target
                    if col_name_to_init in ['deri_gen', 'sel_gen']: output_data[col_name_to_init] = pd.NA
                    elif col_name_to_init == 's': output_data[col_name_to_init] = np.nan
                    elif col_name_to_init == 'selpos': output_data[col_name_to_init] = 0

            output_data['pos'] = pd.to_numeric(output_data['pos'], errors='coerce')
            pos_sel_target_numeric = pd.to_numeric(pos_sel_target, errors='coerce')

            if not pd.isna(pos_sel_target_numeric):
                target_row_mask = (output_data['pos'] == pos_sel_target_numeric)
                if target_row_mask.any():
                    output_data.loc[target_row_mask, 'selpos'] = 1
                    if not current_sweep_params_row.empty:
                        params = current_sweep_params_row.iloc[0]
                        output_data.loc[target_row_mask, 'deri_gen'] = params.get('deri_gen', pd.NA)
                        output_data.loc[target_row_mask, 'sel_gen'] = params.get('sel_gen', pd.NA)
                        output_data.loc[target_row_mask, 's'] = params.get('s', np.nan)
                        print(f"    Applied sweep params to pos {pos_sel_target_numeric} for sim_id {sim_id_from_filename}")
                    else: print(f"    Warning: No sweep parameters found in sweep_params_df for sim_id {sim_id_from_filename}.")
                    
                    pos_sel_extract = output_data[target_row_mask].copy()
                    current_file_exists_and_has_content = os.path.exists(par_inputs_file_path) and os.path.getsize(par_inputs_file_path) > 0
                    write_header_for_par_inputs = not current_file_exists_and_has_content
                    if not par_inputs_header_written and current_file_exists_and_has_content :
                         write_header_for_par_inputs = False 
                    
                    pos_sel_extract.to_csv(par_inputs_file_path, mode='a', 
                                           header=write_header_for_par_inputs, 
                                           index=False, sep='\t', na_rep='NA')
                    if write_header_for_par_inputs and not par_inputs_header_written: # Only set once per overall run
                        par_inputs_header_written = True 
            
            # Final type casting for the main collated file
            output_data['deri_gen'] = output_data['deri_gen'].astype('Int64')
            output_data['sel_gen'] = output_data['sel_gen'].astype('Int64')
            output_data['s'] = pd.to_numeric(output_data['s'], errors='coerce') # Keep s as float
            output_data['selpos'] = output_data['selpos'].astype(int)
            
            # Reorder columns to ensure sweep params are at the end
            pop1_ref_str = str(config_data_global.get('selected_pop', '')) 
            cols_order = ['sim_batch_no', 'sim_id', 'pos']
            # Gather all existing stat columns, excluding sweep params for now
            stat_cols = [
                'mean_fst', 'deldaf', 'daf_pop1', 'maf_pop1',
                'iSAFE', 'norm_ihs', 'norm_nsl', 'norm_ihh12', 'norm_delihh', 'max_xpehh'
            ]
            # Add FST pair columns dynamically
            fst_pair_cols_to_add = []
            for pop_id_iter_val in config_data_global.get('pop_ids', []):
                pop_id_iter = str(pop_id_iter_val) # Ensure string for comparison
                if pop_id_iter != pop1_ref_str: 
                    fst_pair_col_name = f"fst_{pop1_ref_str}_vs_{pop_id_iter}"
                    if fst_pair_col_name in output_data.columns:
                        fst_pair_cols_to_add.append(fst_pair_col_name)
            
            final_column_order = cols_order + \
                                 [col for col in stat_cols if col in output_data.columns] + \
                                 sorted([col for col in fst_pair_cols_to_add if col in output_data.columns]) + \
                                 ['deri_gen', 'sel_gen', 's', 'selpos']
            
            # Add any other columns that might exist but weren't explicitly ordered
            other_existing_cols = [col for col in output_data.columns if col not in final_column_order]
            final_column_order.extend(sorted(other_existing_cols))
            
            # Ensure unique columns in the final order, preserving first occurrence
            seen_cols = set()
            final_column_order_unique = [x for x in final_column_order if not (x in seen_cols or seen_cols.add(x))]
            
            output_data = output_data[final_column_order_unique]

            output_data.to_csv(collated_file_path, sep='\t', index=False, na_rep='NA')
            print(f"  Updated collated file with sweep parameters: {collated_file_path}")

        except Exception as e: print(f"  Error during final parameter append for {collated_file_path}: {e}"); import traceback; traceback.print_exc()
    print(f"Finished finalizing collated files.")

def zip_output_files(file_pattern_prefix, zip_filename_base, output_dir_loc):
    current_date = datetime.now().strftime("%Y-%m-%d"); zipfile_name = os.path.join(output_dir_loc, f"{zip_filename_base}_{current_date}.zip")
    print(f"\nPython: Creating zip file: {zipfile_name}")
    files_to_zip = [os.path.join(output_dir_loc, f) for f in os.listdir(output_dir_loc) if f.startswith(file_pattern_prefix) and f.endswith(".tsv")]
    if not files_to_zip: print("  Warning: No files found matching pattern to zip."); return False
    try:
        with zipfile.ZipFile(zipfile_name, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zipf:
            for file_path in files_to_zip: zipf.write(file_path, os.path.basename(file_path))
        print(f"  Successfully created zip file with {len(files_to_zip)} files."); return True
    except Exception as e: print(f"  Error creating zip file {zipfile_name}: {e}"); return False

def cleanup_tsv_files(files_to_remove_list):
    print("\nPython: Cleaning up individual TSV files..."); removed_count = 0
    for file_path in files_to_remove_list:
        if os.path.exists(file_path):
            try: os.remove(file_path); removed_count += 1
            except Exception as e: print(f"  Error removing file {file_path}: {e}")
    print(f"  Removed {removed_count} files.")

config_data_global = {} 
sim_ids_global_list = []

if __name__ == "__main__":
    script_start_time = datetime.now()
    print(f"Python Finalize: Starting script execution at {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    config_data_global = load_config(config_file_name)
    demographic_model_name = get_config_value(config_data_global, 'demographic_model')
    sim_serial_num = get_config_value(config_data_global, 'simulation_serial_number')
    pos_sel_target = get_config_value(config_data_global, 'pos_sel_position')
    runtime_dir_main = "runtime"; output_dir_main = "output"
    onepop_stats_sel_dir_main = "one_pop_stats_sel"; two_pop_stats_dir_main = "two_pop_stats"; norm_dir_main = "norm"
    sim_id_csv_file = os.path.join(runtime_dir_main, "nsl.sel.runtime.csv") 
    try:
        sim_ids_series = pd.read_csv(sim_id_csv_file, header=None, usecols=[1]).iloc[:, 0]
        sim_ids_global_list = sim_ids_series.dropna().astype(int).unique().tolist() 
        if not sim_ids_global_list: print(f"Python: Error - No simulation IDs found in '{sim_id_csv_file}'."); sys.exit(1)
        print(f"Python: Found {len(sim_ids_global_list)} unique simulation IDs: {sim_ids_global_list}")
    except Exception as e: print(f"Python: Error reading simulation IDs from '{sim_id_csv_file}': {e}"); sys.exit(1)
    os.makedirs(output_dir_main, exist_ok=True)
    print("\nPython: --- Step 1: Initial Collation of statistics (placeholders for sweep params) ---")
    collated_files_generated = []
    for sim_id_val in sim_ids_global_list: 
        collated_file_path = collate_stats_for_sim(sim_id_val, demographic_model_name, sim_serial_num, runtime_dir_main, onepop_stats_sel_dir_main, two_pop_stats_dir_main, norm_dir_main, output_dir_main)
        if collated_file_path: collated_files_generated.append(collated_file_path)
    if not collated_files_generated: print("\nPython: CRITICAL ERROR - No simulations were successfully collated in Step 1. Exiting."); sys.exit(1)

    print("\nPython: --- Step 2: Parsing sweep parameters from all cosi.sel.*.sampled_loci.csv files ---")
    all_sweep_parameters_df = parse_sweep_parameters_from_cosi(runtime_dir_main)

    print("\nPython: --- Step 3: Finalizing collated files and creating par_inputs (metadata) file ---")
    current_date_param_file_str = datetime.now().strftime("%Y-%m-%d")
    par_inputs_file_main = os.path.join(output_dir_main, f"{demographic_model_name}_batch{sim_serial_num}_par_inputs_{current_date_param_file_str}.tsv")
    finalize_collated_files_and_extract_metadata(collated_files_generated, all_sweep_parameters_df, output_dir_main, demographic_model_name, sim_serial_num, pos_sel_target, par_inputs_file_main)
    
    print("\nPython: --- Step 4: Zipping collated statistics files ---")
    collated_file_pattern_prefix = f"{demographic_model_name}_batch{sim_serial_num}_cms_stats_"
    zip_base_name_collated = f"{demographic_model_name}_batch{sim_serial_num}_cms_stats_all"
    stats_files_zipped_success = zip_output_files(collated_file_pattern_prefix, zip_base_name_collated, output_dir_main)
    
    print("\nPython: --- Step 5: Zipping parameter input file ---")
    param_file_zipped_success = False
    if os.path.exists(par_inputs_file_main) and os.path.getsize(par_inputs_file_main) > 0 : 
        par_inputs_basename_no_ext = os.path.basename(par_inputs_file_main).replace(".tsv", "")
        param_file_zipped_success = zip_output_files(par_inputs_basename_no_ext, par_inputs_basename_no_ext, output_dir_main)
    else: print(f"  Skipping zip for parameter file: {par_inputs_file_main} not found or empty.")
    
    print("\nPython: --- Step 6: Cleaning up ---")
    if stats_files_zipped_success: cleanup_tsv_files(collated_files_generated)
    else: print("  Skipping cleanup of collated stats TSV files because zipping failed or no files to zip.")
    if param_file_zipped_success and os.path.exists(par_inputs_file_main): cleanup_tsv_files([par_inputs_file_main])
    elif os.path.exists(par_inputs_file_main): print("  Skipping cleanup of parameter input TSV file as it was not zipped (or zipping failed).")
    
    script_end_time = datetime.now()
    print(f"\nPython Finalize: Script finished at {script_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Python Finalize: Total execution time: {script_end_time - script_start_time}")
    print("Python Finalize: Processing complete.")

PYTHON_SCRIPT_EOF
chmod +x "./${PYTHON_FINALIZE_SCRIPT_NAME}"
log_container "Python Collate & Finalize script created."

# --- Main Execution in Container ---
log_container "Executing Python script for final collation and processing..."
python3 "./${PYTHON_FINALIZE_SCRIPT_NAME}"
python_exit_status=$?
if [ $python_exit_status -eq 0 ]; then
    log_container "Python Collate & Finalize script completed successfully."
else
    log_container "ERROR: Python Collate & Finalize script FAILED with exit status ${python_exit_status}."
fi
rm -f "./${PYTHON_FINALIZE_SCRIPT_NAME}"
log_container "Python helper script removed."
log_container "Collation and finalization process finished."
log_container "----------------------------------------------------"
log_container "Container Script (Collate & Finalize) Finished: $(date)"
log_container "----------------------------------------------------"
EOF_INNER

# --- Host Post-run ---
docker_exit_status=$? 
log_message "INFO" "Docker container (Collate & Finalize) finished with exit status: ${docker_exit_status}."
if [ ${docker_exit_status} -eq 130 ]; then log_message "INFO" "Script (Collate & Finalize) likely interrupted."; fi
if [ ${docker_exit_status} -ne 0 ] && [ ${docker_exit_status} -ne 130 ]; then log_message "ERROR" "Docker container (Collate & Finalize) reported an error."; fi
log_message "INFO" "----------------------------------------------------"
log_message "INFO" "Host Script (11_collate_and_finalize.sh) Finished: $(date)"
log_message "INFO" "----------------------------------------------------"