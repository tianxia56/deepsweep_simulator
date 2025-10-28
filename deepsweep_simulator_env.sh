#!/bin/bash

# This script sets up a Conda environment named 'deepsweep_simulator' using a Conda-first approach.
# It installs Python 3.9, R-base, all necessary Python/R packages, and bioinformatics tools
# (iSAFE, hapbin, selscan, cosi2) required for the deepsweep data generation pipeline.

# Exit immediately if a command exits with a non-zero status.
set -e
set -o pipefail # Added for robustness in pipelines

ENV_NAME="deepsweep_simulator"
echo "Starting setup script for '$ENV_NAME' Conda environment..."

# --- Miniconda Module Load ---
# Load miniconda if not already loaded. This assumes 'module' command is available.
echo "Checking for miniconda environment..."
if command -v module &> /dev/null && ! command -v conda &> /dev/null; then
    echo "Attempting to load miniconda module..."
    module load miniconda || { echo "Error: Failed to load miniconda module. Please check your module setup."; exit 1; }
    echo "Miniconda module loaded."
elif command -v conda &> /dev/null; then
    echo "Conda is already in PATH. Skipping miniconda module load."
else
    echo "Warning: 'module' command not found or miniconda not loaded and conda not in PATH."
    echo "Please ensure miniconda is available or loaded before running this script."
    exit 1
fi

# --- Conda Environment Creation ---
echo "Creating Conda environment '$ENV_NAME' with all dependencies..."
# Check if the environment already exists, remove and recreate for a clean slate
if conda info --env | grep -q "${ENV_NAME}"; then
    echo "Conda environment '$ENV_NAME' already exists. Removing and recreating."
    conda env remove -n "$ENV_NAME" -y || { echo "Error: Failed to remove existing Conda environment '$ENV_NAME'."; exit 1; }
fi

# Create the environment and install all Python/system packages in one step for best dependency resolution.
# We prioritize conda-forge and bioconda channels.
# **FIXED**: Comments have been moved outside the multi-line command.
conda create -n "$ENV_NAME" -c conda-forge -c bioconda -y \
    python=3.9 \
    r-base \
    pip setuptools wheel \
    pandas \
    numpy \
    scipy \
    scikit-learn \
    matplotlib \
    seaborn \
    pyfaidx \
    pysam \
    compilers \
    cmake \
    git \
    zlib \
    bzip2 \
    xz \
    libcurl \
    ncurses \
    wget \
    unzip \
    jq \
    || { echo "Error: Failed to create Conda environment '$ENV_NAME' with all dependencies."; exit 1; }

echo "Conda environment '$ENV_NAME' created with all core packages."

# --- Activate Conda Environment ---
echo "Activating Conda environment '$ENV_NAME'..."
# Source conda.sh to ensure 'conda activate' works in scripts
source "$(conda info --base)/etc/profile.d/conda.sh" || { echo "Error: Could not source conda.sh. Ensure conda is properly installed and initialized."; exit 1; }
conda activate "$ENV_NAME" || { echo "Error: Failed to activate Conda environment '$ENV_NAME'."; exit 1; }
echo "Conda environment '$ENV_NAME' activated."

# Define the base directory for custom tools source code
TOOLS_BASE_DIR="$HOME/project_pi_skr2/tx56/tools_for_${ENV_NAME}_env"

# Create the tools base directory and navigate into it
echo "Setting up custom tools directory: $TOOLS_BASE_DIR"
mkdir -p "$TOOLS_BASE_DIR" || { echo "Error: Failed to create tools directory '$TOOLS_BASE_DIR'."; exit 1; }
cd "$TOOLS_BASE_DIR"
echo "Current working directory for custom tools: $(pwd)"

# --- Install iSAFE ---
echo ""
echo ">>> Installing iSAFE..."
if [ -d "iSAFE" ]; then rm -rf iSAFE; fi # Clean up previous clone if exists
git clone --depth 1 https://github.com/alek0991/iSAFE.git || { echo "Error: Failed to clone iSAFE repository."; exit 1; }
cd iSAFE
# All dependencies from requirements.txt were already installed via Conda.
# We now use pip to install the iSAFE source code itself into the environment. This is a safe use of pip.
pip install --no-cache-dir . || { echo "Error: Failed to install iSAFE from source using pip."; exit 1; }
cd "$TOOLS_BASE_DIR"
echo "iSAFE installation complete."

# --- Install hapbin (ihsbin, xpehhbin) ---
echo ""
echo ">>> Installing hapbin (ihsbin, xpehhbin)..."
if [ -d "hapbin" ]; then rm -rf hapbin; fi # Clean up previous clone if exists
git clone https://github.com/evotools/hapbin.git || { echo "Error: Failed to clone hapbin repository."; exit 1; }
cd hapbin/build
# Use the CMAKE_POLICY_VERSION_MINIMUM flag to fix compatibility with modern CMake
cmake -DCMAKE_POLICY_VERSION_MINIMUM=3.5 ../src || { echo "Error: Failed to configure hapbin with CMake."; exit 1; }
make -j$(nproc) || { echo "Error: Failed to compile hapbin."; exit 1; }

# Copy compiled executables to the Conda env's bin directory
echo "Copying hapbin executables to $CONDA_PREFIX/bin/"
cp ./ihsbin "$CONDA_PREFIX"/bin/ || { echo "Error: Failed to copy ihsbin."; exit 1; }
cp ./xpehhbin "$CONDA_PREFIX"/bin/ || { echo "Error: Failed to copy xpehhbin."; exit 1; }

# Verify installation
echo "Verifying hapbin installation:"
ls -l "$CONDA_PREFIX"/bin/ihsbin "$CONDA_PREFIX"/bin/xpehhbin || { echo "Warning: hapbin executables not found where expected."; }
ihsbin --help | head -n 1 || { echo "Warning: ihsbin command verification failed."; }
xpehhbin --help | head -n 1 || { echo "Warning: xpehhbin command verification failed."; }

cd "$TOOLS_BASE_DIR"
echo "hapbin installation complete."

# --- Install selscan ---
echo ""
echo ">>> Installing selscan..."
if [ -d "selscan" ]; then rm -rf selscan; fi # Clean up previous clone if exists
git clone https://github.com/szpiech/selscan.git || { echo "Error: Failed to clone selscan repository."; exit 1; }
cd selscan/src
make clean || { echo "Warning: 'make clean' for selscan failed, continuing anyway."; }
make || { echo "Error: Failed to compile selscan."; exit 1; }
echo "Copying selscan executables to $CONDA_PREFIX/bin/"
cp ./selscan "$CONDA_PREFIX"/bin/ || { echo "Error: Failed to copy selscan executable."; exit 1; }
cp ./norm "$CONDA_PREFIX"/bin/ || { echo "Error: Failed to copy norm executable (part of selscan)."; exit 1; }
cd "$TOOLS_BASE_DIR"
echo "selscan installation complete."

# --- Install cosi2 (coalescent) - Conservative Universal Method ---
echo ""
echo ">>> Installing cosi2 (coalescent) using a conservative approach..."
COSI2_URL="https://github.com/broadinstitute/cosi2/archive/refs/heads/is-251008-drop-imposs-traj.zip"
ZIP_FILE="is-251008-drop-imposs-traj.zip"
SOURCE_DIR="cosi2-is-251008-drop-imposs-traj"
BASE_CXXFLAGS="-O2 -g -Wno-error=deprecated-declarations -Wno-error=maybe-uninitialized -Wno-error=array-bounds -Wno-error=stringop-overflow -fpermissive -std=c++11"
mkdir -p cosi2_build || { echo "Error: Failed to create cosi2_build directory."; exit 1; }
cd cosi2_build
echo "Downloading cosi2 from $COSI2_URL..."
rm -f "${ZIP_FILE}"
rm -rf "${SOURCE_DIR}"
wget "${COSI2_URL}" -O "${ZIP_FILE}" || { echo "Error: Failed to download cosi2 zip file."; exit 1; }
unzip "${ZIP_FILE}" || { echo "Error: Failed to unzip cosi2 archive."; exit 1; }
cd "${SOURCE_DIR}"
echo "Configuring cosi2 with CXXFLAGS: ${BASE_CXXFLAGS}"
chmod +x ./configure
./configure CXXFLAGS="${BASE_CXXFLAGS}" || { echo "Error: Failed to configure cosi2."; exit 1; }
echo "Patching Makefile to ensure conservative compilation flags..."
sed -i 's/-Werror[^[:space:]]*//g' Makefile
if ! grep -q "\-Wno-error=array-bounds" Makefile; then sed -i '/^CXXFLAGS =/s/$/ -Wno-error=array-bounds/' Makefile; fi
if ! grep -q "\-Wno-error=deprecated-declarations" Makefile; then sed -i '/^CXXFLAGS =/s/$/ -Wno-error=deprecated-declarations/' Makefile; fi
if ! grep -q "\-Wno-error=maybe-uninitialized" Makefile; then sed -i '/^CXXFLAGS =/s/$/ -Wno-error=maybe-uninitialized/' Makefile; fi
if ! grep -q "\-Wno-error=stringop-overflow" Makefile; then sed -i '/^CXXFLAGS =/s/$/ -Wno-error=stringop-overflow/' Makefile; fi
if ! grep -q "\-fpermissive" Makefile; then sed -i '/^CXXFLAGS =/s/$/ -fpermissive/' Makefile; fi
if ! grep -q "\-std=c++11" Makefile; then sed -i '/^CXXFLAGS =/s/$/ -std=c++11/' Makefile; fi
echo "Compiling cosi2..."
make || { echo "Error: Failed to compile cosi2. Please review the compilation log above."; exit 1; }
echo "Copying cosi2 executable to $CONDA_PREFIX/bin/"
cp coalescent "$CONDA_PREFIX"/bin/ || { echo "Error: Failed to copy coalescent executable."; exit 1; }
echo "Cleaning up cosi2 build artifacts..."
cd ..; rm -rf "${SOURCE_DIR}"; rm -f "${ZIP_FILE}"; cd "$TOOLS_BASE_DIR"
echo "cosi2 installation complete."

# --- Install R Packages ---
echo ""
echo ">>> Installing R packages..."
R --vanilla <<-EOF
  if (is.null(getOption("Ncpus"))) { options(Ncpus = parallel::detectCores(logical = TRUE)) }
  options(repos = c(CRAN = "https://cloud.r-project.org/"))
  cran_packages <- c("data.table", "dplyr", "readr", "stringr", "tidyr", "argparse", "ggplot2", "ggrepel", "gggenes", "remotes")
  bioc_packages <- c("biomaRt")
  if (!requireNamespace("BiocManager", quietly = TRUE)) { install.packages("BiocManager") }
  for (pkg in cran_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) { message(paste("Installing CRAN package:", pkg)); install.packages(pkg) }
  }
  for (pkg in bioc_packages) {
    if (!requireNamespace(pkg, quietly = TRUE)) { message(paste("Installing Bioconductor package:", pkg)); BiocManager::install(pkg, update = FALSE, ask = FALSE, force = TRUE) }
  }
  print("All required R packages should now be installed.")
EOF
if [ $? -ne 0 ]; then
    echo "ERROR: R package installation failed. Please review the R output above." >&2
    exit 1
fi
echo "R and R package installation complete."

echo ""
echo "*************************************************************"
echo "All installations for '$ENV_NAME' environment are complete!"
echo "Executables are available in your PATH when '$ENV_NAME' is active."
echo "*************************************************************"
echo "To deactivate this environment: conda deactivate"
echo "To reactivate this environment: conda activate $ENV_NAME"
echo ""
echo "Setup script finished successfully. Ready for running simulations."
echo "Test key modules by:"
echo "isafe -h"
echo "ihsbin -h"
echo "xpehhbin -h"
echo "selscan --help"
echo "coalescent"
echo "jq --version"
echo "R --version"
echo "python -c 'import pandas; import numpy; print(\"Pandas and Numpy are installed.\")'"
