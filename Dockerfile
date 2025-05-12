# Dockerfile for DeepSweep Test Environment
# Stage 1: Base setup from Dockerfile.base content
FROM python:3.9

# Updated Description Label:
LABEL description="Test environment with iSAFE, hapbin, selscan, R "

# Set DEBIAN_FRONTEND to noninteractive
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies from Dockerfile.base
RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    build-essential \
    cmake \
    zlib1g-dev \
    libbz2-dev \
    liblzma-dev \
    libcurl4-openssl-dev \
    libncurses5-dev \
    bash \
    # Added R dependencies here for efficiency (avoiding separate apt-get update)
    r-base \
    r-base-dev \
    software-properties-common \
    dirmngr \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install essential Python build tools
RUN pip install --no-cache-dir --upgrade pip wheel setuptools

# Create a working directory for cloning/building the tools
WORKDIR /opt/tools_src

# --- Stage 2: Install Software (Mirroring temporary container steps for TEST build) ---

# Install iSAFE 
RUN echo ">>> [TEST BUILD] Ensuring clean state for iSAFE installation..." \
    && rm -rf iSAFE \
    && echo ">>> [TEST BUILD] Installing iSAFE..." \
    && git clone --depth 1 https://github.com/alek0991/iSAFE.git \
    && cd iSAFE \
    && pip install --no-cache-dir -r requirements.txt \
    && python setup.py install \
    && cd .. \
    && echo ">>> [TEST BUILD] iSAFE installation complete (source retained in /opt/tools_src/iSAFE)."

# Install hapbin 
RUN echo ">>> [TEST BUILD] Ensuring clean state for hapbin installation..." \
    && rm -rf hapbin \
    && echo ">>> [TEST BUILD] Installing hapbin..." \
    && git clone https://github.com/evotools/hapbin.git \
    && cd hapbin/build \
    && echo ">>> [TEST BUILD] Running CMake for hapbin..." \
    && cmake ../src \
    && echo ">>> [TEST BUILD] Compiling hapbin..." \
    && make -j$(nproc) \
    && echo ">>> [TEST BUILD] Installing hapbin (expect a header file error at the end)..." \
    # Use || true or similar to ignore potential non-zero exit from make install
    && (make install || echo ">>> [TEST BUILD] Hapbin 'make install' finished (error for calcmpiselect.hpp is expected and handled).") \
    && echo ">>> [TEST BUILD] Updating shared library cache for hapbin..." \
    # ldconfig is crucial
    && ldconfig \
    && cd ../.. \
    && echo ">>> [TEST BUILD] hapbin installation complete (source retained in /opt/tools_src/hapbin)."

# Install selscan
RUN echo ">>> [TEST BUILD] Cloning selscan..." \
    # && rm -rf selscan
    && git clone https://github.com/szpiech/selscan.git \
    && cd selscan \
    && echo ">>> [TEST BUILD] Compiling selscan from source..." \
    && cd src \
    && make clean \
    && make \
    && echo ">>> [TEST BUILD] Selscan compilation complete. Verifying compiled files in src/..." \
    && if [ ! -f ./selscan ] || [ ! -f ./norm ]; then \
         echo "ERROR: [TEST BUILD] Compiled selscan or norm not found in ./src/ after make." >&2; \
         echo "Listing current directory (should be src/):" >&2; \
         pwd; \
         ls -la ./; \
         exit 1; \
       fi \
    && echo "[TEST BUILD] Compiled executables (selscan, norm) found in ./src/." \
    && echo ">>> [TEST BUILD] Copying compiled selscan executables from src/ to /usr/local/bin/..." \
    && cp ./selscan /usr/local/bin/ \
    && cp ./norm /usr/local/bin/ \
    && cd .. \
    && echo ">>> [TEST BUILD] selscan installation from source complete (source retained in /opt/tools_src/selscan)."

# Install R Packages
RUN echo ">>> [TEST BUILD] Installing R packages..." \
    && R -e "install.packages(c('dplyr', 'readr', 'stringr', 'tidyr', 'argparse', 'ggplot2', 'ggrepel', 'gggenes', 'BiocManager'), repos='https://cloud.r-project.org/', Ncpus=$(nproc))" \
    && R -e "BiocManager::install('biomaRt', update=FALSE, ask=FALSE)" \
    && echo ">>> [TEST BUILD] R and R package installation complete."

# Clean Up Apt Caches 
RUN echo ">>> [TEST BUILD] Cleaning up apt caches..." \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && echo ">>> [TEST BUILD] Apt cache cleanup complete."

# Optional: Set a final working directory for convenience when running the container
WORKDIR /work

# Default command when container starts
CMD ["bash"]
