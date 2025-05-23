# DeepSweep Simulator Showcase (Local Execution)

This repository provides a showcase for generating simulated genotypes and compute population genetics statistics to detect selective signatures, originally designed for a cluster environment, on a local machine in a linear fashion. 

## Requirements

*   **Docker:** This pipeline relies on Docker to run containerized tools. If you don't have Docker installed, please follow the official installation guide for your operating system:
    *   [Install Docker Engine](https://docs.docker.com/engine/install/)
    *   Can build or customize your deepsweep env docker image by saving ```Dockerfile``` in any pwd, edit and ```docker build -t deepsweep_env_debugged_test:latest .```
    *   ```docker run -it --rm deepsweep_env_debugged_test:latest bash``` launch interactive shell to test
    *   isafe -h
    *   ihsbin
    *   selscan --help
    *   R --version

## How to use

1.  **Configuration:**

    Edit the `00config.json` file in the root directory to set your desired simulation parameters. This file controls aspects like:
    *   Number of neutral and selection simulations
    *   Demographic model file
    *   Selective sweep parameters: pop_event sweep_mult \"sweep\" {sweep pop} {derived allele age by generations ago} {selection coefficient} {selected variant relative position} {final daf range} {selection begin pop} {selection begin generations ago}
    *   Target population id for analysis, versatile to include/exclude pops in the demographic model
    *   Simulation length
    *   Position of selection should equal to {selected variant relative position} times simulation length, e.g. 0.5*3mb=1.5mb


    Example of `00config.json`:


    ```json
    {
        "selected_simulation_number": 2,
        "simulation_serial_number": 1,
        "neutral_simulation_number": 2,
        "demographic_model": "jv_default_112115_825am.par",
        "selective_sweep": "pop_event sweep_mult_standing \"sweep\" 1 U(0, 5000) E(20) .5 .05-.95 1 U(0, 5000)",
        "selected_pop": 1,
        "pos_sel_position": 1500000,
        "simulation_length": 3000000,
        "pop_ids": [1, 2, 3, 4]
    }
    ```

2.  **Ensure Necessary Files are Present:**
    *   Make sure your demographic model file is located in the `demographic_models/` directory.
    *   Make sure your recombination map file (0 values are not allowed) is present in the project root directory.

To execute the entire pipeline, run the main launch script from the root directory of this project:

```bash
bash launch.sh
```



3. **Output:**
    *   Simulation summary statistics.
    *   Selected variant summary statistics with specific sweep input parameters (derived age, selected age, selection coefficient).
