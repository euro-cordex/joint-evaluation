#!/bin/bash

source /mnt/CORDEX_CMIP6_tmp/software/miniforge3/etc/profile.d/conda.sh
env=catalog-update
conda env list
conda activate $env
python code/catalog/catalog.py

# Exit the script with exit code 0
exit 0
