#!/bin/bash

environment=$1

source /mnt/CORDEX_CMIP6_tmp/software/miniforge3/etc/profile.d/conda.sh
env=catalog-update
conda env remove -y -n $env || true
conda env create -f $1 -n $env
conda activate $env
which python
#python code/catalog/catalog.py

# Exit the script with exit code 0
exit 0
