#!/bin/bash

file=$1
name=$2

source /mnt/CORDEX_CMIP6_tmp/software/miniforge3/etc/profile.d/conda.sh
conda env remove -y -n $name || true
conda env create -f $file -n $name
conda activate $name
which python
#python code/catalog/catalog.py

# Exit the script with exit code 0
exit 0
