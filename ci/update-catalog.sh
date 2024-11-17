#!/bin/bash

source /mnt/CORDEX_CMIP6_tmp/software/miniforge3/etc/profile.d/conda.sh
env=catalog-update
conda env list
conda activate $env
python code/catalog/catalog.py

if [[ `git diff --quiet catalog.csv` ]]; then
  git commit catalog.csv catalog.xlsx -m"catalog update"
  git push origin main
else
  echo "no change!"
fi


# Exit the script with exit code 0
exit 0
