#!/bin/bash

source /mnt/CORDEX_CMIP6_tmp/software/miniforge3/etc/profile.d/conda.sh
env=catalog-update
conda env list
conda activate $env
python code/catalog/catalog.py

# Check if catalog.csv has changed
git diff --quiet catalog.csv
changed=$?

if [ $changed -ne 0 ]; then
  echo "committing changes!"
  git add catalog.csv catalog.xlsx
  git commit -m "catalog update"
  git push origin main
else
  echo "no change!"
fi

# Exit the script with exit code 0
exit 0
