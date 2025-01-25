#!/bin/bash

source /mnt/CORDEX_CMIP6_tmp/software/miniforge3/etc/profile.d/conda.sh
env=eval-book
conda env list
conda activate $env

jupyter nbconvert --to notebook --execute eval-book/eobs.ipynb

# Check if catalog.csv has changed
git diff --quiet eval-book/eobs.nbconvert.ipynb
changed=$?

if [ $changed -ne 0 ]; then
  echo "committing changes!"
  git add eval-book/eobs.nbconvert.ipynb
  git commit --author="github-actions[bot] <github-actions[bot]@users.noreply.github.com>" -m "notebook update"
  git push origin main
else
  echo "no change!"
fi
