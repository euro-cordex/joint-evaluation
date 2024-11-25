#!/bin/bash

source /mnt/CORDEX_CMIP6_tmp/software/miniforge3/etc/profile.d/conda.sh
env=eval-book
conda env list
conda activate $env

jupyter nbconvert --to notebook --execute eval-book/eobs.ipynb

