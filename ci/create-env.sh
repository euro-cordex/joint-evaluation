conda env remove -y -n catalog-update || true
conda env create -f code/environment.yaml -n catalog-update
