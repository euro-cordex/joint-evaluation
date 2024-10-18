env = catalog-update
conda env remove -y -n $env || true
conda env create -f code/environment.yaml -n $env
