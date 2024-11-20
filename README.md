# Joint evaluation
Joint evaluation of the CMIP6 downscaling within EURO-CORDEX.

This repository is supposed to hold meta data concerning the EURO-CORDEX joint evaluation, e.g., tables and catalogs. If you have any questions, please don't hesitate to [open an issue](https://github.com/euro-cordex/joint-evaluation/issues/new)!

## Data exchange
Some storage space on the existing jsc-cordex data exchange infrastructure at Jülich Supercomputing Centre (JSC) is available and may be used to provide some intermediate, temporary, limited storage for the first joint analyses publications of CORDEX-CMIP6 simulations before data will be stored longer-term on ESGF-related storage at the respective centres.

Access to storate at JSC is decribed [here](https://github.com/euro-cordex/jsc-cordex) (private repo until this is released to the community).

## Catalog
A catalog of available data at JSC-CORDEX is available from this repository. For example,
```python
import intake

cat = intake.open_esm_datastore("https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/CORDEX-CMIP6.json")
cat.keys()
```
gives
```
['CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.hurs.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.pr.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.prsn.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.ps.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.tas.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.tasmax.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.tasmin.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.uas.v20240529',
 'CORDEX.EUR-12.GERICS.ERA5.evaluation.r1i1p1f1.REMO2020.v1.mon.vas.v20240529']
```

## Starting Jupyter Lab

If you want to work interactively on jsc-cordex, you can use jupyterlab via ssh (right now, jsc-cordex is not available from [Jupyter-JSC](https://jupyter.jsc.fz-juelich.de)).
> [!NOTE]
> You need to activate a virtual environemt, e.g., using `conda activate base`, or any other environment in which you have installed your requirements including `conda install juypterlab`. The current base environment only contains some basic requirements, usually, you might want to setup a dedicated environment for yourself. Please consult the [README](https://github.com/euro-cordex/jsc-cordex?tab=readme-ov-file#conda).

Once you are logged in to jsc-cordex and set up, you can start the jupyter server (without a browser) like this:
```
jupyter lab --no-browser
```
Note the port in the URL, e.g. `http://localhost:8888` (the port can be different if several servers are running) and start an ssh tunnel with port forwarding on your local computer:
```
ssh -N -L 8000:localhost:8888 jsc-cordex
```
The jupyterlab should then be available in your local browser at `https://localhost:8000/`. The login token can also be found in the URL on the jsc-terminal. Please don't forget to kill your server once you are finished. It will be killed automatically if you close the terminal in which you started the server.
