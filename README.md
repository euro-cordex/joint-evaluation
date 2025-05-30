<img src="https://mirrors.creativecommons.org/presskit/logos/cc.logo.large.png" width="150" align="right"/>

# Joint evaluation

<img src="https://imgs.xkcd.com/comics/data_trap.png" width="300" align="right"/>

Joint evaluation of the CMIP6 downscaling within EURO-CORDEX.

This repository is supposed to help in organizing the EURO-CORDEX joint evaluation, e.g., it holds meta data like tables, and catalogs, but also README files and [issues](https://github.com/euro-cordex/joint-evaluation/issues) regarding evaluation. If you have any topics, questions or suggestions, please don't hesitate to [open a new issue](https://github.com/euro-cordex/joint-evaluation/issues/new)! There is also the possibility to [join a team](https://github.com/orgs/euro-cordex/teams) to stay in touch easily if you want.

For a broader overview, you can also have a look at the [project board](https://github.com/orgs/euro-cordex/projects/5).

There is also a repository ([evaltools](https://github.com/euro-cordex/evaltools)) that might hold useful code used in the evaluation analysis. This is separated for better maintainability.

[xkcd](https://xkcd.com) comic used under [CC BY-NC 2.5 license](https://xkcd.com/license.html)

## How to contribute an analysis

You are very welcome and encouraged to contribute your own analysis to the overall joint evaluation effort. We recommend to [create an issue](https://github.com/euro-cordex/joint-evaluation/issues/new) to let us know about your analysis and find synergies with other topics (maybe reuse code from other topics?).
Right now, most evaluation topics have their own dedicated repository under the EURO-CORDEX github organization, e.g., the [Fire Weather Index Evaluation](https://github.com/euro-cordex/fwi-evaluation) and the [Water Budget Analysis](https://github.com/euro-cordex/water-budget-evaluation).
If you need a repository for your own topic or already have one and would like to move it here, just let us know! You can also have a look at our common code base ([evaltools](https://github.com/euro-cordex/evaltools)) which can help you accessing, loading and fixing CORDEX-CMIP6 datasets.

If you have never contributed on github before, you find also some helpful insight [here](https://book.the-turing-way.org/reproducible-research/vcs/vcs-github.html#contributing-to-other-projects).

## Data exchange
Some storage space on the existing jsc-cordex data exchange infrastructure at Jülich Supercomputing Centre (JSC) is available and may be used to provide some intermediate, temporary, limited storage for the first joint analyses publications of CORDEX-CMIP6 simulations before data will be stored longer-term on ESGF-related storage at the respective centres.

Access to storate at JSC is decribed [here](https://github.com/euro-cordex/jsc-cordex).

The variables requested for this joint evaluation, along their corresponding metadata, are available in this repository, under the file [dreq_EUR_joint_evaluation.csv](https://github.com/euro-cordex/joint-evaluation/blob/main/dreq_EUR_joint_evaluation.csv). This file can be explored using the search box in Github; e.g. search for "Overview" to filter the variables currently requested for the overview evaluation work. This file can also be downloaded [here](https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/dreq_EUR_joint_evaluation.csv), to use it in your data uploading scripts.

```python
import pandas as pd

pd.read_csv("https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/dreq_EUR_joint_evaluation.csv")
```
```
   out_name frequency  units                       long_name        standard_name            cell_methods   priority comment
0       clt       mon      %    Total Cloud Cover Percentage  cloud_area_fraction        area: time: mean     Trends     NaN
1      hurs       1hr      %  Near-Surface Relative Humidity    relative_humidity  area: mean time: point        FWI     NaN
2      hurs       day      %  Near-Surface Relative Humidity    relative_humidity        area: time: mean        FWI     NaN
3   hus1000       6hr      1               Specific Humidity    specific_humidity  area: mean time: point  AtmRivers     NaN
4    hus200       6hr      1               Specific Humidity    specific_humidity  area: mean time: point  AtmRivers     NaN
..      ...       ...    ...                             ...                  ...                     ...        ...     ...
63    va700       6hr  m s-1                  Northward Wind       northward_wind  area: mean time: point  AtmRivers     NaN
64    va850       6hr  m s-1                  Northward Wind       northward_wind  area: mean time: point  AtmRivers     NaN
65    va925       6hr  m s-1                  Northward Wind       northward_wind  area: mean time: point  AtmRivers     NaN
66      vas       mon  m s-1     Northward Near-Surface Wind       northward_wind        area: time: mean     Trends     NaN
67    zg500       mon      m             Geopotential Height  geopotential_height        area: time: mean     Trends     NaN
```

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
You can filter and load datasets using, e.g.,:
```python
dataset_dict = cat.search(variable_id=["tas", "orog", "sftlf"]).to_dataset_dict()
```
and you will get a dictionary of datasets back. Note that this, for now, will only work when you are logged in to `jsc-cordex` since datasets reside on the local filesystem. For more info about `intake-esm`, please also see the [documentation](https://intake-esm.readthedocs.io/en/stable/how-to/understand-keys-and-how-to-change-them.html).

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

### Port forwarding for Dask dashbaord

If you are working with Juypter notebooks in other environments, (e.g., VS Code), you probably have to explicitly set the dashboard adress, e.g.,
```
client = Client(dashboard_address="localhost:8787", threads_per_worker=1)
```
You can then forward the port to the localhost on your laptop and see the dashboard.d

## Plots

These plots show some preliminary results from available data at `jsc-cordex` and show regional and yearly means in the PRUDENCE regions. These plots give an overview of what data is available and are [updated](https://github.com/euro-cordex/joint-evaluation/blob/main/code/timeseries.py) once new data comes in.

<img src="https://raw.githubusercontent.com/euro-cordex/joint-evaluation/main/plots/timeseries-prudence-tas.png">
