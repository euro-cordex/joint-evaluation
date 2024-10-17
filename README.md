# Joint evaluation
Joint evaluation of the CMIP6 downscaling within EURO-CORDEX.


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
