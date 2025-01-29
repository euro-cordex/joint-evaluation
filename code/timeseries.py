"""
timeseries.py

This script processes climate model datasets to create regional mean time series and plots them.
It includes functions to open and sort datasets, compute regional means, and plot the results.
The script uses Dask for parallel computing and Seaborn for plotting.

Functions:
- create_regional_means(dsets, regions): Computes regional mean time series for given datasets and regions.
- plot(data, y, prefix="timeseries"): Plots the regional mean time series.
"""

import matplotlib.pyplot as plt
from dask.distributed import Client
import regionmask
import seaborn as sns
import numpy as np
import xarray as xr

from evaltools.source import get_source_collection, open_and_sort
from evaltools.eval import regional_mean
from evaltools.obs import eobs

time_range = slice("1979", "2020")

# dask.config.set(scheduler="single-threaded")
sns.set_theme(style="darkgrid")

variables = ["tas", "pr"]

add_eobs = True

european_countries = [
    "Albania",
    "Andorra",
    "Armenia",
    "Austria",
    "Azerbaijan",
    "Belarus",
    "Belgium",
    "Bosnia and Herzegovina",
    "Bulgaria",
    "Croatia",
    "Cyprus",
    "Czech Republic",
    "Denmark",
    "Estonia",
    "Finland",
    "France",
    "Georgia",
    "Germany",
    "Greece",
    "Hungary",
    "Iceland",
    "Ireland",
    "Italy",
    "Kazakhstan",
    "Kosovo",
    "Latvia",
    "Liechtenstein",
    "Lithuania",
    "Luxembourg",
    "Malta",
    "Moldova",
    "Monaco",
    "Montenegro",
    "Netherlands",
    "North Macedonia",
    "Norway",
    "Poland",
    "Portugal",
    "Romania",
    "Russia",
    "San Marino",
    "Serbia",
    "Slovakia",
    "Slovenia",
    "Spain",
    "Sweden",
    "Switzerland",
    "Turkey",
    "Ukraine",
    "United Kingdom",
    "Vatican City",
]

# prudence
prudence = regionmask.defined_regions.prudence

# ar6 srex
ar6 = regionmask.defined_regions.ar6.all
numbers = [16, 17, 19]
srex_europe = ar6[numbers]

# europe
countries = regionmask.defined_regions.natural_earth_v5_0_0.countries_110
numbers = [i for i, name in enumerate(countries.names) if name in european_countries]
europe = countries[numbers]


regions_dict = {
    "prudence": prudence,
    "srex-europe": srex_europe,
    "european-countries": europe,
}


def concat(dsets, concat_dim="iid"):
    concat_dim = xr.DataArray(list(dsets.keys()), dims=concat_dim, name=concat_dim)
    return xr.concat(
        dsets.values(),
        dim=concat_dim,
        coords="minimal",
        compat="override",
    )


def open_datasets(variables, frequency="mon"):
    catalog = get_source_collection(variables, frequency)
    return open_and_sort(catalog, merge=True, time_range=time_range)


def yearly_means(dsets, compute=False):
    """
    Computes yearly means for given datasets.

    Parameters:
    dsets (xarray.Dataset): The datasets to process.

    Returns:
    xarray.Dataset: Dataset containing the yearly means.
    """
    means = {dset_id: ds.groupby("time.year").mean() for dset_id, ds in dsets.items()}
    if compute is True:
        # with ProgressBar():
        means = {dset_id: mean.compute() for dset_id, mean in means.items()}
    return means


def regional_means(dsets, regions, compute=False):
    """
    Computes regional mean time series for given datasets and regions.

    Parameters:
    dsets (xarray.Dataset): The datasets to process.
    regions (dict): Dictionary of regions to compute means for.

    Returns:
    pandas.DataFrame: DataFrame containing the regional mean time series.
    """
    means = {dset_id: regional_mean(ds, regions) for dset_id, ds in dsets.items()}
    if compute is True:
        # with ProgressBar():
        means = {dset_id: mean.compute() for dset_id, mean in means.items()}

    return means


def plot(data, y, prefix="timeseries"):
    """
    Plots the regional mean time series.

    Parameters:
    data (pandas.DataFrame): DataFrame containing the regional mean time series.
    y (str): The variable to plot.
    prefix (str): Prefix for the plot filename.

    Returns:
    None
    """
    nregions = data.region.nunique()
    col_wrap = 4
    if nregions > 4:
        col_wrap = int(nregions / np.sqrt(nregions).astype(int))
    ax = sns.relplot(
        data=data,
        x="year",
        y=y,
        hue="iid",
        col="region",
        col_wrap=col_wrap,
        kind="line",
        facet_kws=dict(sharey=True),
    )
    sns.move_legend(ax, "lower left", bbox_to_anchor=(0.2, -0.2))
    ax.savefig(f"{prefix}-{y}.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    with Client(dashboard_address=None, threads_per_worker=1) as client:
        dsets = open_datasets(variables)
        if add_eobs is True:
            dsets["eobs"] = eobs(to_cf=True)[variables].sel(time=time_range)
        ymeans = yearly_means(dsets, compute=True)
        for name, regions in regions_dict.items():
            print(f"plotting: {name}")
            means = regional_means(ymeans, regions)
            data = concat(means).to_dataframe().reset_index()
            for y in variables:
                print(f"plotting: {y}")
                plot(data, y, prefix=f"timeseries-{name}")
