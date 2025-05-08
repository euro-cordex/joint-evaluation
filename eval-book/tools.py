import cordex as cx
import cf_xarray as cfxr
import xarray as xr
import xesmf as xe
from warnings import warn
import numpy as np
import os
from evaltools.source import get_source_collection, open_and_sort


default_attrs_ = [
    "project_id",
    "domain_id",
    "institution_id",
    "driving_source_id",
    "driving_experiment_id",
    "driving_variant_label",
    "source_id",
    "version_realization",
    "frequency",
    "variable_id",
    "version",
]

var_dic = {
    "tas": {
        "variable": "tas",
        "name": "Temperature BIAS [K]",
        "diff": "abs",
        "aggr": "mean",
        "levels": np.arange(-5, 6, 1),
        "cmap": "RdBu_r",
        "units": "C",
        "datasets": ["era5", "cerra"],
    },
    "pr": {
        "variable": "pr",
        "name": "Precipitation BIAS [%]",
        "diff": "rel",
        "aggr": "mean",
        "levels": np.arange(-100, 110, 10),
        "cmap": "BrBG",
        "units": "%",
        "datasets": ["era5", "cerra-land"],
    },
    "psl": {
        "variable": "psl",
        "name": "Sea-level pressure BIAS [hp]",
        "diff": "rel",
        "aggr": "mean",
        "levels": np.arange(-6, 7, 1),
        "cmap": "RdBu_r",
        "units": "hpa",
        "datasets": ["era5", "cerra"],
    },
}

variable_mapping = {
    "cerra": {"tas": "t2m", "pr": "tp"},
    "cerra-land": {"tas": "tas", "pr": "tp"},
    "era5": {"tas": "t2m", "pr": "tp"},
}


def load_obs(variable, dataset, add_mask=False):
    root = f"/mnt/CORDEX_CMIP6_tmp/aux_data/{dataset}/mon/{variable}/"
    ds = xr.open_mfdataset(
        np.sort(list(traverseDir(root))), concat_dim="valid_time", combine="nested"
    )
    ds = ds.rename({"valid_time": "time"})
    return ds


def add_bounds(ds):
    if "longitude" not in ds.cf.bounds and "latitude" not in ds.cf.bounds:
        ds = cx.transform_bounds(ds, trg_dims=("vertices_lon", "vertices_lat"))
    lon_bounds = ds.cf.get_bounds("longitude")
    lat_bounds = ds.cf.get_bounds("latitude")
    bounds_dim = [dim for dim in lon_bounds.dims if dim not in ds.indexes][0]
    # reshape bounds for xesmf
    ds = ds.assign_coords(
        lon_b=cfxr.bounds_to_vertices(
            lon_bounds, bounds_dim=bounds_dim, order="counterclockwise"
        ),
        lat_b=cfxr.bounds_to_vertices(
            lat_bounds, bounds_dim=bounds_dim, order="counterclockwise"
        ),
    )
    return ds


def mask_with_sftlf(ds, sftlf=None):
    if sftlf is None and "sftlf" in ds:
        sftlf = ds["sftlf"]
        for var in ds.data_vars:
            if var != "sftlf":
                ds[var] = ds[var].where(sftlf > 0)
        ds["mask"] = sftlf > 0
    else:
        warn(f"sftlf not found in dataset: {ds.source_id}")
    return ds


def open_datasets(
    variables,
    frequency="mon",
    mask=True,
    add_fx=None,
    merge_fx=True,
    add_missing_bounds=True,
    rewrite_grid=True,
    apply_fixes=True,
    **kwargs,
):
    if merge_fx is True and add_fx is None:
        add_fx = ["orog", "sftlf", "areacella"]
    cat = get_source_collection(variables, frequency, add_fx=add_fx, **kwargs)
    dsets = open_and_sort(cat, merge_fx=merge_fx, apply_fixes=apply_fixes)
    if rewrite_grid is True:
        for dset_id, ds in dsets.items():
            dsets[dset_id] = rewrite_coords(ds)
    if mask is True:
        for ds in dsets.values():
            mask_with_sftlf(ds)
    if add_missing_bounds is True:
        for dset_id, ds in dsets.items():
            dsets[dset_id] = add_bounds(ds)
    return dsets


def create_cordex_grid(domain_id):
    grid = cx.domain(domain_id, bounds=True, mip_era="CMIP6")
    # grid["lon"].attrs = {}
    # grid["vertices_lat"].attrs = {}
    lon_b = cfxr.bounds_to_vertices(
        grid.vertices_lon, bounds_dim="vertices", order="counterclockwise"
    )
    lat_b = cfxr.bounds_to_vertices(
        grid.vertices_lat, bounds_dim="vertices", order="counterclockwise"
    )
    return grid.assign_coords(lon_b=lon_b, lat_b=lat_b)


def create_regridder(source, target, method="bilinear"):
    regridder = xe.Regridder(source, target, method=method)
    return regridder


def regrid(ds, regridder, mask_after_regrid=True):
    ds_regrid = regridder(ds)
    if mask_after_regrid:
        for var in ds.data_vars:
            if var not in ["orog", "sftlf", "areacella"]:
                ds_regrid[var] = ds_regrid[var].where(ds_regrid["mask"] > 0.0)
    return ds_regrid


def regrid_dsets(dsets, target_grid, method="bilinear"):
    for dset_id, ds in dsets.items():
        try:
            mapping = ds.cf["grid_mapping"].grid_mapping_name
        except KeyError as e:
            warn(f"KeyError: {e} for {dset_id}")
            mapping = "rotated_latitude_longitude"
        if mapping != "rotated_latitude_longitude":
            print(f"regridding {dset_id} with grid_mapping: {mapping}")
            regridder = create_regridder(ds, target_grid, method=method)
            print(regridder)
            dsets[dset_id] = regrid(ds, regridder)
    return dsets


def mask_invalid(ds, vars=None, threshold=0.1):
    if isinstance(vars, str):
        vars = [vars]
    if vars is None:
        var = list(ds.data_vars)
    for var in vars:
        var_nan = ds[var].isnull().sum(dim="time") / ds.time.size
        ds[var] = ds[var].where(var_nan < threshold)
    return ds


def rewrite_coords(ds, coords="all"):
    if ds.cf["grid_mapping"].grid_mapping_name == "rotated_latitude_longitude":
        ds = ds.cx.rewrite_coords(coords=coords)
    return ds


def height_temperature_correction(model_elev, obs_elev):
    """
    Height correction for temperature
    """
    lapse_rate = 0.0065  # °C per meter
    # Apply correction (adjust model temp to obs elevation)
    return lapse_rate * (obs_elev - model_elev)


def seasonal_mean(da):
    """Optimized function to calculate seasonal averages from time series of monthly means

    based on: https://xarray.pydata.org/en/stable/examples/monthly-means.html
    """
    # Get number od days for each month
    month_length = da.time.dt.days_in_month
    # Calculate the weights by grouping by 'time.season'.
    weights = (
        month_length.groupby("time.season") / month_length.groupby("time.season").sum()
    )

    # Test that the sum of the weights for each season is 1.0
    # np.testing.assert_allclose(weights.groupby("time.season").sum().values, np.ones(4))

    # Calculate the weighted average
    return (
        (da * weights).groupby("time.season").sum(dim="time", skipna=True, min_count=1)
    )


def standardize_unit(ds, variable):
    if variable == "tas":
        ds = convert_celsius_to_kelvin(ds)
    elif variable == "pr":
        ds = convert_precipitation_to_mm(ds)
    return ds


def convert_celsius_to_kelvin(ds, threshold=200):
    """
    Converts all temperature variables in an xarray Dataset from degrees Celsius to Kelvin
    based on the 'units' attribute, value magnitude, or 'standard_name' attribute.

    Parameters:
        ds (xarray.Dataset): The input dataset.
        threshold (float): A heuristic threshold (default=200) to assume temperatures
                           below this value might be in Celsius.

    Returns:
        xarray.Dataset: A new dataset with converted temperature values.
    """
    ds = ds.copy()  # Avoid modifying the original dataset

    for var in ds.data_vars:
        units = ds[var].attrs.get("units", "").lower()
        standard_name = ds[var].attrs.get("standard_name", "").lower()

        # Check if units explicitly indicate Celsius
        if units in ["c", "°c", "celsius", "degc"]:
            ds[var] = ds[var] + 273.15
            ds[var].attrs["units"] = "K"
            print("Convert celsius to kelvin")

        # If no unit attribute exists, check standard_name for temperature-related terms
        elif standard_name in [
            "air_temperature",
            "sea_surface_temperature",
            "surface_temperature",
        ]:
            data_vals = ds[var].values
            if np.nanmax(data_vals) < threshold:  # Likely in °C
                ds[var] = ds[var] + 273.15
                ds[var].attrs["units"] = "K"
                print("Convert celsius to kelvin")

    return ds


def convert_precipitation_to_mm(ds):
    """
    Converts all precipitation variables in an xarray Dataset to millimeters (mm)
    based on the 'units' attribute or 'standard_name' attribute.

    Parameters:
        ds (xarray.Dataset): The input dataset.

    Returns:
        xarray.Dataset: A new dataset with converted precipitation values.
    """
    ds = ds.copy()  # Avoid modifying the original dataset

    for var in ds.data_vars:
        units = ds[var].attrs.get("units", "").lower()
        # standard_name = ds[var].attrs.get("standard_name", "").lower()

        # Check if units explicitly indicate meters (m) or kilograms per meter per second squared (kg/m/s²)
        if units in ["m", "meters"]:
            ds[var] = ds[var] * 1000  # Convert from meters to millimeters
            ds[var].attrs["units"] = "mm"
            print("Convert precipitation from meters to millimeters (mm).")

        elif units in ["kg m-2 s-1", "kg/m/s2"]:
            # Precipitation rate in kg/m/s² can be converted to mm/s by multiplying by 1000
            ds[var] = ds[var] * 86400  # Convert kg/m/s² to mm/s
            ds[var].attrs["units"] = "mm"
            print("Convert precipitation from kg/m/s² to mm/s.")

    return ds


def check_equal_period(ds, period):
    years_in_ds = np.unique(ds.time.dt.year.values)
    expected_years = np.arange(int(period.start), int(period.stop) + 1)
    return np.array_equal(years_in_ds, expected_years)


def fix_360_longitudes(dataset: xr.Dataset, lonname: str = "lon") -> xr.Dataset:
    """
    Fix longitude values.

    Function to transform datasets where longitudes are in (0, 360) to (-180, 180).

    Parameters
    ----------
    dataset (xarray.Dataset): data stored by dimensions
    lonname (str): name of the longitude dimension

    Returns
    -------
    dataset (xarray.Dataset): data with the new longitudes
    """
    lon = dataset[lonname]
    if lon.max().values > 180 and lon.min().values >= 0:
        dataset[lonname] = dataset[lonname].where(lon <= 180, other=lon - 360)
    return dataset


def traverseDir(root):
    for dirpath, dirnames, filenames in os.walk(root):
        for file in filenames:
            if file.endswith(".nc"):
                yield os.path.join(dirpath, file)
