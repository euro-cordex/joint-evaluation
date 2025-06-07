import cordex as cx
import cf_xarray as cfxr
import xarray as xr
import xesmf as xe
from warnings import warn
import numpy as np
import os
from evaltools.source import get_source_collection, open_and_sort
import cmocean

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
        "levels": [-5, -4, -3, -2, -1, -0.5, 0.5, 1, 2, 3, 4, 5],
        "cmap": "RdBu_r",
        "units": "K",
        "datasets": ["era5", "cerra"],
    },
    "pr": {
        "variable": "pr",
        "name": "Precipitation BIAS [%]",
        "diff": "rel",
        "aggr": "mean",
        "levels": [
            -100,
            -90,
            -80,
            -70,
            -60,
            -50,
            -40,
            -30,
            -20,
            -10,
            10,
            20,
            30,
            40,
            50,
            60,
            70,
            80,
            90,
            100,
        ],
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

e_obs_dic = {
    "tas": {"cmap": cmocean.cm.thermal, "levels": np.arange(264, 303, 3), "units": "K"},
    "pr": {
        "cmap": cmocean.cm.rain,
        "levels": [0, 30, 60, 90, 120, 150, 210, 270, 330, 360, 420, 480],
        "units": "mm",
    },
}

variable_mapping = {
    "cerra": {"tas": "t2m", "pr": "tp"},
    "cerra-land": {"tas": "tas", "pr": "tp"},
    "era5": {"tas": "t2m", "pr": "tp"},
}

# List of models that need regridding although they are on rotated pole
force_regrid = ["WRF541Q", "RegCM5-0"]


def load_obs(variable, dataset, add_fx=True, mask=True):
    root = f"/mnt/CORDEX_CMIP6_tmp/aux_data/{dataset}/mon/{variable}/"
    ds = xr.open_mfdataset(
        np.sort(list(traverseDir(root))), concat_dim="valid_time", combine="nested"
    )
    ds = ds.rename({"valid_time": "time"})
    ds = fix_360_longitudes(ds, lonname="longitude")

    if add_fx is True:
        files_fx = list(traverseDir(f"/mnt/CORDEX_CMIP6_tmp/aux_data/{dataset}/fx/"))
        files_fx = [f for f in files_fx if "fixed" in f]
        for fx in ["orog", "sftlf", "areacella"]:
            file_fx = [f for f in files_fx if fx in f]
            if file_fx:
                ds_fx = xr.open_dataset(file_fx[0])
                ds_fx = fix_360_longitudes(ds_fx, lonname="longitude")
                ds_fx, ds = xr.align(ds_fx, ds, join="inner")
                print(f"merging {dataset} with {fx}")
                ds[fx] = ds_fx[fx]
        if mask is True:
            sftlf = ds["sftlf"]
            ds["mask"] = sftlf > 0
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
        add_fx = ["orog", "sftlf", "areacella", "sfturf"]
    cat = get_source_collection(variables, frequency, add_fx=add_fx, **kwargs)
    dsets = open_and_sort(cat, merge_fx=merge_fx, apply_fixes=apply_fixes)
    if rewrite_grid is True:
        for dset_id, ds in dsets.items():
            print("rewriting coordinates for", dset_id)
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


def regional_mean(ds, regions=None, weights=None, aggr=None):
    """
    Compute the regional mean of a dataset over specified regions.

    Parameters:
    ds (xarray.Dataset): The dataset to compute the regional mean for.
    regions (regionmask.Regions): The regions to compute the mean over.

    Returns:
    xarray.Dataset: The regional mean values.
    """
    mask = 1.0
    if "lon" in ds.coords:
        x = "lon"
        y = "lat"
    elif "longitude" in ds.coords:
        x = "longitude"
        y = "latitude"
    if weights is None:
        weights = xr.ones_like(ds[x])
    if regions:
        mask = regions.mask_3D(ds[x], ds[y], drop=False)
    if aggr == "mean":
        result = ds.cf.weighted(mask * weights).mean(dim=("X", "Y"), skipna=True)
    elif aggr == "P95":
        ds = np.abs(ds)
        ds = ds.where(mask)
        result = ds.cf.quantile(0.95, dim=["X", "Y"], skipna=True)

    return result


def regional_means(dsets, regions=None, aggr=None):
    """
    Compute the regional means for multiple datasets over specified regions.

    Parameters:
    dsets (dict): A dictionary of datasets to compute the regional means for.
    regions (regionmask.Regions): The regions to compute the means over.

    Returns:
    xarray.Dataset: The concatenated regional mean values for all datasets.
    """
    concat_dim = xr.DataArray(list(dsets.keys()), dims="iid", name="iid")
    return xr.concat(
        [regional_mean(ds, regions, None, aggr) for ds in dsets.values()],
        dim=concat_dim,
        coords="minimal",
        compat="override",
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
    return np.all(np.isin(expected_years, years_in_ds))


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


# def select_period(ds: xr.DataArray) -> xr.DataArray:
#    """
#    Add a new dimension 'period' based on the time coordinate.
#
#    Parameters:
#    - data: xarray DataArray with a 'time' dimension
#    - period: dictionary mapping periods
#
#    Returns:
#    - DataArray with a new 'period' dimension added
#    """
#
#    # Create a list of seasons based on the 'time' coordinate
#    period_dim = {}
#    for period in periods.items():
#        mask = ds["time"].dt.month.isin(months)
#        season_dim[season] = ds.where(mask, np.nan)
#
#    season_da = xr.concat(
#        list(season_dim.values()),
#        dim="season",
#        coords="minimal",
#        compat="override",
#    )
#
#    season_da.coords["season"] = ["winter", "spring", "summer", "fall"]
#
#    return season_da


def select_season(ds: xr.DataArray) -> xr.DataArray:
    """
    Add a new dimension 'season' based on the month of the time coordinate.

    Parameters:
    - data: xarray DataArray with a 'time' dimension
    - season_months: dictionary mapping season names to lists of months (1-12)

    Returns:
    - DataArray with a new 'season' dimension added
    """

    season_months = {
        "winter": [12, 1, 2],
        "spring": [3, 4, 5],
        "summer": [6, 7, 8],
        "fall": [9, 10, 11],
    }

    # Create a list of seasons based on the 'time' coordinate
    season_dim = {}
    for season, months in season_months.items():
        mask = ds["time"].dt.month.isin(months)
        season_dim[season] = ds.where(mask, np.nan)

    season_da = xr.concat(
        list(season_dim.values()),
        dim="season",
        coords="minimal",
        compat="override",
    )

    season_da.coords["season"] = ["winter", "spring", "summer", "fall"]

    return season_da


class TaylorDiagram(object):
    """Taylor diagram.

    Plot model standard deviation and correlation to reference (data)
    sample in a single-quadrant polar plot, with r=stddev and
    theta=arccos(correlation).

    __version__ = "Time-stamp: <2012-02-21 15:52:15 ycopin>"
    __author__ = "Yannick Copin <yannick.copin@...547...>"
    Taylor diagram (Taylor, 2001) implementation.
    Source: http://www-pcmdi.llnl.gov/about/staff/Taylor/CV/Taylor_diagram_primer.htm

    """

    def __init__(
        self,
        refstd,
        fig=None,
        rect=111,
        label="_",
        srange=(0, 2.5),
        rlocs=np.concatenate(
            [[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9], [0.95, 0.99]]
        ),
    ):
        """Set up Taylor diagram axes, i.e. single quadrant polar
        plot, using `mpl_toolkits.axisartist.floating_axes`.

        Parameters:

        * refstd: reference standard deviation to be compared to
        * fig: input Figure or None
        * rect: subplot definition
        * label: reference label
        * srange: stddev axis extension, in units of *refstd*
        * rlocs: correlation values
        """

        from matplotlib.projections import PolarAxes
        import mpl_toolkits.axisartist.floating_axes as FA
        import mpl_toolkits.axisartist.grid_finder as GF

        self.refstd = refstd  # Reference standard deviation
        self.rlocs = rlocs

        tr = PolarAxes.PolarTransform()

        # Correlation labels
        tlocs = np.arccos(rlocs)  # Conversion to polar angles
        gl1 = GF.FixedLocator(tlocs)  # Positions
        tf1 = GF.DictFormatter(dict(zip(tlocs, map(str, rlocs))))

        # Standard deviation axis extent (in units of reference stddev)
        self.smin = srange[0] * self.refstd
        self.smax = srange[1] * self.refstd

        ghelper = FA.GridHelperCurveLinear(
            tr,
            extremes=(0, np.pi / 2, self.smin, self.smax),  # 1st quadrant
            grid_locator1=gl1,
            tick_formatter1=tf1,
        )

        if fig is None:
            fig = plt.figure()  # noqa

        ax = FA.FloatingSubplot(fig, rect, grid_helper=ghelper)
        fig.add_subplot(ax)

        # Adjust axes
        ax.axis["top"].set_axis_direction("bottom")  # "Angle axis"
        ax.axis["top"].toggle(ticklabels=True, label=True)
        ax.axis["top"].major_ticklabels.set_axis_direction("top")
        ax.axis["top"].label.set_axis_direction("top")
        ax.axis["top"].label.set_text("Temporal Correlation")

        ax.axis["left"].set_axis_direction("bottom")  # "X axis"
        ax.axis["left"].label.set_text("Normalized Standard Deviation")

        ax.axis["right"].set_axis_direction("top")  # "Y axis"
        ax.axis["right"].toggle(ticklabels=True)
        ax.axis["right"].major_ticklabels.set_axis_direction("left")

        ax.axis["bottom"].set_visible(False)  # Useless

        self._ax = ax  # Graphical axes
        self.ax = ax.get_aux_axes(tr)  # Polar coordinates

        # Add reference point and stddev contour
        plot_reference = False
        if plot_reference is True:
            (lx,) = self.ax.plot([0], self.refstd, "k*", ls="", ms=10, label=label)
            # Collect sample points for latter use (e.g. legend)
            self.samplePoints = [lx]

        else:
            self.samplePoints = []

        t = np.linspace(0, np.pi / 2)
        r = np.zeros_like(t) + self.refstd
        self.ax.plot(t, r, "k--", label="_")

        for v in [0.5, 1, 1.5, 2]:
            t = np.linspace(0, np.pi / 2)
            r = np.zeros_like(t) + v
            self.ax.plot(t, r, "k--", linewidth=0.5)

    def add_sample(self, stddev, corrcoef, *args, **kwargs):
        """Add sample (*stddev*,*corrcoeff*) to the Taylor
        diagram. *args* and *kwargs* are directly propagated to the
        `Figure.plot` command."""

        (la,) = self.ax.plot(
            np.arccos(corrcoef), stddev, *args, **kwargs
        )  # (theta,radius)
        # l, = self.ax.scatter(np.arccos(corrcoef), stddev,
        #                  *args, **kwargs) # (theta,radius)

        self.samplePoints.append(la)

        return la

    def add_grid(self, *args, **kwargs):
        """Add a grid."""

        self.ax.grid(*args, **kwargs)

    def add_correlation_lines(self, **kwargs):
        """Draw radial lines indicating correlation values."""
        rlocs = [0.2, 0.4, 0.6, 0.8, 0.9]
        for r in rlocs:
            theta = np.arccos(r)
            self.ax.plot(
                [theta, theta],
                [self.smin, self.smax],
                color="gray",
                linestyle="--",
                linewidth=0.4,
                **kwargs,
            )

    def add_contours(self, levels=5, **kwargs):
        """Add constant centered RMS difference contours, defined by
        *levels*."""

        rs, ts = np.meshgrid(
            np.linspace(self.smin, self.smax), np.linspace(0, np.pi / 2)
        )
        # Compute centered RMS difference
        rms = np.sqrt(self.refstd**2 + rs**2 - 2 * self.refstd * rs * np.cos(ts))

        contours = self.ax.contour(ts, rs, rms, levels, **kwargs)

        return contours
