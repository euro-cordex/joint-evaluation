import cdsapi

dataset = "reanalysis-cerra-single-levels"
request = {
    "variable": [
        "10m_wind_direction",
        "10m_wind_gust_since_previous_post_processing",
        "10m_wind_speed",
        "2m_relative_humidity",
        "2m_temperature",
        "albedo",
        "evaporation",
        "high_cloud_cover",
        "land_sea_mask",
        "liquid_volumetric_soil_moisture",
        "low_cloud_cover",
        "maximum_2m_temperature_since_previous_post_processing",
        "mean_sea_level_pressure",
        "medium_cloud_cover",
        "minimum_2m_temperature_since_previous_post_processing",
        "momentum_flux_at_the_surface_u_component",
        "momentum_flux_at_the_surface_v_component",
        "orography",
        "skin_temperature",
        "snow_density",
        "snow_depth",
        "snow_depth_water_equivalent",
        "snow_fall_water_equivalent",
        "soil_temperature",
        "surface_latent_heat_flux",
        "surface_net_solar_radiation",
        "surface_net_solar_radiation_clear_sky",
        "surface_net_thermal_radiation",
        "surface_net_thermal_radiation_clear_sky",
        "surface_pressure",
        "surface_roughness",
        "surface_sensible_heat_flux",
        "surface_solar_radiation_downwards",
        "surface_thermal_radiation_downwards",
        "time_integrated_surface_direct_short_wave_radiation_flux",
        "total_cloud_cover",
        "total_column_integrated_water_vapour",
        "total_precipitation",
        "volumetric_soil_moisture",
    ],
    "year": [
        "1984",
        "1985",
        "1986",
        "1987",
        "1988",
        "1989",
        "1990",
        "1991",
        "1992",
        "1993",
        "1994",
        "1995",
        "1996",
        "1997",
        "1998",
        "1999",
        "2000",
        "2001",
        "2002",
        "2003",
        "2004",
        "2005",
        "2006",
        "2007",
        "2008",
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
    ],
    "month": ["01", "02", "03", "04", "05", "06", "07", "08", "09", "10", "11", "12"],
    "data_format": "grib",
}

target = "/mnt/CORDEX_CMIP6_tmp/aux_data/cerra/reanalysis-cerra-single-levels.grb"
client = cdsapi.Client()
client.retrieve(dataset, request, target).download()
