import pandas as pd
import numpy as np
import sys
from icecream import ic

ic.disable()

experiment_patterns = sys.argv[1:]
experiment_query = " | ".join(
    [f'experiment.str.contains("{patt}")' for patt in experiment_patterns]
)

precision_factor = 4  # float
compression_factor = 0.6
bytes_to_TB = 1.0e-12

# Number of time records per year
frequency_factor = {"mon": 12, "day": 365, "6hr": 365 * 4, "1hr": 365 * 24}

# Number of years depending on the experiment. Minimal periods considered here
# as the evaluation can be extended beyond 2020 and the historical could start
# in 1951. See https://cordex.org/wp-content/uploads/2021/05/CORDEX-CMIP6_exp_design_RCM.pdf
experiment_factor = {"evaluation": 2020 - 1980 + 1, "historical": 2014 - 1961 + 1}
experiment_factor_default = 2100 - 2015 + 1

domains = pd.read_csv(
    "https://raw.githubusercontent.com/WCRP-CORDEX/domain-tables/refs/heads/main/CORDEX-CMIP5_rotated_grids.csv",
    usecols=["domain_id", "nlon", "nlat"],
)
domains["ngridcells"] = domains["nlon"] * domains["nlat"]
ngridcells = (
    domains.drop(columns=["nlon", "nlat"])
    .set_index("domain_id")
    .to_dict()
    .get("ngridcells")
)
# Some fixes for missing domains
ngridcells["AUS-20i"] = ngridcells["AUS-25"]  #!!
ngridcells["MENA-25"] = ngridcells["MNA-25"]
ngridcells["MED-25"] = ngridcells["MED-12"] / 4
ngridcells["SEA-12"] = ngridcells["SEA-25"] * 4

plans = (
    pd.read_csv(
        "https://raw.githubusercontent.com/WCRP-CORDEX/simulation-status/refs/heads/main/CMIP6_downscaling_plans.csv",
        usecols=["domain", "institute", "experiment", "status", "comments"],
    )
    .query('domain == "EUR-12"')
    .query('status in ["completed", "running"]')
    .query('~comments.str.contains("#ESD", na=False)')
    .query(experiment_query)
)

simulation_count = plans.pivot_table(
    index="domain", columns="experiment", aggfunc="size", fill_value=0
).drop(columns=["selected"], errors="ignore")
ic(simulation_count)

dreq = pd.read_csv(
    "https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/dreq_EUR_joint_evaluation.csv"
)

studies = set()
for priorities in dreq["priority"].dropna():
    for priority in priorities.split():
        studies.add(priority.strip())
ic(studies)
dreq["priority"] = dreq["priority"].fillna("")

print(f"Considering experiments: {experiment_patterns}")
for study in sorted(list(studies)) + ["ALL-STUDIES"]:
    dreq_study = (
        dreq.query("priority.str.contains(@study)").copy()
        if study != "ALL-STUDIES"
        else dreq.copy()
    )
    dreq_study["priority"] = study

    variable_count = dreq_study.pivot_table(
        index="frequency", columns="priority", aggfunc="size", fill_value=0
    )
    ic(variable_count)

    freq_factor = variable_count.index.map(lambda x: frequency_factor.get(x, 0))
    ic(freq_factor)
    variable_records_per_yr = variable_count.mul(freq_factor, axis=0)
    ic(variable_records_per_yr)

    nrecords_factor = variable_records_per_yr.values.sum()
    ic(nrecords_factor)

    ngridcell_factor = simulation_count.index.map(lambda x: ngridcells.get(str(x)))
    exp_factor = simulation_count.columns.map(
        lambda x: experiment_factor.get(x, experiment_factor_default)
    )
    ic(exp_factor)

    size_TB = (
        simulation_count.mul(ngridcell_factor, axis=0)
        .mul(exp_factor, axis=1)
        .mul(nrecords_factor)
        .mul(precision_factor)
        .mul(compression_factor)
        .mul(bytes_to_TB)
        .round(3)
    )
    ic(size_TB)

    ic(size_TB.T.sum())

    print(
        f"Total {study:21} study estimated data request size is: {np.nansum(size_TB.values):8.3f} TB"
    )
