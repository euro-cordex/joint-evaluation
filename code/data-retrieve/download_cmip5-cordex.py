from pyesgf.search import SearchConnection
import glob
import os
import subprocess
import numpy as np
import pandas as pd
from itertools import product


def search_dic(var, freq, exp="evaluation"):
    "Dictionary with constant facets for CMIP5-CORDEX-EUR-11 evaluation"
    return dict(
        project="CORDEX",
        product="output",
        experiment=exp,
        driving_model="ECMWF-ERAINT",
        domain="EUR-11",
        time_frequency=[
            freq,
        ],
        latest="true",
        variable=[
            var,
        ],
        facets="dataset_id",
    )


def get_urls(vardic, nodeURL, ires=0):
    "Get urls for opendap"
    conn = SearchConnection(nodeURL, distrib=True)
    ctx = conn.new_context(**vardic)
    results = ctx.search(batch_size=200)
    # dids = [result.dataset_id for result in results]
    # print(dids)
    files = results[ires].file_context().search()
    return [file.opendap_url for file in files]


def get_id(vardic, nodeURL):
    "Get dataset_id"
    conn = SearchConnection(nodeURL, distrib=True)
    ctx = conn.new_context(**vardic)
    results = ctx.search(batch_size=200)
    return [result.dataset_id for result in results]


def get_ds(vardic, nodeURL):
    "Get object for download"
    conn = SearchConnection(nodeURL, distrib=True)
    ctx = conn.new_context(**vardic)
    return ctx.search(batch_size=200)[0]


def df_2_dict(dataset_id):
    "Convert dataset_id into a dictionary for searching on the ESGF"
    facets_to_exclude = ["driving_model", "version", "data_node"]
    df_filtered = dataset_id.drop(labels=facets_to_exclude)
    df_dict = df_filtered.to_dict()
    df_dict["latest"] = "true"
    df_dict["facets"] = "dataset_id"
    return df_dict


def datasetid_2_dataframe(dataset_id):
    "Convert dataset_id to dataframe"
    columns = [
        "project",
        "product",
        "domain",
        "institute",
        "driving_model",
        "experiment",
        "ensemble",
        "rcm_name",
        "rcm_version",
        "time_frequency",
        "variable",
        "version",
        "data_node",
    ]

    rows = [item.split("|")[0].split(".") + [item.split("|")[1]] for item in dataset_id]
    return pd.DataFrame(rows, columns=columns)


def download_datasetid_ESGF(dataset_id, nodeURL, dest):
    "Function to fownload a specific dataset_id from ESGF with its DRS"
    id_dic = df_2_dict(dataset_id)
    sim_search = ".".join(str(value) for value in id_dic.values())
    print(sim_search)
    # urls = get_urls(id_dic, nodeURL, ires=0)
    ds = get_ds(id_dic, nodeURL)

    # create DRS
    target_dir = (
        f"{dest}/"
        f"{dataset_id['project']}/"
        f"{dataset_id['product']}/"
        f"{dataset_id['domain']}/"
        f"{dataset_id['institute']}/"
        f"{dataset_id['driving_model']}/"
        f"{dataset_id['experiment']}/"
        f"{dataset_id['ensemble']}/"
        f"{dataset_id['rcm_name']}/"
        f"{dataset_id['rcm_version']}/"
        f"{dataset_id['time_frequency']}/"
        f"{dataset_id['variable']}/"
        f"{dataset_id['version']}"
    )

    os.makedirs(target_dir, exist_ok=True)

    # download if not nc files available in the directory
    nc_files = glob.glob(os.path.join(target_dir, "*.nc"))
    if not nc_files:

        # create wget
        fc = ds.file_context()
        wget_script_content = fc.get_download_script()
        script_path = f"{target_dir}/{sim_search}.sh"
        with open(script_path, "w") as writer:
            writer.write(wget_script_content)

        # ejecute wget
        os.chmod(script_path, 0o750)
        # download_dir = os.path.dirname(script_path)
        subprocess.check_output(
            ["bash", "{}".format(script_path), "-s"], cwd=target_dir
        )
        os.remove(script_path)


def main():

    os.environ["ESGF_PYCLIENT_NO_FACETS_STAR_WARNING"] = "1"

    dest = "/mnt/CORDEX_CMIP6_tmp/aux_data/cordex-cmip5/"
    nodeURL = "http://esgf-data.dkrz.de/esg-search"

    dreq = pd.read_csv(
        "https://raw.githubusercontent.com/euro-cordex/joint-evaluation/refs/heads/main/dreq_EUR_joint_evaluation.csv"
    )
    variables = dreq[dreq["priority"].str.contains("overview", case=False, na=False)][
        "out_name"
    ].values.tolist()
    frequencies = dreq[dreq["priority"].str.contains("overview", case=False, na=False)][
        "frequency"
    ].values.tolist()
    # add day in case there is no data available at monthly resolution
    frequencies = list(np.unique(frequencies + ["day"]))

    # search datasets on the ESGF
    CMIP5_CORDEX_dict = search_dic(variables, frequencies)
    datasets_id = get_id(CMIP5_CORDEX_dict, nodeURL)

    # convert datasets_id 2 dataframe
    df_id = datasetid_2_dataframe(datasets_id)

    # RCMs found
    df_id_mon_day = df_id[df_id["time_frequency"].isin(["mon", "day"])]
    facets_used = ["institute", "rcm_name", "rcm_version"]
    df_rcms = df_id_mon_day[facets_used].drop_duplicates().reset_index(drop=True)
    df_rcms.to_csv("RCMs.csv")

    # delete datasets with day resolution if mon resolution is available
    # (version and data_node facets are ommited)
    df_id_down = pd.DataFrame()
    for var, freq in product(variables, frequencies):
        # Get rcm_list for the current variable and frequency
        rcm_list = df_id.query("variable == @var & time_frequency == @freq")
        if freq == "mon":
            rcm_list_day = df_id.query('variable == @var & time_frequency == "day"')
            # List of columns that you do NOT want to compare
            facets_excluded = ["time_frequency", "version", "data_node"]
            # Compare and get extra rows in day resolution
            extra = rcm_list_day[
                ~rcm_list_day.drop(columns=facets_excluded)
                .apply(tuple, axis=1)
                .isin(rcm_list.drop(columns=facets_excluded).apply(tuple, axis=1))
            ]
            # Concatenate results
            df_id_down = pd.concat([df_id_down, rcm_list, extra], ignore_index=True)
        elif freq == "fx":
            df_id_down = pd.concat([df_id_down, rcm_list], ignore_index=True)
    # Reset the index and print the results
    df_id_down = df_id_down.reset_index(drop=True)
    df_aux = df_id_down.query("time_frequency == 'day'")
    df_aux.to_csv("no_monthly_data.csv")

    # Variables not found
    rcms_var_not_found = []
    for indx, row in df_rcms.iterrows():
        query_str = " & ".join([f'{col} == "{val}"' for col, val in row.items()])
        result = df_id_down.query(query_str)
        var_not_found = [
            var for var in variables if var not in result["variable"].values
        ]
        for var in var_not_found:
            new_row = row.copy()
            new_row["variable"] = var
            rcms_var_not_found.append(new_row)

    pd.DataFrame(rcms_var_not_found).to_csv("variables_not_found.csv")

    # download
    df_id_down = df_id_down.replace("cordex", "CORDEX")
    for indx, dataset_id in df_id_down.iterrows():
        download_datasetid_ESGF(dataset_id, nodeURL, dest)


if __name__ == "__main__":
    main()
