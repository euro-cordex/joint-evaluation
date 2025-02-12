"""
catalog.py

This script processes and organizes metadata for the EURO-CORDEX joint evaluation project, encompassing both CMIP5 and CMIP6 driving models in a single catalog.
It includes functions to parse file paths, create human-readable summaries, and export data to Excel.
The script uses pandas for data manipulation and xlsxwriter for Excel file creation.

Functions:
- human_readable(df): Creates a human-readable summary of the dataset.
- create_excel(filename): Creates a human-readable Excel file from the dataset.
- update_catalog(catalog, root): Updates the catalog with metadata from the specified root directory.
"""

import os
import re
import pandas as pd
from os import path as op

root_dic = {
    "cmip5-cordex": "/mnt/CORDEX_CMIP6_tmp/aux_data/cordex-cmip5",
    "cmip6-cordex": "/mnt/CORDEX_CMIP6_tmp/sim_data/CORDEX/CMIP6",
}

CATALOG = "catalog.csv"

COLS = [
    "project_id",
    "mip_era",
    "activity_id",
    "domain_id",
    "institution_id",
    "driving_source_id",
    "driving_experiment_id",
    "driving_variant_label",
    "source_id",
    "version_realization",
    "frequency",
    "version",
    "time_range",
    "variable_id",
]


def insert_at_position(string, constant, position, string_="/"):
    parts = string.split(string_)
    if position <= len(parts):
        parts.insert(position, constant)
    else:
        parts.append(constant)
    return string_.join(parts)


def create_path_pattern(drs, sep="/"):
    attrs = drs.split(sep)
    drs = sep.join([f"(?P{attr}[^/]+)" for attr in attrs])
    # Allow for an optional root directory
    drs = r"^/?(?:[^/]+/)*" + drs
    return re.compile(drs)


def parse_filepath(filename, project):
    # pattern = create_pattern(drs)
    if project == "cmip6-cordex":
        regex = r"(?P<project_id>[^/]+)/(?P<mip_era>[^/]+)/(?P<activity_id>[^/]+)/(?P<domain_id>[^/]+)/(?P<institution_id>[^/]+)/(?P<driving_source_id>[^/]+)/(?P<driving_experiment_id>[^/]+)/(?P<driving_variant_label>[^/]+)/(?P<source_id>[^/]+)/(?P<version_realization>[^/]+)/(?P<frequency>[^/]+)/(?P<variable_id>[^/]+)/(?P<version>[^/]+)/(?P<filename>(?P<variable_id_2>[^_]+)_(?P<domain_id_2>[^_]+)_(?P<driving_source_id_2>[^_]+)_(?P<driving_experiment_id_2>[^_]+)_(?P<driving_variant_label_2>[^_]+)_(?P<institution_id_2>[^_]+)_(?P<source_id_2>[^_]+)_(?P<version_realization_2>[^_]+)_(?P<frequency_2>[^_]+)(?:_(?P<time_range>[^.]+))?\.nc)"
        regex = r"^/?(?:[^/]+/)*" + regex
    if project == "cmip5-cordex":
        regex = r"/(?P<project_id>[^/]+)/(?P<mip_era>[^/]+)/(?P<activity_id>[^/]+)/(?P<domain_id>[^/]+)/(?P<institution_id>[^/]+)/(?P<driving_source_id>[^/]+)/(?P<driving_experiment_id>[^/]+)/(?P<driving_variant_label>[^/]+)/(?P<source_id>[^/]+)/(?P<version_realization>[^/]+)/(?P<frequency>[^/]+)/(?P<variable_id>[^/]+)/(?P<version>[^/]+)/(?P<filename>(?P<variable_id_2>[^_]+)_(?P<domain_id_2>[^_]+)_(?P<driving_source_id_2>[^_]+)_(?P<driving_experiment_id_2>[^_]+)_(?P<driving_variant_label_2>[^_]+)_(?P<institution_id_2>[^_]+)_(?P<source_id_2>[^_]+)_(?P<version_realization_2>[^_]+)_(?P<frequency_2>[^_]+)(?:_(?P<time_range>[^.]+))?\.nc)"
        mip_era = "CMIP5"
        institution_id_2 = filename.split("/")[8]
        filename = re.sub(root_dic[project], "", filename)
        filename = insert_at_position(filename, mip_era, 2, string_="/")
        filename = filename.replace(f"{institution_id_2}-", f"{institution_id_2}_")
    pattern = re.compile(regex)
    match = pattern.match(filename)
    if match:
        return match.groupdict()
    else:
        print(
            f"The filepath does not match the expected pattern (will be ignored): {filename}"
        )
        return {}


def create_catalog(root, project):
    datasets = []
    # Define the regex pattern for the filename
    for root, dirs, files in os.walk(root):
        # only parse if files found
        if not files:
            continue
        for file in files:
            if ".nc" in file:
                filename = op.join(root, file)
                print(f"parsing {filename}")
                metadata = parse_filepath(filename, project)
                if metadata:
                    metadata["path"] = filename
                    datasets.append(metadata)
    return datasets


def human_readable(df):
    """
    Creates a human-readable summary of the dataset.

    Parameters:
    df (pandas.DataFrame): The input DataFrame containing the dataset.

    Returns:
    pandas.DataFrame: A DataFrame with grouped and summarized data.
    """
    cols = [item for item in COLS if item not in ["variable_id", "time_range"]]

    def to_list(x):
        return list(dict.fromkeys(list(x)))

    return df.groupby(cols)["variable_id"].apply(to_list).to_frame()  # .reset_index()


def create_excel(filename):
    """
    Creates a human-readable Excel file from the dataset.

    Parameters:
    filename (str): The path to the CSV file containing the dataset.

    Returns:
    str: The path to the created Excel file.
    """
    df = pd.read_csv(filename)
    sheets = {"jsc-cordex": human_readable(df)}

    stem, suffix = op.splitext(filename)
    xlsxfile = f"{stem}.xlsx"

    with pd.ExcelWriter(xlsxfile, engine="xlsxwriter") as writer:
        for sheet_name, sheet_df in sheets.items():
            print(sheet_name)
            sheet_df.to_excel(writer, sheet_name=sheet_name, index=True)
            worksheet = writer.sheets[sheet_name]  # pull worksheet object

            # Set the column width to the maximum width of the content
            for idx, col in enumerate(sheet_df.columns):
                max_len = (
                    max(
                        sheet_df[col].astype(str).map(len).max(),  # len of largest item
                        len(str(col)),  # len of column name/header
                    )
                    + 2
                )  # adding a little extra space
                worksheet.set_column(
                    idx + len(sheet_df.index.names),
                    idx + len(sheet_df.index.names),
                    max_len,
                )

            # Set the column width for the index levels
            for idx, level in enumerate(sheet_df.index.names):
                max_len = (
                    max(
                        sheet_df.index.get_level_values(level)
                        .astype(str)
                        .map(len)
                        .max(),  # len of largest item in index level
                        len(str(level)),  # len of index level name
                    )
                    + 2
                )  # adding a little extra space
                worksheet.set_column(idx, idx, max_len)

    return xlsxfile


def update_catalog(catalog, root, project):
    """
    Updates the catalog with metadata from the specified root directory.

    Parameters:
    catalog (str): The path to the catalog CSV file.
    root (str): The root directory to scan for metadata.

    Returns:
    pandas.DataFrame: The updated catalog DataFrame.
    """
    df = pd.DataFrame(create_catalog(root, project))[COLS + ["path"]]
    # print(f"writing catalog to {catalog}")
    # df.to_csv(catalog, index=False)
    return df


if __name__ == "__main__":
    # df = update_catalog(CATALOG, root_dic[project])
    # create_excel(CATALOG)
    df_CMIP5 = update_catalog(CATALOG, root_dic["cmip5-cordex"], "cmip5-cordex")
    df_CMIP6 = update_catalog(CATALOG, root_dic["cmip6-cordex"], "cmip6-cordex")
    df = pd.concat([df_CMIP5, df_CMIP6])
    df.to_csv(f"{CATALOG}", index=False)
    create_excel(f"{CATALOG}")
