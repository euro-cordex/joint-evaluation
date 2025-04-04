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
    "CORDEX-CMIP5": "/mnt/CORDEX_CMIP6_tmp/aux_data/cordex-cmip5",
    "CORDEX-CMIP6": "/mnt/CORDEX_CMIP6_tmp/sim_data/CORDEX-CMIP6",
}

pattern_dict = {
    "CORDEX-CMIP6": r"(?P<project_id>[^/]+)/(?P<activity_id>[^/]+)/(?P<domain_id>[^/]+)/(?P<institution_id>[^/]+)/(?P<driving_source_id>[^/]+)/(?P<driving_experiment_id>[^/]+)/(?P<driving_variant_label>[^/]+)/(?P<source_id>[^/]+)/(?P<version_realization>[^/]+)/(?P<frequency>[^/]+)/(?P<variable_id>[^/]+)/(?P<version>[^/]+)/(?P<filename>(?P<variable_id_2>[^_]+)_(?P<domain_id_2>[^_]+)_(?P<driving_source_id_2>[^_]+)_(?P<driving_experiment_id_2>[^_]+)_(?P<driving_variant_label_2>[^_]+)_(?P<institution_id_2>[^_]+)_(?P<source_id_2>[^_]+)_(?P<version_realization_2>[^_]+)_(?P<frequency_2>[^_]+)(?:_(?P<time_range>[^.]+))?\.nc)",
    "CORDEX-CMIP5": r"(?P<project_id>[^/]+)/(?P<product>[^/]+)/(?P<CORDEX_domain>[^/]+)/(?P<institute>[^/]+)/(?P<driving_institute>[^_]+)-(?P<driving_model>[^/]+)/(?P<experiment>[^/]+)/(?P<ensemble>[^/]+)/(?P<rcm_name>[^/]+)/(?P<rcm_version>[^/]+)/(?P<frequency>[^/]+)/(?P<variable>[^/]+)/(?P<version>[^/]+)/(?P<filename>(?P<variable_2>[^_]+)_(?P<CORDEX_domain_2>[^_]+)_(?P<driving_institute_2>[^_]+)-(?P<driving_model_2>[^_]+)_(?P<experiment_2>[^_]+)_(?P<ensemble_2>[^_]+)_(?P<institute_2>[^_]+)-(?P<rcm_name_2>[^_]+)_(?P<rcm_version_2>[^_]+)_(?P<frequency_2>[^_]+)(?:_(?P<time_range>[^.]+))?\.nc)",
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

attrs_mapping = {
    "CORDEX_domain": "domain_id",
    "rcm_name": "source_id",
    "rcm_version": "version_realization",
    "institute": "institution_id",
    "driving_institute": "driving_institution_id",
    "driving_model": "driving_source_id",
    "experiment": "driving_experiment_id",
    "ensemble": "driving_variant_label",
    "variable": "variable_id",
    "product": "activity_id",
}


def check_consistency(attrs):
    inconsistent = []
    check_keys = [k for k in attrs.keys() if k.endswith("_2")]
    for k in check_keys:
        if attrs[k] != attrs[k[:-2]]:
            print(f"Warning: {k} != {k[:-2]}")
            inconsistent.append(k)
    return inconsistent


def insert_at_position(string, constant, position, string_="/"):
    parts = string.split(string_)
    if position <= len(parts):
        parts.insert(position, constant)
    else:
        parts.append(constant)
    return string_.join(parts)


def translate_attributes(attrs):
    pass


def create_path_pattern(drs, sep="/"):
    attrs = drs.split(sep)
    drs = sep.join([f"(?P{attr}[^/]+)" for attr in attrs])
    # Allow for an optional root directory
    drs = r"^/?(?:[^/]+/)*" + drs
    return re.compile(drs)


def translate_attrs_to_CMIP6(attrs):
    translated_attrs = {}
    for key, value in attrs.items():
        if key in attrs_mapping:
            translated_key = attrs_mapping[key]
            translated_attrs[translated_key] = value
        else:
            translated_attrs[key] = value
    return translated_attrs


def parse_filepath(filename, project):
    # pattern = create_pattern(drs)
    regex = pattern_dict[project]
    mip_era = project.split("-")[1]
    regex = r"^/?(?:[^/]+/)*" + regex
    pattern = re.compile(regex)
    match = pattern.match(filename)
    if not match:
        print(f"Error: Parsing failed for: {filename}")
        return {}
    attrs = match.groupdict() | {"mip_era": mip_era}
    if match:
        check = check_consistency(attrs)
        if check:
            print(f"Warning: parsing returns inconsistent attributes: {check}")
            print(f"Ignoring: {filename}")
            return {}
    else:
        print(
            f"The filepath does not match the expected pattern (will be ignored): {filename}"
        )
        return {}
    if mip_era == "CMIP5":
        attrs = translate_attrs_to_CMIP6(attrs)
    return attrs


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
    df_CMIP5 = update_catalog(CATALOG, root_dic["CORDEX-CMIP5"], "CORDEX-CMIP5")
    df_CMIP6 = update_catalog(CATALOG, root_dic["CORDEX-CMIP6"], "CORDEX-CMIP6")
    df = pd.concat([df_CMIP5, df_CMIP6])
    folder_path = "./"  # os.path.abspath(os.path.join(os.getcwd(), "..", ".."))
    df.to_csv(os.path.join(folder_path, f"{CATALOG}"), index=False)
    create_excel(os.path.join(folder_path, f"{CATALOG}"))
