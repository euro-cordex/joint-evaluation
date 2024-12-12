import os
import re
import pandas as pd
from os import path as op

# DRS = {
#     "directory_path_template": "<project_id>/<mip_era>/<activity_id>/<domain_id>/<institution_id>/<driving_source_id>/<driving_experiment_id>/<driving_variant_label>/<source_id>/<version_realization>/<frequency>/<variable_id>/<version>",
#     "filename_template": "<variable_id>_<domain_id>_<driving_source_id>_<driving_experiment_id>_<driving_variant_label>_<institution_id>_<source_id>_<version_realization>_<frequency>[_<time_range>].nc",
# }

ROOT = "/mnt/CORDEX_CMIP6_tmp/sim_data/CORDEX/CMIP6"
CATALOG = "catalog.csv"

COLS = [
    "project_id",
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


def create_path_pattern(drs, sep="/"):
    attrs = drs.split(sep)
    drs = sep.join([f"(?P{attr}[^/]+)" for attr in attrs])
    # Allow for an optional root directory
    drs = r"^/?(?:[^/]+/)*" + drs
    return re.compile(drs)


def parse_filepath(filename):
    # pattern = create_pattern(drs)
    regex = r"(?P<project_id>[^/]+)/(?P<mip_era>[^/]+)/(?P<activity_id>[^/]+)/(?P<domain_id>[^/]+)/(?P<institution_id>[^/]+)/(?P<driving_source_id>[^/]+)/(?P<driving_experiment_id>[^/]+)/(?P<driving_variant_label>[^/]+)/(?P<source_id>[^/]+)/(?P<version_realization>[^/]+)/(?P<frequency>[^/]+)/(?P<variable_id>[^/]+)/(?P<version>[^/]+)/(?P<filename>(?P<variable_id_2>[^_]+)_(?P<domain_id_2>[^_]+)_(?P<driving_source_id_2>[^_]+)_(?P<driving_experiment_id_2>[^_]+)_(?P<driving_variant_label_2>[^_]+)_(?P<institution_id_2>[^_]+)_(?P<source_id_2>[^_]+)_(?P<version_realization_2>[^_]+)_(?P<frequency_2>[^_]+)(?:_(?P<time_range>[^.]+))?\.nc)"
    regex = r"^/?(?:[^/]+/)*" + regex
    pattern = re.compile(regex)
    match = pattern.match(filename)
    if match:
        return match.groupdict()
    else:
        raise ValueError("The filepath does not match the expected pattern.")


def create_catalog(root):
    datasets = []
    # Define the regex pattern for the filename
    for root, dirs, files in os.walk(root):
        # only parse if files found
        if not files:
            continue
        for file in files:
            filename = op.join(root, file)
            print(f"parsing {filename}")
            metadata = parse_filepath(filename)
            metadata["path"] = filename
            datasets.append(metadata)
    return datasets


def human_readable(df):
    cols = [item for item in COLS if item not in ["variable_id", "time_range"]]

    def to_list(x):
        return list(dict.fromkeys(list(x)))

    return df.groupby(cols)["variable_id"].apply(to_list).to_frame()  # .reset_index()


def create_excel(filename):
    """create human readable excel file"""

    df = pd.read_csv(filename)
    sheets = {"jsc-cordex": human_readable(df)}

    stem, suffix = op.splitext(filename)
    xlsxfile = f"{stem}.xlsx"

    with pd.ExcelWriter(xlsxfile) as writer:
        for k, v in sheets.items():
            print(k)
            v.to_excel(writer, sheet_name=k, index=True)
            nlevels = v.index.nlevels + len(v.columns)
            worksheet = writer.sheets[k]  # pull worksheet object
            
            # Set the column width to the maximum width of the content
            for idx, col in enumerate(sheet_df.columns):
                max_len = max(
                    sheet_df[col].astype(str).map(len).max(),  # len of largest item
                    len(str(col))  # len of column name/header
                ) + 2  # adding a little extra space
                worksheet.set_column(idx + len(sheet_df.index.names), idx + len(sheet_df.index.names), max_len)

            # Set the column width for the index levels
            for idx, level in enumerate(sheet_df.index.names):
                max_len = max(
                    sheet_df.index.get_level_values(level).astype(str).map(len).max(),  # len of largest item in index level
                    len(str(level))  # len of index level name
                ) + 2  # adding a little extra space
                worksheet.set_column(idx, idx, max_len)

    return xlsxfile


def update_catalog(catalog, root):
    df = pd.DataFrame(create_catalog(root))[COLS + ["path"]]
    print(f"writing catalog to {catalog}")
    df.to_csv(catalog, index=False)
    return df


if __name__ == "__main__":
    df = update_catalog(CATALOG, ROOT)
    create_excel(CATALOG)
    print(df)
