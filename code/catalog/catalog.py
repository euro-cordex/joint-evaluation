import os
import re
import pandas as pd
from os import path as op

DRS = {
    "directory_path_template": "<project_id>/<mip_era>/<activity_id>/<domain_id>/<institution_id>/<driving_source_id>/<driving_experiment_id>/<driving_variant_label>/<source_id>/<version_realization>/<frequency>/<variable_id>/<version>",
    "filename_template": "<variable_id>_<domain_id>_<driving_source_id>_<driving_experiment_id>_<driving_variant_label>_<institution_id>_<source_id>_<version_realization>_<frequency>[_<time_range>].nc",
}

ROOT = "/mnt/CORDEX_CMIP6_tmp/sim_data/CORDEX/CMIP6"
CATALOG = "catalog.csv"


def create_pattern(drs):
    attrs = drs.split("/")
    drs = "/".join([f"(?P{attr}[^/]+)" for attr in attrs])
    # Allow for an optional root directory
    drs = r"^/?(?:[^/]+/)*" + drs
    return re.compile(drs)


def parse_filepath(filepath, drs):
    pattern = create_pattern(drs)
    match = pattern.match(filepath)
    if match:
        return match.groupdict()
    else:
        raise ValueError("The filepath does not match the expected pattern.")


def create_catalog(root):
    datasets = []
    for root, dirs, files in os.walk(root):
        # only parse if files found
        if not files:
            continue
        try:
            print(f"parsing {root}")
            metadata = parse_filepath(root, DRS["directory_path_template"])
            datasets.append(metadata)
        except ValueError:
            print(f"Could not parse {root}")
            continue
    return datasets


def human_readable(df):
    cols = [
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
    ]
    return df.groupby(cols)["variable_id"].apply(list).to_frame()  # .reset_index()


def create_excel(filename):
    """create human readable excel file with one sheet per periority"""

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
            for i in range(nlevels):
                worksheet.set_column(i, i, 40)

    return xlsxfile


def update_catalog(catalog, root):
    df = pd.DataFrame(create_catalog(root))
    print(f"writing catalog to {catalog}")
    df.to_csv(catalog, index=False)
    return df


if __name__ == "__main__":
    df = update_catalog(CATALOG, ROOT)
    create_excel(CATALOG)
    print(df)
