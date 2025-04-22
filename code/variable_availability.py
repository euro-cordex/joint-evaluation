#!/usr/bin/env python3

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from icecream import ic


def get_studies(dreq):
    studies = set()
    for priorities in dreq["priority"].dropna():
        for priority in priorities.split():
            studies.add(priority.strip())
    return sorted(list(studies))


def plot_availability(study, dreq, catalog, plans, outname="availability.png"):
    dreq_study = dreq.query("priority.str.contains(@study)")
    dreq_study = dreq_study[["out_name", "frequency"]].rename(
        columns={"out_name": "variable_id"}
    )
    #
    #  Plot variable availability as heatmap
    #
    data = catalog.merge(dreq_study, on=["variable_id", "frequency"], how="right")
    # Avoid showing different subdaily frequencies
    # data['frequency'] = data['frequency'].replace('.hr', 'xhr', regex = True)
    data.drop_duplicates(inplace=True)
    # matrix with models as rows and variables as columns
    matrix = data.pivot_table(
        index=["mip_era", "source_id"],
        columns=["frequency", "variable_id"],
        aggfunc="size",
        fill_value=0,
    )
    matrix = matrix.replace(0, np.nan)
    plans_empty = pd.DataFrame(columns=matrix.columns, index=plans.index)
    plans_empty[:] = np.nan
    matrix = matrix.combine_first(plans_empty).astype(float)
    cmip5_mask = matrix.index.get_level_values(0) == "CMIP5"
    matrix.loc[cmip5_mask, :] *= 0.5  # change value (i.e color) for CMIP5-driven sims.
    ic(matrix)
    #
    # Plot as heatmap (make sure to show all ticks and labels)
    #
    plt.figure(figsize=(14, 12))
    plt.title(f"Variable availability for {study} study")
    ax = sns.heatmap(
        matrix,
        cmap="Blues",
        vmin=0,
        vmax=1,
        annot=False,
        cbar=False,
        linewidths=1,
        linecolor="lightgray",
    )
    ax.set_xticks(0.5 + np.arange(len(matrix.columns)))
    xticklabels = [f"{v}\n({f})" for f, v in matrix.columns]
    xticklabels = (
        pd.Series(xticklabels)
        .replace(r"(.*) \(fx\)", r"\1 (fx)   ", regex=True)
        .replace(r"(.*) \(xhr\)", r"\1 (xhr)  ", regex=True)
    ).to_list()
    ax.set_xticklabels(xticklabels)
    ax.set_xlabel("variable (freq.)")
    ax.set_yticks(0.5 + np.arange(len(matrix.index)))
    yticklabels = [f"{s}" if e == "CMIP6" else f"{s} ({e})" for e, s in matrix.index]
    ax.set_yticklabels(yticklabels, rotation=0)
    ax.set_ylabel("source_id")
    ax.set_aspect("equal")
    plt.savefig(outname, bbox_inches="tight")
    plt.close()


if __name__ == "__main__":
    url_dreq = "dreq_EUR_joint_evaluation.csv"
    url_plans = "https://raw.githubusercontent.com/WCRP-CORDEX/simulation-status/refs/heads/main/CMIP6_downscaling_plans.csv"
    dreq = pd.read_csv(url_dreq)
    plans = (
        pd.read_csv(url_plans)
        .query(
            "domain == 'EUR-12' & experiment == 'evaluation' & status in ['completed',]"
        )
        .assign(mip_era="CMIP6")
        .rename(columns={"rcm_name": "source_id"})
        .loc[:, ["mip_era", "source_id"]]
        .set_index(["mip_era", "source_id"])
    )
    catalog = pd.read_csv(
        "catalog.csv", usecols=["variable_id", "frequency", "source_id", "mip_era"]
    )
    md_lines = ["# Variable Availability Plots\n"]
    for study in get_studies(dreq):
        plot_path = f"plots/variable_availability__{study}.png"
        plot_availability(study, dreq, catalog, plans, outname=plot_path)
        md_lines.append(f"## {study}")
        md_lines.append(f"![{study}]({plot_path})\n")

    with open("variable_availability.md", "w") as md_file:
        md_file.write("\n".join(md_lines))
