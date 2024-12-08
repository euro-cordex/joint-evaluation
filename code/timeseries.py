import dask
from dask.diagnostics import ProgressBar
import matplotlib.pyplot as plt
from dask.distributed import Client
import regionmask
import seaborn as sns


from evaltools.source import get_source_collection, open_and_sort
from evaltools.eval import regional_means

dask.config.set(scheduler="single-threaded")
sns.set_theme(style="darkgrid")

variables = ["tas", "pr"]


def create_regional_means():
    catalog = get_source_collection(variables, "mon")
    dsets = open_and_sort(catalog, merge=True)

    means = regional_means(dsets, regionmask.defined_regions.prudence)
    with ProgressBar():
        data = means.groupby("time.year").mean().compute().to_dataframe().reset_index()

    return data


def plot(data, y):
    ax = sns.relplot(
        data=data,
        x="year",
        y=y,
        hue="iid",
        col="names",
        col_wrap=4,
        kind="line",
        facet_kws=dict(sharey=True),
    )
    sns.move_legend(ax, "lower left", bbox_to_anchor=(0.2, -0.1))
    ax.savefig(f"plots/prudence-timeseries-{y}.png", dpi=300)
    plt.close()


if __name__ == "__main__":
    with Client(dashboard_address=None, threads_per_worker=1) as client:
        data = create_regional_means()

    for y in variables:
        print(f"plotting: {y}")
        plot(data, y)
