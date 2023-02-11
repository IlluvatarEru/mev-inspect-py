import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from profit_analysis.coingecko import add_cg_ids
from profit_analysis.column_names import CG_ID_RECEIVED_KEY
from profit_analysis.constants import DATA_PATH

PROFIT_DISTRIBUTION_FILE_NAME = "profit_distribution.png"
COLOR_ONE = "navy"
COLOR_TWO = "lightblue"


def get_all_graphs():
    graph_profit_by("block_number")
    graph_profit_by("date")
    graph_count_by("block_number")
    graph_count_by("date")

    plot_distribution_by("block_number", "sum", "profit")
    plot_distribution_by("date", "sum", "profit")
    plot_distribution_by("block_number", "count", "tx #")
    plot_distribution_by("date", "count", "tx #")
    total_usd_profit = pd.read_csv(DATA_PATH + "total_usd_profit.csv")
    plot_profit_distribution(total_usd_profit)


def hist_data(data, bins=None):
    if bins is None:
        bins = int(np.sqrt(len(data)))
    hist, bin_edges = np.histogram(data, bins=bins)
    return hist, bin_edges


def plot_profit_distribution(profit: pd.DataFrame):
    profit = profit[np.isfinite(profit["profit_usd"])]["profit_usd"]
    hist, bin_edges = hist_data(profit)
    plt.bar(
        bin_edges[:-1], hist, width=np.diff(bin_edges), align="edge", color=COLOR_ONE
    )
    plt.xlabel("Profit (USD)")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of profit by tx")
    plt.savefig(DATA_PATH + PROFIT_DISTRIBUTION_FILE_NAME)


def compute_profit_skewness(profit: pd.DataFrame):
    return profit["profit_usd"].skew()


def compute_profit_kurtosis(profit: pd.DataFrame):
    return profit["profit_usd"].kurtosis()


def get_top_tokens(profit, chain, top=10, save_to_csv=False):
    profit = add_cg_ids(profit, chain, False)
    top_tokens = profit[CG_ID_RECEIVED_KEY].value_counts().sort_values(ascending=False)
    top_tokens = top_tokens.reset_index()
    top_tokens.columns = ["Token", "Count"]
    n_tx = top_tokens["Count"].sum()
    top_tokens["Frequency"] = top_tokens["Count"] / n_tx
    if save_to_csv:
        file_name = DATA_PATH + "top_tokens.csv"
        top_tokens.to_csv(file_name, index=False)
    top_tokens = top_tokens.head(top)
    return top_tokens


def graph_profit_by(by_column="block_number"):
    # Load the data and calculate the cumulative sum
    file_path = DATA_PATH + f"profit_by_{by_column}.csv"
    data = pd.read_csv(file_path)
    data["cumsum"] = data["sum"].cumsum()

    # Plot the bar plot
    ax = data.plot(x=by_column, y="sum", kind="bar", label="", color=COLOR_ONE)
    ax.legend(loc="center left")

    # Create a second y-axis on the right hand side of the plot
    ax2 = ax.twinx()
    # Add the line plot of the cumulative sum to the second y-axis
    data["cumsum"].plot(kind="line", ax=ax2, label="cumulative profit", color=COLOR_TWO)
    ax2.legend(loc="upper left")

    # Reduce the frequency of the x-axis ticks
    x_ticks = np.arange(0, len(data), 1000)
    ax.set_xticks(x_ticks)
    plt.xlabel(by_column)
    plt.ylabel("MEV Profit ($)")
    plt.title(f"MEV Profit by {by_column}")
    file_name = f"timeseries_of_profit_by_{by_column}.png"
    plt.savefig(DATA_PATH + file_name)
    plt.show()


def graph_count_by(by_column="block_number"):
    # Load the data and calculate the cumulative sum
    file_path = DATA_PATH + f"profit_by_{by_column}.csv"
    data = pd.read_csv(file_path)
    data["cumcount"] = data["count"].cumsum()

    # Plot the bar plot
    ax = data.plot(x=by_column, y="count", kind="bar", label="", color=COLOR_ONE)
    ax.legend(loc="center left")

    # Create a second y-axis on the right hand side of the plot
    ax2 = ax.twinx()
    # Add the line plot of the cumulative sum to the second y-axis
    data["cumcount"].plot(
        kind="line", ax=ax2, label="cumulative # of tx", color=COLOR_TWO
    )
    ax2.legend(loc="upper left")

    # Reduce the frequency of the x-axis ticks
    x_ticks = np.arange(0, len(data), 1000)
    ax.set_xticks(x_ticks)
    plt.xlabel(by_column)
    plt.ylabel("# of txs")
    plt.title(f"# of txs by {by_column}")
    file_name = f"timeseries_of_tx_#_by_{by_column}.png"
    plt.savefig(DATA_PATH + file_name)
    plt.show()


def plot_distribution_by(by_column, col="count", col_name=None):
    if col_name is None:
        col_name = col
    file_path = DATA_PATH + f"profit_by_{by_column}.csv"
    data = pd.read_csv(file_path)
    data = data[np.isfinite(data[col])][col]
    hist, bin_edges = hist_data(data)
    plt.bar(
        bin_edges[:-1], hist, width=np.diff(bin_edges), align="edge", color=COLOR_ONE
    )
    plt.xlabel(f"# of txs by {by_column}")
    plt.ylabel("Frequency")
    plt.title(f"Distribution of {col_name} by {by_column}")
    col_name = col_name.replace(" ", "_")
    file_name = f"distribution_of_{col_name}_by_{by_column}.png"
    plt.savefig(DATA_PATH + file_name)
    plt.show()
