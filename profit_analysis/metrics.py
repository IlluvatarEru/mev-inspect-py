import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from profit_analysis.coingecko import add_cg_ids
from profit_analysis.column_names import CG_ID_RECEIVED_KEY
from profit_analysis.constants import DATA_PATH

PROFIT_DISTRIBUTION_FILE_NAME = "profit_distribution.png"


def hist_data(data, bins=None):
    if bins is None:
        bins = int(np.sqrt(len(data)))
    hist, bin_edges = np.histogram(data, bins=bins)
    return hist, bin_edges


def plot_profit_distribution(profit: pd.DataFrame):
    profit = profit[np.isfinite(profit["profit_usd"])]["profit_usd"]
    hist, bin_edges = hist_data(profit)
    plt.bar(bin_edges[:-1], hist, width=np.diff(bin_edges), align="edge")
    plt.xlabel("Profit (USD)")
    plt.ylabel("Frequency")
    plt.savefig(DATA_PATH + PROFIT_DISTRIBUTION_FILE_NAME)
    return hist, bin_edges


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
