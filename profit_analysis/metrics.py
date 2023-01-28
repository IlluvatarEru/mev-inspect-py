import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

PROFIT_DISTRIBUTION_FILE_NAME = "profit_distribution.png"


def plot_profit_distribution(profit: pd.DataFrame):
    profit = profit["profit_usd"]
    optimal_bins = int(np.sqrt(len(profit)))
    plt.hist(column=profit, bins=optimal_bins)
    plt.xlabel("Profit (USD)")
    plt.ylabel("Frequency")
    plt.savefig(PROFIT_DISTRIBUTION_FILE_NAME)
    plt.show()


def compute_profit_skewness(profit: pd.DataFrame):
    return profit["profit_usd"].skew()


def compute_profit_kurtosis(profit: pd.DataFrame):
    return profit["profit_usd"].kurtosis()
