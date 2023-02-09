import os

import pandas as pd

from mev_inspect.crud.read import read_profit_from_to
from mev_inspect.web3_provider import W3
from profit_analysis.block_utils import add_block_timestamp
from profit_analysis.chains import (
    ARBITRUM_CHAIN,
    ETHEREUM_CHAIN,
    OPTIMISM_CHAIN,
    POLYGON_CHAIN,
)
from profit_analysis.coingecko import (
    get_address_to_coingecko_ids_mapping,
)
from profit_analysis.column_names import (
    AMOUNT_DEBT_KEY,
    AMOUNT_RECEIVED_KEY,
    BLOCK_KEY,
    CATEGORY_KEY,
    DATE_KEY,
    DECIMAL_DEBT_KEY,
    PRICE_DEBT_KEY,
    PRICE_KEY,
    PRICE_RECEIVED_KEY,
    PROFIT_USD_KEY,
    TIMESTAMP_KEY,
    TOKEN_DEBT_KEY,
    TOKEN_KEY,
    TOKEN_RECEIVED_KEY,
    TRANSACTION_HASH_KEY,
)
from profit_analysis.constants import DATA_PATH
from profit_analysis.metrics import (
    compute_profit_kurtosis,
    compute_profit_skewness,
    get_top_tokens,
    plot_profit_distribution,
)
from profit_analysis.prices import get_uniswap_historical_prices
from profit_analysis.token_utils import get_decimals


def analyze_profit(profit, save_to_csv=False):
    print(f"Launching profit analysis, saving to CSV? {save_to_csv}")
    rpc_url = os.environ.get("RPC_URL")
    chain = get_chain_from_url(rpc_url)
    print("    -------------------------------------------------------------------")
    print("    Profit By Block")
    print(get_profit_by(profit, BLOCK_KEY, save_to_csv))
    print("    -------------------------------------------------------------------")
    print("    Profit By Day")
    print(get_profit_by(profit, DATE_KEY, save_to_csv))
    print("    -------------------------------------------------------------------")
    print("    Profit By Category")
    print(get_profit_by(profit, CATEGORY_KEY, save_to_csv))
    print("    -------------------------------------------------------------------")
    print("    Profit Skewnes")
    print(compute_profit_skewness(profit))
    print("    -------------------------------------------------------------------")
    print("    Profit Kurtosis")
    print(compute_profit_kurtosis(profit))
    print("    -------------------------------------------------------------------")
    print("    Top 10 tokens profit was taken in")
    print(get_top_tokens(profit, chain, 10, save_to_csv))
    print("    -------------------------------------------------------------------")
    print("    Profit Distribution")
    print(plot_profit_distribution(profit))


async def compute_usd_profit(
        inspect_db_session, block_from, block_to, save_to_csv=False
):
    """

    :return: pd.DataFrame, with columns = ['block_number', 'timestamp', 'date', 'transaction_hash',
       'amount_received', 'token_received', 'price_received',
       'amount_debt', 'token_debt', 'price_debt',
       'profit_usd' ]
    """
    profit = read_profit_from_to(inspect_db_session, block_from, block_to)
    profit = add_block_timestamp(profit)
    chain = get_chain_from_url(W3.w3_provider.provider.endpoint_uri)
    profit = await get_usd_profit(profit, chain, save_to_csv)
    print(profit)
    return profit


async def get_usd_profit(profit, chain, save_to_csv=False):
    """
    For each token involved in mev transactions, will get its price at the time of the transaction and
    compute the profit of each mev transaction.

    :param profit: pd.DataFrame, with columns = ['block_number', 'timestamp', 'transaction_hash',
        'token_debt', 'amount_debt',
       'token_received', 'amount_received']
    :param chain: str, the blockchain
    :param save_to_csv: bool, whether to save the analysed profits to csv or not
    :return: pd.DataFrame, with columns = ['block_number', 'timestamp', 'date', 'transaction_hash',
       'amount_received', 'token_received', 'price_received',
       'amount_debt', 'token_debt', 'price_debt',
       'profit_usd' ]
    """
    print(f"Computing USD profit for:\n{profit}")
    tokens = list(profit[TOKEN_RECEIVED_KEY].unique())
    mapping = get_address_to_coingecko_ids_mapping(chain)
    profit_with_price_tokens = pd.DataFrame()
    profit[TIMESTAMP_KEY] = pd.to_datetime(
        profit[TIMESTAMP_KEY], format="%Y-%m-%d %H:%M:%S"
    )
    profit[BLOCK_KEY] = pd.to_numeric(profit[BLOCK_KEY], downcast="integer")
    profit = profit.sort_values(by=[BLOCK_KEY])
    failures = {}
    for i in range(len(tokens)):
        token = tokens[i]
        print(f"Processing {token} ")
        if (token != "nan") and (not pd.isna(token)):
            try:
                profit_by_received_token = pd.DataFrame(
                    profit.loc[profit[TOKEN_RECEIVED_KEY] == token]
                )

                # get prices
                target_blocks = profit_by_received_token[BLOCK_KEY].unique()
                token_prices = await get_uniswap_historical_prices(
                    target_blocks,
                    token,
                    chain,
                )
                token_prices = token_prices.rename(
                    columns={PRICE_KEY: PRICE_RECEIVED_KEY}
                )
                token_prices[TOKEN_RECEIVED_KEY] = token

                # get received token decimals
                decimals = get_decimals(
                    profit_by_received_token[TOKEN_RECEIVED_KEY].values[0], chain
                )

                # get debt tokens prices
                debt_tokens_prices = pd.DataFrame()
                debt_tokens = (
                    profit_by_received_token[TOKEN_DEBT_KEY]
                    .astype(str)
                    .unique()
                    .tolist()
                )
                debt_tokens.remove("nan")

                for k in range(len(debt_tokens)):
                    debt_token = debt_tokens[k]
                    if debt_token != "nan":
                        # get prices
                        target_blocks = profit_by_received_token[BLOCK_KEY].unique()
                        debt_token_prices = await get_uniswap_historical_prices(
                            target_blocks,
                            debt_token,
                            chain,
                        )
                        debt_token_prices[TOKEN_DEBT_KEY] = debt_token
                        debt_tokens_prices = pd.concat(
                            [debt_tokens_prices, debt_token_prices]
                        )
                debt_tokens_prices = debt_tokens_prices.rename(
                    columns={PRICE_KEY: PRICE_DEBT_KEY}
                )

                # get debt tokens decimals
                debt_tokens_decimals = pd.DataFrame(
                    columns=[TOKEN_DEBT_KEY, DECIMAL_DEBT_KEY]
                )
                for debt_token in (
                        profit_by_received_token[TOKEN_DEBT_KEY]
                                .astype(str)
                                .unique()
                                .tolist()
                ):
                    if debt_token != "":
                        debt_token_decimals = get_decimals(debt_token, chain)
                        debt_tokens_decimals = pd.concat(
                            [
                                debt_tokens_decimals,
                                pd.DataFrame(
                                    [[debt_token, debt_token_decimals]],
                                    columns=[TOKEN_DEBT_KEY, DECIMAL_DEBT_KEY],
                                ),
                            ]
                        )

                profit_by_received_token = profit_by_received_token.merge(
                    debt_tokens_decimals, on=TOKEN_DEBT_KEY, how="outer"
                )

                profit_by_received_token.loc[
                    pd.isna(profit_by_received_token[AMOUNT_DEBT_KEY]), AMOUNT_DEBT_KEY
                ] = 0

                # apply decimals
                profit_by_received_token[AMOUNT_RECEIVED_KEY] = pd.to_numeric(
                    profit_by_received_token[AMOUNT_RECEIVED_KEY]
                ).div(10 ** decimals)
                profit_by_received_token[AMOUNT_DEBT_KEY] = pd.to_numeric(
                    profit_by_received_token[AMOUNT_DEBT_KEY]
                )

                profit_with_price_token = pd.merge_asof(
                    profit_by_received_token,
                    token_prices,
                    direction="nearest",
                    on=BLOCK_KEY,
                    suffixes=("", "_y"),
                )

                if len(debt_tokens_prices) > 0:
                    debt_tokens_prices[TIMESTAMP_KEY] = pd.to_datetime(
                        debt_tokens_prices[TIMESTAMP_KEY]
                    )

                    profit_with_price_token = pd.merge_asof(
                        profit_with_price_token,
                        debt_tokens_prices,
                        direction="nearest",
                        on=BLOCK_KEY,
                        suffixes=("", "_y"),
                    )

                    category = "liquidation"
                else:
                    category = "arbitrage"
                    profit_with_price_token[PRICE_DEBT_KEY] = 0

                profit_with_price_token[CATEGORY_KEY] = category

                profit_with_price_tokens = pd.concat(
                    [profit_with_price_tokens, profit_with_price_token]
                )
            except Exception as e:
                print(f"    Failed for token={token}")
                print(e)
                failures[token] = e
    print("Finished processing all tokens")
    print(f"profit_with_price_tokens=\n{profit_with_price_tokens}")
    profit_with_price_tokens[PRICE_DEBT_KEY] = profit_with_price_tokens[
        PRICE_DEBT_KEY
    ].fillna(value=0)
    profit_with_price_tokens[AMOUNT_DEBT_KEY] = profit_with_price_tokens[
        AMOUNT_DEBT_KEY
    ].fillna(value=0)
    profit_with_price_tokens[PROFIT_USD_KEY] = (
            profit_with_price_tokens[AMOUNT_RECEIVED_KEY]
            * profit_with_price_tokens[PRICE_RECEIVED_KEY]
            - profit_with_price_tokens[AMOUNT_DEBT_KEY]
            * profit_with_price_tokens[PRICE_DEBT_KEY]
    )
    profit_with_price_tokens = profit_with_price_tokens.reset_index(drop=True)
    profit_with_price_tokens[DATE_KEY] = profit_with_price_tokens[
        TIMESTAMP_KEY
    ].dt.normalize()
    if save_to_csv:
        profit_with_price_tokens.to_csv(DATA_PATH + "usd_profit.csv", index=False)
        pd.DataFrame(failures.items(), columns=[TOKEN_KEY, "error"]).to_csv(
            DATA_PATH + "analyze_profit_failures.csv", index=False
        )
    return profit_with_price_tokens[
        [
            BLOCK_KEY,
            TIMESTAMP_KEY,
            DATE_KEY,
            TRANSACTION_HASH_KEY,
            AMOUNT_RECEIVED_KEY,
            TOKEN_RECEIVED_KEY,
            PRICE_RECEIVED_KEY,
            AMOUNT_DEBT_KEY,
            TOKEN_DEBT_KEY,
            PRICE_DEBT_KEY,
            PROFIT_USD_KEY,
            CATEGORY_KEY,
        ]
    ]


def get_profit_by(profit_with_price_tokens, col, save_to_csv=False):
    profit_by_block = (
        profit_with_price_tokens.groupby([col])
        .agg({"profit_usd": ["sum", "mean", "median", "count"]})
        .reset_index()
    )
    profit_by_block.columns = profit_by_block.columns.droplevel(0)
    profit_by_block.rename(columns={"": col}, inplace=True)
    if save_to_csv:
        file_name = DATA_PATH + "profit_by_" + col + ".csv"
        profit_by_block.to_csv(file_name, index=False)
    return profit_by_block


def get_chain_from_url(url):
    if "ether" in url:
        return ETHEREUM_CHAIN
    elif "poly" in url:
        return POLYGON_CHAIN
    elif "arb" in url:
        return ARBITRUM_CHAIN
    elif "opti" in url:
        return OPTIMISM_CHAIN
    else:
        raise Exception(f"Could not determine blockchain from url: {url}")
