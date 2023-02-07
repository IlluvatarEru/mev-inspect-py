import pandas as pd
import pycoingecko as pg
from profit_analysis.column_names import (
    AMOUNT_DEBT_KEY,
    AMOUNT_RECEIVED_KEY,
    BLOCK_KEY,
    CG_ID_DEBT_KEY,
    CG_ID_KEY,
    CG_ID_RECEIVED_KEY,
    PRICE_KEY,
    TIMESTAMP_KEY,
    TOKEN_DEBT_KEY,
    TOKEN_KEY,
    TOKEN_RECEIVED_KEY,
    TRANSACTION_HASH_KEY,
)
from profit_analysis.constants import DATA_PATH

TRAILING_ZEROS = "000000000000000000000000"


def get_address_to_coingecko_ids_mapping(chain, add_cg_id_debt=True):
    """
    return: pd.DataFrame, with columns [CG_ID_KEY, TOKEN_KEY]
    """
    token_cg_ids = pd.read_csv(DATA_PATH + "address_to_coingecko_ids.csv")
    token_cg_ids = token_cg_ids[[CG_ID_KEY, chain.lower()]]
    token_cg_ids.columns = [CG_ID_KEY, TOKEN_KEY]
    token_cg_ids[CG_ID_RECEIVED_KEY] = token_cg_ids[CG_ID_KEY]
    token_cg_ids[TOKEN_RECEIVED_KEY] = token_cg_ids[TOKEN_KEY].astype(str)
    if add_cg_id_debt:
        token_cg_ids[TOKEN_DEBT_KEY] = token_cg_ids[TOKEN_KEY].astype(str)
        token_cg_ids[CG_ID_DEBT_KEY] = token_cg_ids[CG_ID_KEY]
    return token_cg_ids


def add_cg_ids(profit_by_block, chain, add_cg_id_debt=True):
    token_cg_ids = get_address_to_coingecko_ids_mapping(
        chain, add_cg_id_debt=add_cg_id_debt
    )
    token_cg_ids[TOKEN_RECEIVED_KEY] = token_cg_ids[TOKEN_RECEIVED_KEY].str.lower()
    profit_by_block[TOKEN_RECEIVED_KEY] = (
        profit_by_block[TOKEN_RECEIVED_KEY]
        .map(lambda x: x.replace(TRAILING_ZEROS, ""))
        .str.lower()
    )
    profit_by_block = profit_by_block.merge(
        token_cg_ids[[TOKEN_RECEIVED_KEY, CG_ID_RECEIVED_KEY]], how="left"
    )
    columns_to_return = [
        BLOCK_KEY,
        TIMESTAMP_KEY,
        TRANSACTION_HASH_KEY,
        TOKEN_RECEIVED_KEY,
        AMOUNT_RECEIVED_KEY,
        CG_ID_RECEIVED_KEY,
    ]
    if add_cg_id_debt:
        token_cg_ids[TOKEN_DEBT_KEY] = token_cg_ids[TOKEN_DEBT_KEY].str.lower()
        profit_by_block[TOKEN_DEBT_KEY] = (
            profit_by_block[TOKEN_DEBT_KEY]
            .map(lambda x: x.replace(TRAILING_ZEROS, ""))
            .str.lower()
        )
        profit_by_block = profit_by_block.merge(
            token_cg_ids[[TOKEN_DEBT_KEY, CG_ID_DEBT_KEY]],
            on=TOKEN_DEBT_KEY,
            how="left",
        )
        columns_to_return += [TOKEN_DEBT_KEY, AMOUNT_DEBT_KEY, CG_ID_DEBT_KEY]

    addresses_with_nan_cg_ids = profit_by_block.loc[
        pd.isna(profit_by_block[CG_ID_RECEIVED_KEY]), TOKEN_RECEIVED_KEY
    ]
    print(
        f"Tokens with missing coingecko ids in mapping:\n{addresses_with_nan_cg_ids.value_counts()}"
    )
    return profit_by_block[columns_to_return]


def get_coingecko_historical_prices(start, end, token):
    cg = pg.CoinGeckoAPI()
    token_prices = cg.get_coin_market_chart_range_by_id(
        id=token, vs_currency="usd", from_timestamp=start, to_timestamp=end
    )["prices"]
    token_prices = pd.DataFrame(token_prices, columns=[TIMESTAMP_KEY, PRICE_KEY])
    token_prices[TIMESTAMP_KEY] = pd.to_datetime(
        pd.to_numeric(token_prices[TIMESTAMP_KEY]), unit="ms"
    )
    return token_prices[[TIMESTAMP_KEY, PRICE_KEY]]


def get_token_address_from_lower(lower_address, chain):
    token_cg_ids = pd.read_csv(DATA_PATH + "address_to_coingecko_ids.csv")
    token_addresses = token_cg_ids[chain].unique()
    for address in token_addresses:
        if address.lower() == lower_address:
            return address
    return "NA"
