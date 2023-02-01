import datetime
from time import sleep

import pandas as pd
from profit_analysis.column_names import BLOCK_KEY, TIMESTAMP_KEY

from mev_inspect.web3_provider import W3


def add_block_timestamp(profit_by_block):
    block_timestamp = pd.DataFrame(
        profit_by_block[BLOCK_KEY].unique(), columns=[BLOCK_KEY]
    )
    block_timestamp[TIMESTAMP_KEY] = block_timestamp[BLOCK_KEY].apply(
        lambda x: get_block_timestamp(x)
    )
    return profit_by_block.merge(block_timestamp, on=BLOCK_KEY)


def get_block_timestamp(block):
    trials = 0
    n_trials = 3
    while trials < 3:
        trials += 1
        try:
            block_info = W3.w3_provider.eth.get_block(int(block))
            ts = block_info[TIMESTAMP_KEY]
            dt = datetime.datetime.fromtimestamp(ts)
            return dt
        except Exception as e:
            print(f"Error ({trials}/{n_trials}), retrying {e}")
            sleep(0.05)
    W3.rotate_rpc_url()
    return get_block_timestamp(block)


def interpolate_block_number(tx, t1, t2, b1, b2):
    return int(b1 + (b2 - b1) * (tx - t1) / (t2 - t1))


def find_block_for_timestamp_polygon(w3_polygon, ts):
    start_of_2020_poly_block_number = 9194000
    start_of_2020_poly_block_timestamp = 1577832303
    last_poly_block = w3_polygon.eth.get_block("latest")
    last_poly_block_number = last_poly_block["number"]
    last_poly_block_timestamp = last_poly_block["timestamp"]
    interpolated_block_number, _ = find_block_for_timestamp(
        w3_polygon,
        ts,
        start_of_2020_poly_block_timestamp,
        last_poly_block_timestamp,
        start_of_2020_poly_block_number,
        last_poly_block_number,
    )
    return interpolated_block_number


def find_block_for_timestamp_ethereum(w3_ethereum, ts):
    start_of_2020_eth_block_number = 9194000
    start_of_2020_eth_block_timestamp = 1577832303
    last_eth_block = w3_ethereum.eth.get_block("latest")
    last_eth_block_number = last_eth_block["number"]
    last_eth_block_timestamp = last_eth_block["timestamp"]
    interpolated_block_number, _ = find_block_for_timestamp(
        w3_ethereum,
        ts,
        start_of_2020_eth_block_timestamp,
        last_eth_block_timestamp,
        start_of_2020_eth_block_number,
        last_eth_block_number,
    )
    return interpolated_block_number


def find_block_for_timestamp(
    w3, ts, start_ts, end_ts, start_block, end_block, threshold=60
):
    interpolated_block_number = interpolate_block_number(
        ts, start_ts, end_ts, start_block, end_block
    )
    interpolated_block_ts = w3.eth.get_block(interpolated_block_number)["timestamp"]
    diff = interpolated_block_ts - ts
    if diff > threshold:
        return find_block_for_timestamp(
            w3,
            ts,
            start_ts,
            interpolated_block_ts,
            start_block,
            interpolated_block_number,
        )
    elif diff < (-1 * threshold):
        return find_block_for_timestamp(
            w3, ts, interpolated_block_ts, end_ts, interpolated_block_number, end_block
        )
    else:
        return interpolated_block_number, interpolated_block_ts
