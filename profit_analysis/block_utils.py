import datetime
from time import sleep

import pandas as pd
from profit_analysis.column_names import BLOCK_KEY, TIMESTAMP_KEY
from profit_analysis.web3_provider import W3


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
    while trials < 3:
        try:
            trials += 1
            block_info = W3.w3_provider.eth.get_block(int(block))
            ts = block_info[TIMESTAMP_KEY]
            dt = datetime.datetime.fromtimestamp(ts)
            return dt
        except Exception as e:
            print(f"Error, retrying {e}")
            sleep(0.05)
    W3.rotate_rpc_url()
    return get_block_timestamp(block)
