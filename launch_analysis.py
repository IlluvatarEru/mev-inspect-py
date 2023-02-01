import sys

from profit_analysis.analysis import analyze_profit, compute_usd_profit

from mev_inspect.db import get_inspect_session

block_from = int(sys.argv[1])
block_to = int(sys.argv[2])
inspect_db_session = get_inspect_session()
profit = compute_usd_profit(inspect_db_session, block_from, block_to, True)
analyze_profit(profit)
