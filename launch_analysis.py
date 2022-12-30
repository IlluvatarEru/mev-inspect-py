import sys

from profit_analysis.analysis import analyze_profit

block_from = int(sys.argv[1])
block_to = int(sys.argv[2])
profit = analyze_profit(block_from, block_to)
print(profit)
