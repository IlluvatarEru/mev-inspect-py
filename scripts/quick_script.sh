mev_inspect_pool_id=$(kubectl get pods | sed -n -e '/^mev-inspect-/p' | sed '/^mev-inspect-workers/d' | awk '{print $1}')

./mev analyze-profits 194 True

declare -a file_names=("profit_by_date.csv" "profit_by_block_number.csv" "profit_by_category.csv" "total_usd_profit.csv"  "top_tokens.csv" "distribution_of_profit_by_block_number.png" "distribution_of_profit_by_date.png" "distribution_of_tx_#_by_block_number.png" "distribution_of_tx_#_by_date.png" "profit_distribution.png" "timeseries_of_profit_by_block_number.png" "timeseries_of_profit_by_date.png" "timeseries_of_tx_#_by_block_number.png" "timeseries_of_tx_#_by_date.png" "analyze_profit_failures.csv" "addresses_with_nan_cg_ids.csv")
for fname in "${file_names[@]}"
do
  kubectl cp $mev_inspect_pool_id:resources/$fname $fname
done