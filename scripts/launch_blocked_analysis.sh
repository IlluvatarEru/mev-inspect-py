#!/bin/bash

run_analysis () {
  from=$(($(($1)) + $(($3)) * $(($2))))
  to=$(($(($1)) + $(($3 + 1)) * $(($2))))

  echo "Running analysis for block range [$((from)) - $((to))]"

  ./mev inspect-many $from $to
  ./mev compute-usd-profit $from $to True

  echo "Finished analysis for block range [$((from)) - $((to))]"
}

mev_inspect_pool_id=$(kubectl get pods | sed -n -e '/^mev-inspect-/p' | sed '/^mev-inspect-workers/d' | awk '{print $1}')

block_from=$((10000000))
block_to=$((20000000))
step=$((1000000)) # 1M
window=$((100000)) # 100k

total_number_of_blocks=$(($((block_to)) - $((block_from)))) # 10M

start_date=$(date +%s)
start_date_formatted=$(date +"%Y-%m-%d-%H:%M:%S.")$((start_date))

echo "Starting analysis from block=${block_from} for ${total_number_of_blocks} blocks on the ${start_date_formatted}"
./mev exec alembic upgrade head

for ((i=block_from; i < block_to; i += step))
do
  for ((j = i; j < i + step; j += window))
  do
    run_analysis block_from window $((j / window - $((block_from / window)))) &
  done

  wait
done

echo $((total_number_of_blocks / window))

./mev analyze-profits $((total_number_of_blocks / window)) True

end_date=$(date +%s)
end_date_formatted=$(date +"%Y-%m-%d-%H:%M:%S.")$((end_date))
echo "Finished analysis of ${total_number_of_blocks} blocks on the ${end_date}. It took $(((end_date_formatted - start_date) / 60)) minutes."

declare -a file_names=("profit_by_date.csv" "profit_by_block_number.csv" "profit_by_category.csv" "total_usd_profit.csv"  "top_tokens.csv" "distribution_of_profit_by_block_number.png" "distribution_of_profit_by_date.png" "distribution_of_tx_#_by_block_number.png" "distribution_of_tx_#_by_date.png" "profit_distribution.png" "timeseries_of_profit_by_block_number.png" "timeseries_of_profit_by_date.png" "timeseries_of_tx_#_by_block_number.png" "timeseries_of_tx_#_by_date.png" "analyze_profit_failures.csv" "addresses_with_nan_cg_ids.csv")
for fname in "${file_names[@]}"
do
  kubectl cp $mev_inspect_pool_id:resources/$fname $fname
done
