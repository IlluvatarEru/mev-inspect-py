#!/bin/bash
# This is a script to analyze MEV profits
# Run with:
# nohup bash scripts/launch_analysis.sh > analysis.out 2>&1 &

run_analysis () {
  # function to run the analysis
  blockFrom=$1
  window=$2
  i=$3
  from=$(($blockFrom + $i*$window))
  to=$(($blockFrom + ($i+1)*$window))
  echo "--"
  echo "rep=$i/$reps"
  echo "from=$from"
  echo "to=  $to"
  ./mev inspect-many $from $to
  ./mev compute-usd-profit $from $to True
}

# Input the pool Id of mev-inspect (can also be found on your TILT interface)
mevInspectPoolId=$(kubectl get pods | sed -n -e '/^mev-inspect-/p' | sed '/^mev-inspect-workers/d' | awk '{print $1}')
# Input the starting and ending blocks you want to run the profit analysis for
blockFrom=$((0))
nBlocks=39000000
blockTo=$((blockFrom+nBlocks))
window=$((390000))
reps=$(((${blockTo}-${blockFrom})/${window}))
startDate=$(date +%s)
startDateFormatted=$(date +"%Y-%m-%d-%H:%M:%S.")$((startDate))
echo "Starting analysis from block=${blockFrom} for ${nBlocks} blocks on the ${startDateFormatted}"
echo "${reps}"
./mev exec alembic upgrade head
for i in $(seq 0 1 $reps)
do
  run_analysis $blockFrom $window $i &
done
wait
./mev analyze-profits $reps True
endDate=$(date +%s)
endDateFormatted=$(date +"%Y-%m-%d-%H:%M:%S.")$(($endDate))
echo "Finished analysis of ${nBlocks} blocks on the ${endDateFormatted}. It took $(( (endDate - startDate)/60 )) minutes."
declare -a file_names=("profit_by_date.csv" "profit_by_block_number.csv" "profit_by_category.csv" "total_usd_profit.csv"  "top_tokens.csv" "distribution_of_profit_by_block_number.png" "distribution_of_profit_by_date.png" "distribution_of_tx_#_by_block_number.png" "distribution_of_tx_#_by_date.png" "profit_distribution.png" "timeseries_of_profit_by_block_number.png" "timeseries_of_profit_by_date.png" "timeseries_of_tx_#_by_block_number.png" "timeseries_of_tx_#_by_date.png" "analyze_profit_failures.csv" "addresses_with_nan_cg_ids.csv")
for fname in "${file_names[@]}"
do
  kubectl cp $mevInspectPoolId:resources/$fname $fname;
done


