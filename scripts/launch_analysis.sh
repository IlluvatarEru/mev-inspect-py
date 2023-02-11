#!/bin/bash
# This is a script to analyze MEV profits
# Run with:
# sleep(900); ./mev exec alembic upgrade head; sleep(60); nohup bash scripts/launch_analysis.sh > analysis.out 2>&1 &
# Input the pool Id of mev-inspect (can also be found on your TILT interface)
mevInspectPoolId=$(kubectl get pods | sed -n -e '/^mev-inspect-/p' | sed '/^mev-inspect-workers/d' | awk '{print $1}')
# Input the starting and ending blocks you want to run the profit analysis for
blockFrom=$((34500000))
nBlocks=3000
blockTo=$((blockFrom+nBlocks))
window=$((1000))
reps=$(((${blockTo}-${blockFrom})/${window}))
startDate=$(date +%s)
startDateFormatted=$(date +"%Y-%m-%d-%H:%M:%S.")$((startDate))
echo "Starting analysis from block=${blockFrom} for ${nBlocks} blocks on the ${startDateFormatted}"
echo "${reps}"
./mev exec alembic upgrade head
for i in $(seq 0 1 $reps)
do
  from=$(($blockFrom + $i*$window))
  to=$(($blockFrom + ($i+1)*$window))
  fname="total_profit_by_block_$i.csv"
  echo "--"
  echo "rep=$i/$reps"
  echo "from=$from"
  echo "to=  $to"
  echo "$fname"
  ./mev inspect-many $from $to
  ./mev compute-usd-profit $from $to True
done
./mev analyze-profits $reps True
endDate=$(date +%s)
endDateFormatted=$(date +"%Y-%m-%d-%H:%M:%S.")$(($endDate))
echo "Finished analysis of ${nBlocks} blocks on the ${endDateFormatted}. It took $(( (endDate - startDate)/60 )) minutes."
declare -a file_names=("profit_by_date.csv" "profit_by_block_number.csv" "profit_by_category.csv" "total_usd_profit.csv"  "top_tokens.csv" "profit_distribution.png" "analyze_profit_failures.csv" "addresses_with_nan_cg_ids.csv")
for fname in "${file_names[@]}"
do
  kubectl cp $mevInspectPoolId:resources/$fname $fname;
done