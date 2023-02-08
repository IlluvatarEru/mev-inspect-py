#!/bin/bash -l

podID=$(kubectl get pods | sed -n -e '/^mev-inspect-/p' | sed '/^mev-inspect-workers/d' | awk '{print $1}')

declare -a CONCURRENCY_LEVELS=("8")
declare -a STRIDES=("128" "256" "512" "1024")

from=$((34500000))
to=$((from + 1024))

for i in "${CONCURRENCY_LEVELS[@]}"
  do
  for j in "${STRIDES[@]}"
  do
    echo -ne "\nRunning the analysis with ${i} concurrent processes and a stride of ${j}"

    mkdir -p results/$i/$j

    (time (./mev inspect-many $from $to $j $i && ./mev analyze-profit $from $to True)) 2> results/$i/$j/runtime.txt

    declare -a file_names=("profit_by_date.csv" "profit_by_block_number.csv" "profit_by_category.csv" "analyze_profit_failures.csv")

    for fname in "${file_names[@]}"
    do
      kubectl cp $podID:resources/$fname results/$i/$j/$fname;
    done
  done
done
