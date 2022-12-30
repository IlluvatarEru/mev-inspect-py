#!/bin/bash
blockFrom=$((34500000))
blockTo=$((34800000))
window=$((100))
reps=$(((${blockTo}-${blockFrom})/${window}))
reps=0
echo "${reps}"
for i in $(seq 0 1 $reps)
do
    from=$(($blockFrom + $i*$window))
    to=$(($blockFrom + ($i+1)*$window))
    echo "--"
    echo "rep=$i/$reps"
    echo "from=$from"
    echo "to=  $to"
    #./mev inspect-many $from $to
done
echo "Starting profit analysis"
python3 launch_analysis.py $from $to
