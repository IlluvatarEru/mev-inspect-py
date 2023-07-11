#!/bin/bash

# This script will copy all the profits computed from the kubernetes cluster to the local host.

mevInspectPoolId=$(kubectl get pods | sed -n -e '/^mev-inspect-/p' | sed '/^mev-inspect-workers/d' | awk '{print $1}')

for i in {0..10}
do
    kubectl cp $mevInspectPoolId:resources/usd_profit_$i.csv  usd_profit_$i.csv
done
