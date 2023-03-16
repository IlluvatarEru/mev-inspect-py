#!/bin/bash

for i in {0..10}
do
    scp root@65.109.59.49:mev-inspect-py/usd_profit_$i.csv  usd_profit_$i.csv
done

