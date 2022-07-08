#!/usr/bin/env bash

rm res -rf
mkdir res

for ((i = 0; i < 50; i++))
do
    mkdir ./res/$i
    for ((c = $((i*10)); c < $(( (i+1)*10)); c++))
    do
        time=$(date "+%Y-%m-%d %H:%M:%S")
        echo $time '---' BEGIN $c TESTING TRIAL

        (go test -run 2 -race) &> ./res/$i/$c.log &
        sleep 2m
    done

    if grep -nr "WARNING.*" res; then
        echo "WARNING: DATA RACE"
        exit 1
    fi
    if grep -nr "FAIL.*raft.*" res; then
        echo "found fail"
        exit 1
    fi
done

time=$(date "+%Y-%m-%d %H:%M:%S")
echo $time '---' PASSED ALL $i TESTING TRIALS
