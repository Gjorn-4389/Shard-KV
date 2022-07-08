#!/usr/bin/env bash

time=$(date "+%Y-%m-%d %H:%M:%S")
echo $time '---' Test BEGIN

for i in $(seq 500); do
    go test -run TestConcurrent2 -race > race.log &
    # go test -race > race.log &

    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    time=$(date "+%Y-%m-%d %H:%M:%S")
    echo $time '---' PASSED TRIAL $i TESTS
    
done

time=$(date "+%Y-%m-%d %H:%M:%S")
echo $time '---' PASSED ALL $i TESTING TRIALS
