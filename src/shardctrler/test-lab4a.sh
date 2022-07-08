#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

runs=$1

for i in $(seq 1 $runs); do
    go test -race > race.log &

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
