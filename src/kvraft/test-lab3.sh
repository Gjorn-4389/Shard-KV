#!/usr/bin/env bash

for i in $(seq 500); do
    go test -run TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B -race > race.log &

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
