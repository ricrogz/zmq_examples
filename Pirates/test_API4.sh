#!/bin/bash
./ppqueue_API4 &
for i in 1 2 3 4; do
    ./ppworker_API4 &
    sleep 1
done
./lpclient_API4 &