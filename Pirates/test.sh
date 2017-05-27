#!/bin/bash
./ppqueue &
for i in 1 2 3 4; do
    ./ppworker &
    sleep 1
done
./lpclient &