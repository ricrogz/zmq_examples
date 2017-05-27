#!/bin/bash

echo -e "\n\n"
./peering3_API4 c1 c2 c3 c4 &
./peering3_API4 c2 c3 c4 c1 &
./peering3_API4 c3 c4 c1 c2 &
./peering3_API4 c4 c1 c2 c3 &
