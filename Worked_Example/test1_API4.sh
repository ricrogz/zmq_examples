#!/bin/bash

echo -e "\n\n"
./peering1_API4 DC1 DC2 DC3 &
./peering1_API4 DC2 DC3 DC1 &
./peering1_API4 DC3 DC1 DC2 &