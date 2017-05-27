#!/bin/bash

echo -e "\n\n"
./peering1 DC1 DC2 DC3 &
./peering1 DC2 DC3 DC1 &
./peering1 DC3 DC1 DC2 &