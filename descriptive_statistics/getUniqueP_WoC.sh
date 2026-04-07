#!/bin/bash

# get number of unique deforked projects in WoC
zcat /da?_data/basemaps/gz/P2pFull.V2412.s | cut -d\; -f1 | sort -u | wc -l

# get number of unique deforked GitHub projects in WoC
zcat /da?_data/basemaps/gz/P2pFull.V2412.s | cut -d\; -f1 | grep -v "/" | sort -u | wc -l

# get number of deforked projects that have at least one noreply email
wc -l WoC_githubNoreplyUsers_uniqueP.csv

# get number of deforked GitHub projects that have at least one noreply email
grep -v "/" WoC_githubNoreplyUsers_uniqueP.csv | wc -l
