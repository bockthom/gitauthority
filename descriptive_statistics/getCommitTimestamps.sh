#!/bin/bash

# get commit timestamps for commits done by noreply users
zcat /da?_data/basemaps/gz/aFull.V2412.*.s | grep "@users.noreply.github.com" | ~/lookup/getValues -vV2412 -f a2c | cut -d\; -f2 | ~/lookup/getValues -vV2412 c2dat | cut -d\; -f2
