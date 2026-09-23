#!/bin/bash
# wrk2 R-sweep against the bare-metal hotelReservation frontend.
# usage: dsb_wrk_sweep.sh <tag> [R...]
set -u
TAG=$1; shift
RS=${@:-2000 5000 8000 12000}
WRK=~/DeathStarBench/wrk2/wrk
LUA=~/DeathStarBench/hotelReservation/wrk2/scripts/hotel-reservation/mixed-workload_type_1.lua
for R in $RS; do
  echo "=== [$TAG] R=$R"
  (sleep 6; mpstat 8 1 2>/dev/null | tail -1 | awk '{printf "  [host busy ~= %.1f/18 cores]\n", 18*(100-$NF)/100}') &
  $WRK -D exp -t 8 -c 128 -d 20 -L -s $LUA http://127.0.0.1:5000 -R $R 2>/dev/null \
    | grep -aE "Requests/sec|50.000%|99.000%" | tr '\n' ' '
  echo
  wait
done
