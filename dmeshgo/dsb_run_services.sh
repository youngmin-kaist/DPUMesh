#!/bin/bash
# Start hotelReservation services bare-metal with per-service replicas.
# usage: dsb_run_services.sh <tcp|dmesh> [W] [SPEC]   SPEC="srv-reservation:4,srv-rate:4"
set -u
MODE=$1; W=${2:-8}; SPEC=${3:-}
cd ~/hotelres-dmesh
for b in frontend geo rate profile recommendation user reservation review attractions search; do pkill -9 -f "hotelres-dmesh/bin/$b" 2>/dev/null; done; sleep 2
ENVV="DMESH_TCP_DIRECT=1"
[ "$MODE" = "dmesh" ] && ENVV="DMESH_GRPC=1 DMESH_W=$W"
[ -n "$SPEC" ] && ENVV="$ENVV DMESH_REPLICAS=$SPEC"
mkdir -p /tmp/dsb-logs
declare -A PORT=( [geo]=8083 [rate]=8084 [search]=8082 [profile]=8081 [recommendation]=8085 [user]=8086 [reservation]=8087 [review]=8088 [attractions]=8089 )
reps(){ echo ",$SPEC," | grep -oE ",srv-$1:[0-9]+," | grep -oE "[0-9]+" || echo 1; }
for s in geo rate profile recommendation user review attractions reservation search; do
  N=$(reps $s)
  for r in $(seq 0 $((N-1))); do
    EXTRA="DMESH_PORT_OVERRIDE=$((PORT[$s]+10000*r))"
    [ "$MODE" = "dmesh" ] && EXTRA="DMESH_REPLICA_IDX=$r"
    setsid env $ENVV $EXTRA ~/hotelres-dmesh/bin/$s > /tmp/dsb-logs/$s-$r.log 2>&1 </dev/null &
    sleep 0.3
  done
done
sleep 3
setsid env $ENVV ~/hotelres-dmesh/bin/frontend > /tmp/dsb-logs/frontend.log 2>&1 </dev/null &
for i in $(seq 1 40); do (echo > /dev/tcp/127.0.0.1/5000) 2>/dev/null && break; sleep 1; done
(echo > /dev/tcp/127.0.0.1/5000) 2>/dev/null && echo "frontend up (:5000)" || { echo "FRONTEND-DOWN"; tail -3 /tmp/dsb-logs/frontend.log; exit 1; }
echo "services started (mode=$MODE spec=$SPEC) procs=$(pgrep -cf hotelres-dmesh/bin)"
