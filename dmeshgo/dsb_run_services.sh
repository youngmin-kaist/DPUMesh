#!/bin/bash
# Start hotelReservation services bare-metal. usage: dsb_run_services.sh <tcp|dmesh> [W]
set -u
MODE=$1; W=${2:-8}
cd ~/hotelres-dmesh
pkill -f "hotelres-dmesh/bin/" 2>/dev/null; pkill -f "\./bin/frontend" 2>/dev/null; for b in geo rate profile recommendation user reservation review attractions search; do pkill -f "\./bin/$b" 2>/dev/null; done; sleep 2
ENVV="DMESH_TCP_DIRECT=1"
[ "$MODE" = "dmesh" ] && ENVV="DMESH_GRPC=1 DMESH_W=$W"
mkdir -p /tmp/dsb-logs
for s in geo rate profile recommendation user reservation review attractions search; do
  setsid env $ENVV ~/hotelres-dmesh/bin/$s > /tmp/dsb-logs/$s.log 2>&1 </dev/null &
  sleep 0.4
done
sleep 3
setsid env $ENVV ~/hotelres-dmesh/bin/frontend > /tmp/dsb-logs/frontend.log 2>&1 </dev/null &
for i in $(seq 1 40); do (echo > /dev/tcp/127.0.0.1/5000) 2>/dev/null && break; sleep 1; done
(echo > /dev/tcp/127.0.0.1/5000) 2>/dev/null && echo "frontend up (:5000)" || { echo "FRONTEND-DOWN"; tail -3 /tmp/dsb-logs/frontend.log; exit 1; }
echo "services started (mode=$MODE)"
