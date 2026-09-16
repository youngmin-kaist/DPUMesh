#!/bin/bash
# True-L7 (h2-terminated) core sweep. usage: sb-l7.sh <CORES> <W> <M> [curl|load]
# M ingress pairs (port 38080+i, dst 10.0.0.(1+i)) + M backend bridges, K=M.
set -u
RMODE=$1; M=$2; MODE=${3:-load}; CORES=1; W=1
cd $HOME/DPUMesh/dmesh-router-cpp
LOG=$HOME/.claude/jobs/3aac3b67/tmp/prof
HOST=192.168.100.1
step(){ echo "[$(date +%H:%M:%S)] $*"; }
die(){ step "FATAL: $*"; cleanup; exit 1; }
cleanup(){
  for p in $(pgrep -x dmesh-router-cp); do sudo -n kill $p; done
  timeout 20 ssh $HOST 'pkill -x dpumesh; true' </dev/null >/dev/null 2>&1
}

step "0. clean (CORES=$CORES W=$W M=$M)"
cleanup; sleep 12
step "1. dmesh-router-cpp mode=$RMODE (pinned to core 15)"
DMESH_ROUTER_DEV_PCI=03:00.1 DMESH_ROUTER_REP_PCI=0b:00.1 DMESH_ROUTER_MODE=$RMODE DMESH_ROUTER_POLICY_PATHS=${POLICY_PATHS:-/ok} \
    setsid sudo -n -E env taskset -c 15 ${ROUTER_BIN:-./build/dmesh-router-cpp} > $LOG/dproxy.log 2>&1 </dev/null &
for i in $(seq 1 25); do grep -aq "comch server started" $LOG/dproxy.log 2>/dev/null && break; sleep 1; done
grep -aq "comch server started" $LOG/dproxy.log || die "router did not start"
step "   router up: $(grep -a 'comch server started' $LOG/dproxy.log | sed 's/.*mode=//')"

step "2. $M backend bridges"
timeout 15 ssh $HOST "rm -f /tmp/ym_h2_*.log /tmp/ym_in_*.log /tmp/ym_be_*.log" </dev/null >/dev/null 2>&1
timeout 8 ssh $HOST "cd ~/bf-workspace/DPUMesh && for j in \$(seq 0 $((M-1))); do setsid env DMESH_BACKEND_CONNECT=127.0.0.1:8086 DMESH_DST_IP=10.0.0.\$((1+j)) DMESH_DST_PORT=8086 DMESH_SERVER_IDX=\$((j % $W)) ./build/dpumesh -p 0b:00.1 -t 1 -d 1 > /tmp/ym_be_\$j.log 2>&1 </dev/null & done; exit 0" </dev/null >/dev/null 2>&1
for i in $(seq 1 30); do [ "$(grep -ac "Push channel ready (mode 1)" $LOG/dproxy.log 2>/dev/null)" -ge "$M" ] && break; sleep 1; done
B=$(grep -ac "Push channel ready (mode 1)" $LOG/dproxy.log); [ "$B" -ge "$M" ] || die "backend channels $B/$M"
step "   $B backend channels ready (nginx 60s window open)"

step "3. $M ingress bridges (listen-first; channel attaches on client connect)"
timeout 8 ssh $HOST "cd ~/bf-workspace/DPUMesh && for i in \$(seq 0 $((M-1))); do rm -f /tmp/ym_in_\$i.log; setsid env DMESH_PUSH_BRIDGE_PORT=\$((38080+i)) DMESH_DST_IP=10.0.0.\$((1+i)) DMESH_DST_PORT=8086 DMESH_SERVER_IDX=\$((i % $W)) ./build/dpumesh -p 0b:00.1 -t 1 -d 1 > /tmp/ym_in_\$i.log 2>&1 </dev/null & done; exit 0" </dev/null >/dev/null 2>&1
LIS=0; for t in $(seq 1 20); do LIS=$(timeout 15 ssh $HOST "grep -al 'push-ingress: listening' /tmp/ym_in_*.log 2>/dev/null | wc -l" </dev/null 2>/dev/null | tr -dc 0-9); [ "${LIS:-0}" -ge "$M" ] && break; sleep 2; done
[ "${LIS:-0}" -ge "$M" ] || die "ingress listening $LIS/$M"
step "   $LIS/$M listening"

if [ "$MODE" = "curl" ]; then
    step "4. preflight: 1 request (port 38080 only; sequential closes risk the teardown segfault)"
    timeout 30 ssh $HOST "echo -n '  port 38080: '; curl -s -o /dev/null -w '%{http_code} size=%{size_download} in %{time_total}s\n' --max-time 8 --http2-prior-knowledge http://127.0.0.1:38080${CURL_PATH:-/}" </dev/null 2>&1
    sleep 2; grep -a "relay stats" $LOG/dproxy.log | tail -1 | sed 's/.*relay stats/   relay stats/' | cut -c1-260
    grep -aE "WARN" $LOG/dproxy.log | head -3
    step "PREFLIGHT DONE"; cleanup; exit 0
fi

PID=$(pgrep -x dmesh-router-cp | head -1); step "4. h2load x$M (timed 16s, warmup 3s, -c1 -m300) + perf on pid $PID (window: t+5..t+13)"
M0=$(curl -s --max-time 5 http://127.0.0.1:4991/metrics | awk '/^request_total\{direction="outbound"/{s+=$NF} END{printf "%d", s}')
PIDS=""; (sleep 5; sudo -n perf stat -e cycles,instructions,cache-misses,branch-misses,context-switches,cpu-migrations,page-faults -p $PID -- sleep 8 > $LOG/perfstat.txt 2>&1) & PIDS="$PIDS $!"
(sleep 5; sudo -n perf record -F 499 --call-graph dwarf,16384 -p $PID -o $LOG/perf.data -- sleep 8 > $LOG/perfrec.txt 2>&1) & PIDS="$PIDS $!"
(sleep 5; mpstat -P ALL 8 1 > $LOG/mpstat.txt 2>&1) & PIDS="$PIDS $!"
(sleep 5; grep -a "relay stats" $LOG/dproxy.log | tail -1 | grep -oE "header_blocks=[0-9]+" | tr -dc 0-9 > $LOG/req_t5.txt; sleep 8; grep -a "relay stats" $LOG/dproxy.log | tail -1 | grep -oE "header_blocks=[0-9]+" | tr -dc 0-9 > $LOG/req_t13.txt) & PIDS="$PIDS $!"
timeout 120 ssh $HOST "for i in \$(seq 0 $((M-1))); do h2load --duration=16 --warm-up-time=3 -c1 -m300 ${H2EXTRA:-} http://127.0.0.1:\$((38080+i))/ok > /tmp/ym_h2_\$i.log 2>&1 & done; (sleep 6; mpstat -P ALL 8 1 | awk '/Average/ && \$2!=\"CPU\" {b+=100-\$NF} END{printf \"[host] busy cores during window ~= %.1f / 16\\n\", b/100}'; pidstat -u 8 1 -C 'h2load|dpumesh' | awk '/Average/ && \$NF!=\"Command\" {c[\$NF]+=\$8} END{for(k in c) printf \"[host] %s %.2f cores\\n\", k, c[k]/100}') & wait; python3 -c \"
import re,glob
t=0; n=0; ok=0
for f in sorted(glob.glob('/tmp/ym_h2_*.log')):
    s=open(f).read()
    m=re.search(r'finished in [^,]+, ([0-9.]+) req/s', s)
    d=re.search(r'requests: ([0-9]+) total', s)
    if m: t+=float(m.group(1)); ok+=1
    if d: n+=int(d.group(1))
print(f'TOTAL {t:.0f} req/s over {ok} conns, {n} requests')\"; grep -hE 'finished in|requests:|status codes|traffic:' /tmp/ym_h2_0.log | sed 's/^/  [h2_0] /'" </dev/null 2>&1
wait $PIDS
echo "[window] header_blocks in perf window (relay only): $(( $(cat $LOG/req_t13.txt 2>/dev/null || echo 0) - $(cat $LOG/req_t5.txt 2>/dev/null || echo 0) )) over 8s"
echo "[mpstat DPU] $(awk '/Average/ && $2=="15" {printf "core15 busy %.1f%% (usr %.1f sys %.1f irq %.1f soft %.1f)", 100-$NF, $3, $5, $7, $8}' $LOG/mpstat.txt)"
grep -E "cycles|instructions|misses|switches|migrations|faults|elapsed" $LOG/perfstat.txt | sed 's/^/[perf stat] /'
step "5. router stats (last line)"
grep -a "relay stats" $LOG/dproxy.log | tail -1 | sed 's/.*relay stats/   relay stats/'
grep -a "datapath stats" $LOG/dproxy.log | tail -1 | cut -c1-150 | sed 's/^/   /'
step "done"; cleanup
