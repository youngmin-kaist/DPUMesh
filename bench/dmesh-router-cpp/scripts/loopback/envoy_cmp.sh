#!/bin/bash
# DPU loopback TCP engine comparison: h2load x4 (cores 8-11) -> proxy (core 15) -> nginx (cores 12-13)
D=/tmp/dmesh-envoy-cmp; P=$HOME/DPUMesh/linkerd2-proxy
MODES=${MODES:-"envoy nghttpx linkerd linkerd-nghttp2"}; DUR=${DUR:-16}; NPROC=${NPROC:-4}
stop_all(){ for p in $(pgrep -x envoy) $(pgrep -x nghttpx) $(pgrep -x linkerd2-proxy); do sudo -n kill $p 2>/dev/null; done; pkill -x mock-identity; pkill -x mock-policy; pkill -f "[m]ock-destinatio"; sudo -n nginx -c $D/nginx.conf -s quit 2>/dev/null; sleep 2; }
stop_all
for M in $MODES; do
  echo "##### $M $(date +%T)  [contamination check: rustc=$(pgrep -c -x rustc) cargo=$(pgrep -c -x cargo) other-proxy=$(pgrep -c -x linkerd2-proxy)]"
  sudo -n taskset -c 12-13 nginx -c $D/nginx.conf; sleep 1
  case $M in
    envoy)   PORT=10000; setsid taskset -c 15 $D/envoy -c $D/envoy.yaml --concurrency 1 -l warn --base-id 7 > $D/envoy.log 2>&1 & PROXY_PID=$! ;;
    nghttpx) PORT=10001; setsid taskset -c 15 nghttpx --conf=/dev/null --workers=1 --frontend="127.0.0.1,10001;no-tls" --backend="127.0.0.1,8086;;proto=h2" --frontend-http2-max-concurrent-streams=1024 --backend-http2-max-concurrent-streams=1024 --log-level=WARN > $D/nghttpx.log 2>&1 & PROXY_PID=$! ;;
    linkerd|linkerd-nghttp2) PORT=4140; cd $P; source scripts/dev-proxy-env.sh >/dev/null 2>&1
      export LINKERD2_PROXY_IDENTITY_SVC_ADDR=127.0.0.1:8088 LINKERD2_PROXY_DESTINATION_SVC_ADDR=127.0.0.1:8089 LINKERD2_PROXY_POLICY_SVC_ADDR=127.0.0.1:8087 LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:4140 LINKERD2_PROXY_POLICY_WORKLOAD=local-dev LINKERD2_PROXY_DESTINATION_PROFILE_NETWORKS=127.0.0.0/24 MOCK_DESTINATION_ADDR=127.0.0.1:8089 MOCK_DESTINATION_BACKEND=127.0.0.1:8086 MOCK_POLICY_ADDR=127.0.0.1:8087 MOCK_POLICY_BACKEND=127.0.0.1:8086 LINKERD2_PROXY_DOCA_REP_PCI_ADDR=0b:00.1 LINKERD2_PROXY_LOG=warn LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:4991 LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:5143
      setsid ./target/release/mock-identity > $D/mock-identity.log 2>&1 & setsid ./target/release/mock-destination > $D/mock-destination.log 2>&1 & setsid ./target/release/mock-policy > $D/mock-policy.log 2>&1 &
      for p in 8087 8088 8089; do for i in $(seq 1 50); do (echo > /dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.2; done; done
      NG=""; [ $M = linkerd-nghttp2 ] && NG="DMESH_NGHTTP2=1"
      setsid sudo -n -E env DMESH_SHARDED=1 DMESH_NUM_WORKERS=1 LINKERD2_PROXY_CORES=1 $NG taskset -c 15 $D/linkerd2-proxy.native > $D/linkerd.log 2>&1 & PROXY_PID=""
      cd $D ;;
  esac
  for i in $(seq 1 40); do (echo > /dev/tcp/127.0.0.1/$PORT) 2>/dev/null && break; sleep 0.5; done
  sleep 2; echo "  preflight: $(curl -s -o /dev/null -w '%{http_code}' --http2-prior-knowledge http://127.0.0.1:$PORT/ok)"
  PID=${PROXY_PID:-$(pgrep -x linkerd2-proxy | head -1)}; echo "  pid=$PID"
  ( sleep 5; sudo -n perf stat -e cycles,instructions,context-switches -p $PID -- sleep 8 > $D/perfstat_$M.txt 2>&1 ) &
  ( sleep 5; mpstat -P 15 8 1 | awk '/Average/ {printf "  core15 busy %.1f%% (sys %.1f)\n", 100-$NF, $5}' ) &
  LP=""; for i in $(seq 0 $((NPROC-1))); do taskset -c $((8+i)) h2load --duration=$DUR --warm-up-time=3 -c1 -m300 http://127.0.0.1:$PORT/ok > $D/h2_${M}_$i.log 2>&1 & LP="$LP $!"; done; wait $LP; sleep 1
  TOT=$(grep -h "finished in" $D/h2_${M}_*.log | grep -oE "[0-9.]+ req/s" | awk '{s+=$1} END{printf "%.0f", s}'); echo "  TOTAL $TOT req/s ($NPROC conns)"; grep -h "status codes" $D/h2_${M}_0.log | sed 's/^/  [h2_0] /'
  CYC=$(grep -oE "^ +[0-9,]+ +cycles" $D/perfstat_$M.txt | tr -d ', ' | grep -oE "^[0-9]+"); INS=$(grep -oE "^ +[0-9,]+ +instructions" $D/perfstat_$M.txt | tr -d ', ' | grep -oE "^[0-9]+"); CS=$(grep -oE "^ +[0-9,]+ +context-switches" $D/perfstat_$M.txt | tr -d ', ' | grep -oE "^[0-9]+")
  echo "  cycles=$CYC instr=$INS ctxsw=$CS => cycles/req=$(( CYC / (TOT*8) )) instr/req=$(( INS / (TOT*8) ))"
  stop_all
done; echo "##### done $(date +%T)"
