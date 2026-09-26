# gRPC-go 64B echo — 16 ARM cores, 4 conn/core, EU partition 해제 후

**3회 모두 성공했다. 중앙값은 200,568.6 RPC/s**이며 범위는 195,471.1–200,610.0 RPC/s다. 측정 구간의 총 5,966,497 RPC 모두 payload 검증을 통과했고 RPC 오류·재연결은 0건이었다. 이전 세 번의 연결 초기화 실패는 이번 실행에서 재현되지 않았다.

실행 전/후 모두 `dpaeumgmt partition query` 결과는 **EU partition 0개**였다. Host PF partition만 해제됐다고 가정하지 않았으며, 이전 SF용 partition도 현재 조회에는 없다. 칩은 190 EU이고, 이번 데이터 경로는 DPU 소유 DPA thread 128개, host 소유 DPA thread 0개를 사용한다. 별도 EU group도 0개다.

## 결과

| 반복 | RPC/s | 완료 RPC | 최대 worker p99 (ms) | Host client busy | Host server busy |
| --- | --- | --- | --- | --- | --- |
| 1 | 200,568.6 | 2,005,686 | 67.985 | 97.15% | 99.05% |
| 2 | 195,471.1 | 1,954,711 | 101.061 | 96.88% | 98.87% |
| 3 | 200,610.0 | 2,006,100 | 66.692 | 97.22% | 99.11% |

Latency는 각 실행에서 16개 worker의 p99 중 최댓값이며 전체 RPC를 합친 p99가 아니다. Host busy는 각 8-core pool의 `/proc/stat` tick 합계로 계산했다.

이전 12코어·4 conn/core의 175,974.5 RPC/s보다 **14.0% 높다**. 다만 이전 측정은 EU partition 두 개가 설정된 상태였으므로 ARM core 수만 바꾼 비교는 아니다. 이전 실패에는 처리량 수치가 없으므로 해제 전/후의 W16/K4 성능 향상률은 계산하지 않는다.

## CPU

| 반복 | 각 worker + helper CPU (%) | 각 ARM core busy (%) | Proxy 총 CPU (%) |
| --- | --- | --- | --- |
| 1 | 92.98–99.78 | 99.90–99.90 | 1574.93 |
| 2 | 91.17–98.27 | 99.60–99.80 | 1519.94 |
| 3 | 94.44–99.64 | 98.70–99.90 | 1570.51 |

100%는 코어 하나 전체 사용량이다. Worker CPU는 해당 이름을 상속한 DOCA helper를 포함한다. 전체 반복에서 worker는 91.17–99.78%, 실제 ARM core busy는 98.70–99.90%였다. Host client/server busy 중앙값은 **97.15% / 99.05%**다.

Host server의 8개 core가 거의 포화되어 있어 ARM만의 처리량 한계로 해석할 수 없다. DPU도 16개 core를 모두 사용하므로 OS·Codex·다른 작업과 공유한다. 기록한 controller CPU는 최대 한 코어의 0.40%, direct child와 mock CPU는 0%였다. 모든 차이를 controller 탓으로 돌릴 수는 없다.

`DMESH_BUSY_POLL=1`이며 backend만 연결하고 RPC를 보내지 않은 별도 3초 idle 샘플도 16개 core 모두 100% busy였다. CPU 100% 자체가 유효 RPC 처리 연산의 포화를 뜻하지 않는다. 공식 실행에는 perf를 사용하지 않았다. `sched_schedstats=0`이어서 runqueue wait는 null로 기록했다.

각 worker/helper CPU의 3회 중앙값:

| Worker | ARM CPU | CPU (%) |
| --- | --- | --- |
| 0 | 15 | 98.48 |
| 1 | 14 | 96.24 |
| 2 | 13 | 99.48 |
| 3 | 12 | 99.44 |
| 4 | 11 | 92.98 |
| 5 | 10 | 98.04 |
| 6 | 9 | 94.47 |
| 7 | 8 | 95.27 |
| 8 | 7 | 98.44 |
| 9 | 6 | 98.48 |
| 10 | 5 | 98.74 |
| 11 | 4 | 98.44 |
| 12 | 3 | 99.44 |
| 13 | 2 | 98.34 |
| 14 | 1 | 96.54 |
| 15 | 0 | 98.74 |

## 조건과 검증

- `dpu-dma`, gRPC-go unary raw-codec echo, 요청과 응답 application payload 각각 64B. HTTP/2/gRPC framing은 별도다.
- DPU ARM worker 16개, worker당 client connection 4개와 backend connection 4개. 총 client 64개 + backend 64개 = 128개 flow다. Connection당 동시 RPC 64개, 총 4,096개다.
- Host는 x86 jet1 한 대다. Client 16개 process가 CPU0–7, server 16개 process가 CPU8–15를 공유하며 process당 GOMAXPROCS=8이다. 각 process는 해당 `DPUMesh{i}` worker에 연결한다.
- Proxy/runtime/affinity, service registry, backend initial/max pool=4, 바이너리, 5초 RPC deadline은 이전 실험과 같다. Source 수정이나 rebuild는 하지 않았다.
- 병렬 client preflight 후 공통 start-file barrier, warmup 3초, 측정 10초. 세 반복 모두 모든 worker의 measurement_start/end가 정확히 같고 native dial은 connection당 정확히 1회다. Client process는 반복마다 새로 시작하며 server/proxy는 유지했다.
- 128개 flow 연결 성공 후 측정했다. Proxy 로그에 DOCA 오류나 FlexIO RPC timeout은 없다. Server 16개 모두 exit=0, proxy exit=0이며 종료 후 DPA processes=0을 확인했다. Mock들은 scoped SIGTERM으로 정리했다.
- 이전 환경의 118개 active flow 뒤 초기화 timeout과 달리, partition 해제 후 같은 바이너리에서 128개 연결과 3회 부하가 성공했다. 이는 이전 실패의 EU 가용량 부족 해석을 강하게 뒷받침한다. Partition 2개가 모두 없어졌으므로 개별 partition 하나의 효과를 분리한 실험은 아니다.

## 자료

- 원본과 검증 JSON: `bench-results/2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions.json`
- [반복별 CSV](2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions.csv)
- 환경·해시·CPU 설정: `bench-results/2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions-evidence/experiment.json`, 정상 종료 기록: `bench-results/2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions-evidence/stop-result.json`, proxy 로그: `bench-results/2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions-evidence/proxy.log`.
- 전체 로그·스크립트 원본: `/tmp/dmesh-grpc-w16k4-unpartitioned-20260926`. Host 로그는 `host/w16-k4/`, DPU 로그는 `w16/`에 있다.
- [이전 partition 설정의 1~16코어 결과와 W16/K4 실패 기록](2026-09-26_grpc-go-64b-arm-scaling.md). 기존 raw 결과는 그대로 보존했다.

바이너리 SHA-256 (이전 측정과 모두 동일):

```text
a25fa454bb30d25fcb1263653461beda13d756ddf6d5f03ff6339075a3764486  linkerd2-proxy/target/release/linkerd2-proxy
4c5b84fdd524dfee19d2e3b671bbac9518320ec377b5438382d603ad5a8b5e2f  src/transport/build/device/dpa_kernel.a
727ec2ab5dc427140da2c5db9e9f1779a464d84d770f774d19febe84bdc651af  /home/youngmin/DPUMesh/build/lib/libdpumesh.so.5
f5c7b85e496e1d49f30d4633fdd1a776f2ac59921eb3244aa87f568b86c1589f  /home/youngmin/DPUMesh/integrations/grpc/go/bin/channel-bench-scale
```

## 공개 자료의 범위

링크로 연결한 보고서·CSV·그림·작은 결과 JSON은 Git에 포함한다. 코드로 표시한 원본 경로는 Git 추적 대상이 아니며, `/tmp`는 실험 당시 경로라 현재 존재를 보장하지 않는다. 실제 로컬 보존 파일은 [자료 보존 안내](README.md#자료-보존)를 참조한다.
