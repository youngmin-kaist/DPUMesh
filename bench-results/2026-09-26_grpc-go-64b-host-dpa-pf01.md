# gRPC-go 64B echo: host-dpa PF0/PF1 분산 — 2026-09-26

**5/27회 유효, INCOMPLETE.** 측정 구간 완료 RPC는 6,089,369건이다. 공식 조건의 실제 실행 시도는 17회이며 실패 이력 14개에는 별도 최초 control 실패와 해결된 harness 정리 오류도 포함한다. 표의 3/3만 예정된 3회가 모두 통과한 결과다. 1/3 또는 2/3 수치는 일부 반복의 관측값이며 완료된 3회 벤치마크로 취급하지 않는다. 실패 실행과 최초 실패 이력은 별도로 보존한다.

## 수정 전후 control과 바이너리

최초 W1/K1 control은 기존 host library로 실행했고 client PF1에서 Comch와 같은 device를 mmap에 두 번 등록하여 DOCA_ERROR_ALREADY_EXIST로 실패했다. 측정 구간에 도달하지 않았으며 고코어 DPA process 한계와 별개다. 수정 후 control은 별도 control-fixed 원본으로 보존하며 아래 공식 W9/12/16 수치에 합산하지 않는다.

이번 source 변경: src/transport/host/channel.c: skip duplicate mmap registration when Comch and DPA share a DOCA device; isolated host library override; original binaries retained. Host library override: `/tmp/dmesh-grpc-host-dpa-pf01-20260926/host-build/lib`. 원래 host library와 새 isolated library의 SHA는 아래 자료에 각각 표시했다. 실제 로딩 library maps 증거: `/tmp/dmesh-grpc-host-dpa-pf01-20260926/control-fixed/loaded-host-library.json`.

| Control | 유효/실제 시도 | RPC/s 중앙값 | RPC/s 최소–최대 | 공식 합계 포함 |
| --- | --- | --- | --- | --- |
| 수정 전 W1/K1 | 0/1 | N/A | N/A | 아니오 |
| 수정 후 W1/K1 | 3/3 | 15,150.2 | 15,116.7–15,423.9 | 아니오 |

## 조건과 처리량

요청/응답 각각64B gRPC-go raw-codec unary echo, connection당 동시 RPC64개, warmup3초·측정10초다. W는 ARM worker 수, K는 worker당 client connection 및 backend pool 크기다. Client/server 프로세스는 각각 host CPU0–7/8–15를 공유하며 GOMAXPROCS=8이다. 모든 worker의 공통 시작 barrier와 동일한 10초 구간, payload, native dial1회/connection, RPC 오류0·재연결0, CPU/affinity 및 server/proxy 정상 종료를 검사했다.

| W | K | 유효/예정 반복 | 유효/실제 시도 | RPC/s 중앙값 | RPC/s 최소–최대 | 최대 worker p99 중앙값(ms) |
| --- | --- | --- | --- | --- | --- | --- |
| 9 | 1 | 3/3 | 3/3 | 103,770.1 | 103,627.3–104,122.8 | 12.447 |
| 9 | 2 | 0/3 | 0/3 | N/A | N/A | N/A |
| 9 | 4 | 2/3 | 2/5 | 148,708.4 | 148,686.7–148,730.0 | 29.214 |
| 12 | 1 | 0/3 | 0/1 | N/A | N/A | N/A |
| 12 | 2 | 0/3 | 0/1 | N/A | N/A | N/A |
| 12 | 4 | 0/3 | 0/1 | N/A | N/A | N/A |
| 16 | 1 | 0/3 | 0/1 | N/A | N/A | N/A |
| 16 | 2 | 0/3 | 0/1 | N/A | N/A | N/A |
| 16 | 4 | 0/3 | 0/1 | N/A | N/A | N/A |

**지연은 반복별 max(worker p99)의 중앙값이다. 전체 RPC를 합친 pooled p99가 아니다.** 초기화·preflight 실패는 처리량0으로 계산하지 않는다. W9/K4의 중앙값은 정상 종료한 2회만의 값이며 3회 성공 결과가 아니다. K2는 3회 모두, K4는 전체 5회 중 3회 native teardown에서 실패했다. 추가 retry를 거쳐도 오류가 재현되어 반복을 종료했으며, 성공할 때까지 반복한 결과로 제시하지 않는다.

## Teardown 실패 실행의 RPC 구간 참고값

다음 실행은 9개 worker 모두 동일한 10초 구간을 마쳤고 payload64B·connection당 동시RPC64·native dial1회·RPC 오류0·재연결0과 완료 수를 검증했다. 이후 native close에서 bad message(EBADMSG)가 반환되어 실행 전체는 실패다. **공식 처리량·중앙값·완료 RPC 합계에는 넣지 않았다.** 원인은 저장된 로그만으로 특정하지 않았다.

| W | K | 시도 | RPC 구간 참고값(RPC/s) | 오류 worker | 원본 |
| --- | --- | --- | --- | --- | --- |
| 9 | 2 | 최초 | 128,316.8 | 1, 2, 3 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w9-k2-r1.log` |
| 9 | 4 | 최초 | 149,401.9 | 0 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w9-k4-r3.log` |
| 9 | 2 | retry-missing | 128,054.1 | 5 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-missing/w9-k2-r1.log` |
| 9 | 4 | retry-missing | 149,226.2 | 7 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-missing/w9-k4-r3.log` |
| 9 | 2 | retry-final | 127,951.3 | 0, 4 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-final/w9-k2-r1.log` |
| 9 | 4 | retry-final | 149,272.5 | 3, 8 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-final/w9-k4-r3.log` |

## CPU

| W | K | Worker+helper CPU 범위(%) | ARM core busy 범위(%) | Host client busy 중앙값(%) | Host server busy 중앙값(%) |
| --- | --- | --- | --- | --- | --- |
| 9 | 1 | 99.87–100.10 | 100.00–100.00 | 90.89 | 91.71 |
| 9 | 2 | N/A | N/A | N/A | N/A |
| 9 | 4 | 99.89–100.09 | 99.90–100.00 | 93.56 | 92.58 |
| 12 | 1 | N/A | N/A | N/A | N/A |
| 12 | 2 | N/A | N/A | N/A | N/A |
| 12 | 4 | N/A | N/A | N/A | N/A |
| 16 | 1 | N/A | N/A | N/A | N/A |
| 16 | 2 | N/A | N/A | N/A | N/A |
| 16 | 4 | N/A | N/A | N/A | N/A |

범위는 유효 반복의 모든 worker/core 표본에 대한 최소–최대다. Host busy는 /proc/stat의 (total−idle−iowait)/total로 계산하며 process CPU 합계/8과 구분한다. DMESH_BUSY_POLL=1이므로 ARM100%만으로 RPC 처리 연산 포화나 ARM 단독 병목을 확정할 수 없다.

## 실패와 PF별 자원 증거

| W | K | 반복 | Runner stage | 기록된 분류 | 원본 |
| --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | client_repeat | 수정 전 동일-device mmap 등록 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w1-k1-r1.log` |
| 12 | 1 | 1 | client_repeat | DPA process 생성 실패; 합계28 경계 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w12-k1-r1.log` |
| 12 | 2 | 1 | client_repeat | DPA process 생성 실패; 합계28 경계 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w12-k2-r1.log` |
| 12 | 4 | 1 | client_repeat | DPA process 생성 실패; 합계28 경계 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w12-k4-r1.log` |
| 16 | 1 | None | host_ready | DPA process 생성 실패; 합계28 경계 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w16/w16-k1-ready.log` |
| 16 | 2 | None | host_ready | DPA process 생성 실패; 합계28 경계 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w16/w16-k2-ready.log` |
| 16 | 4 | None | host_ready | DPA process 생성 실패; 합계28 경계 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w16/w16-k4-ready.log` |
| 9 | 2 | 1 | client_repeat | 측정 완료 후 native teardown bad message | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w9-k2-r1.log` |
| 9 | 4 | 3 | client_repeat | 측정 완료 후 native teardown bad message | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/w9-k4-r3.log` |
| 9 | 2 | 1 | client_repeat | 측정 완료 후 native teardown bad message | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-missing/w9-k2-r1.log` |
| 9 | 4 | 3 | client_repeat | 측정 완료 후 native teardown bad message | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-missing/w9-k4-r3.log` |
| 9 | 전체 | None | dpu_cleanup | 종료파일 읽기 race; 재확인 해결 | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-missing/w9/dpu-stop.log` |
| 9 | 2 | 1 | client_repeat | 측정 완료 후 native teardown bad message | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-final/w9-k2-r1.log` |
| 9 | 4 | 3 | client_repeat | 측정 완료 후 native teardown bad message | `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-final/w9-k4-r3.log` |

Runner stage만으로 preflight와 측정 후 teardown을 구분하지 않는다. 개별 결과의 measurement_started, 오류 원문 및 classification을 함께 확인한다. 아래 context 수는 각 역할 로그의 성공 메시지 합계이며 한 시점의 hardware live-process query와 구분한다. 서로 다른 반복의 client context를 합산하지 않았다.

| W | K | 반복 | DPU context | Server PF:개수 | Client PF:개수 | 성공 메시지 합계 | 측정 시작 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1 | 1 | 1 | {"0b:00.0": 1} | {"0b:00.1": 1} | 3 | False |
| 12 | 1 | 1 | 12 | {"0b:00.0": 12} | {"0b:00.1": 4} | 28 | False |
| 12 | 2 | 1 | 12 | {"0b:00.0": 12} | {"0b:00.1": 4} | 28 | False |
| 12 | 4 | 1 | 12 | {"0b:00.0": 12} | {"0b:00.1": 4} | 28 | False |
| 16 | 1 | client 미시작 | 16 | {"0b:00.0": 12} | {} | 28 | False |
| 16 | 2 | client 미시작 | 16 | {"0b:00.0": 12} | {} | 28 | False |
| 16 | 4 | client 미시작 | 16 | {"0b:00.0": 12} | {} | 28 | False |
| 9 | 1 | 1 | 9 | {"0b:00.0": 9} | {"0b:00.1": 9} | 27 | True |
| 9 | 1 | 2 | 9 | {"0b:00.0": 9} | {"0b:00.1": 9} | 27 | True |
| 9 | 1 | 3 | 9 | {"0b:00.0": 9} | {"0b:00.1": 9} | 27 | True |
| 9 | 2 | 1 | 9 | {"0b:00.0": 9} | {"0b:00.1": 9} | 27 | True |
| 9 | 4 | 1 | 9 | {"0b:00.0": 9} | {"0b:00.1": 9} | 27 | True |
| 9 | 4 | 2 | 9 | {"0b:00.0": 9} | {"0b:00.1": 9} | 27 | True |
| 9 | 4 | 3 | 9 | {"0b:00.0": 9} | {"0b:00.1": 9} | 27 | True |

PF별 성공 로그를 합산하면 W12는 DPU12 + server PF0 12 + client PF1 4 = 28이며 추가 PF1 client8개의 process 생성이 실패했다. W16은 DPU16 + server PF0 12 = 28에서 server4개가 실패했고 client는 시작하지 못했다. 각 K에서 같은 경계와 Status0xf/syndrome0x269a7e를 확인했다. 각 host PF의 context는28개 미만이므로 **PF를 나눠도 chip 전체 DPA process28개 제한은 우회되지 않았다.** W12/16에서는 측정 구간 자체가 시작되지 않았다.

## PF 배치·정리·제약

Host DPA device는 server=0b:00.0, client=0b:00.1이며 Comch host device는 0b:00.1다. DPU device는 03:00.1/VHCA4. 환경 및 바이너리 식별은 environment.json을 그대로 보존했다. 임시 EU partition: `[{"id": 1, "vhca": 0, "eus": "0-47", "count": 48}, {"id": 2, "vhca": 1, "eus": "48-95", "count": 48}]`. DPU 잔여EU는 94개이며 chip max_processes=28다. 목표 DPA process는 host2W+DPU W=3W개다. 자원 요구량 자체가 실패 원인을 증명하는 것은 아니다.

보존된 server exit 총149개: {'0': 136, '1': 13}. Scoped stop-result 성공 7/7개. **Partition/group/process 모두0 복원 확인.**

Server exit1은 W16에서 DPA process를 만들지 못한 12개와 retry-final W9/K2의 server-6 한 개다. 후자는 종료 시 bad message / connection timed out을 기록했다. **프로세스 정리 완료와 정상 native teardown 성공은 구분한다.** 해당 server 원본: `/tmp/dmesh-grpc-host-dpa-pf01-20260926/retry-final/host/w9-k2/server-6.log`.

retry-missing에서는 종료코드 파일이 생성된 뒤 내용이 쓰이기 전의 짧은 간격에 빈 문자열을 읽어 harness의 int 변환이 실패했다. 최초 stop-result-first-attempt.json을 보존했고, scoped 재확인에서 proxy exit0·mock SIGTERM 종료 및 모든 process 소멸을 확인했다. 빈 종료코드 파일은 내용이 생길 때까지 기다리도록 stop helper를 보강했다. 이 harness race는 위 native teardown bad message와 별개의 문제다.

최종 process/복원/검사 기록: `/tmp/dmesh-grpc-host-dpa-pf01-20260926/final-validation.json`, Native channel-session 검사 PASS: `/tmp/dmesh-grpc-host-dpa-pf01-20260926/channel-session-test.log`.

## 자료

- 검증·원본·실패·환경 JSON: `bench-results/2026-09-26_grpc-go-64b-host-dpa-pf01.json`
- [반복별 CSV](2026-09-26_grpc-go-64b-host-dpa-pf01.csv)
- 원본 디렉터리: `/tmp/dmesh-grpc-host-dpa-pf01-20260926`
- 실험 원본 전체 압축: `bench-results/2026-09-26_grpc-go-64b-host-dpa-pf01-raw.tar.gz`
- 환경: `/tmp/dmesh-grpc-host-dpa-pf01-20260926/environment.json`
- 분석 스크립트: `/tmp/dmesh-grpc-host-dpa-pf01-20260926/scripts/analyze-pf01.py`

Raw results SHA-256: `020ff103ea3c4472170fe762f418d7302d1b7bb31869e545d30a338051cd852e`. Failures SHA-256: `e1555a4c1ecde345dc812e8c693ee622ae60c1ff9e1aa76378e5028be3a92a05`. 분석 시각: 2026-09-26T05:44:35.640512+00:00.

| 환경에 기록된 바이너리 | SHA-256 |
| --- | --- |
| linkerd2-proxy/target/release/linkerd2-proxy | `a25fa454bb30d25fcb1263653461beda13d756ddf6d5f03ff6339075a3764486` |
| src/transport/build/device/dpa_kernel.a | `4c5b84fdd524dfee19d2e3b671bbac9518320ec377b5438382d603ad5a8b5e2f` |
| /home/youngmin/DPUMesh/build/lib/libdpumesh.so.5 | `727ec2ab5dc427140da2c5db9e9f1779a464d84d770f774d19febe84bdc651af` |
| /home/youngmin/DPUMesh/integrations/grpc/go/bin/channel-bench-scale | `f5c7b85e496e1d49f30d4633fdd1a776f2ac59921eb3244aa87f568b86c1589f` |
| /tmp/dmesh-grpc-host-dpa-pf01-20260926/host-build/lib/libdpumesh.so.5 | `20228fb627289f3761f7f5e45b6695201deec43490cf28e68744be847dc8f7c8` |
| /home/youngmin/DPUMesh/src/transport/host/channel.c | `b8a0d3eb3d8f50ea479e3f2e8f37356102ae6448d47813701ec15652b05800fd` |

## 공개 자료의 범위

링크로 연결한 보고서·CSV·그림·작은 결과 JSON은 Git에 포함한다. 코드로 표시한 원본 경로는 Git 추적 대상이 아니며, `/tmp`는 실험 당시 경로라 현재 존재를 보장하지 않는다. 실제 로컬 보존 파일은 [자료 보존 안내](README.md#자료-보존)를 참조한다.
