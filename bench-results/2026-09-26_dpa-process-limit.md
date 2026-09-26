# DPA process 공유 한도 직접 검증 — 2026-09-26

현재 장비에서 **host와 DPU가 동시에 유지할 수 있는 DPA process(context)는 합계 28개**임을 슬롯 해제·재생성으로 직접 확인했다. Host 단독 28개는 성공했고, 어느 쪽이든 합계 29번째 생성은 실패했다.

## 실험 결과

| 순서 | 유지 상태 | 동작 | 결과 |
|---|---|---|---|
| 1 | DPU 1 | Host PF0에서 27개를 순차 생성 | 모두 성공: DPU 1 + host 27 = 28 |
| 2 | DPU 1 + host 27 | Host PF0에 1개 추가 | 실패: 합계 29번째 |
| 3 | DPU 1 + host 27 | Host PF1에 1개 추가 | 실패: PF를 바꿔도 합계 한도 동일 |
| 4 | DPU 1 + host 27 | DPU context만 stop/destroy | 성공: 기존 host 27개는 그대로 유지 |
| 5 | DPU 0 + host 27 | Host PF0에 1개 추가 | 성공: host 단독 28개 |
| 6 | DPU 0 + host 28 | DPU에 1개 추가 | 실패: 이번에는 DPU 쪽에서 합계 29번째 |
| 7 | DPU 0 + host 28 | 방금 추가한 host context 1개 stop/destroy | 성공: host 27개 |
| 8 | DPU 0 + host 27 | DPU에 1개 추가 | 성공: 다시 DPU 1 + host 27 = 28 |

실패한 세 호출은 모두 `doca_dpa_start()`에서 `DOCA_ERROR_DRIVER`(21)를 반환했다. Firmware 로그는 모두 `Failed to create PRM process. Status is 0xf, syndrome 0x269a7e.`였다. 실패한 context의 destroy/device close도 성공했다. 전체 실험 동안 생성 성공은 30회지만, **동시 유지 최대치는 28개**다.

## 방법과 조건

한 OS process에서 `doca_dev_open → doca_dpa_create → doca_dpa_set_app → doca_dpa_start`로 context 하나만 만들고 유지하는 별도 probe를 사용했다. 애플리케이션 DPA thread, completion, Comch, ring, flow는 생성하지 않았다. 기존 DPUMesh DPA app을 양쪽 아키텍처용 archive로 링크했다. Host 원본27개 PID는 대조 실험 동안 재시작하지 않았다. Native 정리는 `doca_dpa_stop → doca_dpa_destroy → doca_dev_close` 결과를 각각 기록했다.

DPU는 `03:00.1`(VHCA4), host 주 경로는 PF0 `0b:00.0`(VHCA0), 추가 대조는 PF1 `0b:00.1`(VHCA1)이다. Host 두 VHCA에 임시 공유 partition(EU0–94, 95개)을 배정했고 DPU에는 95개를 남겼다. Probe는 최대300초 TTL과 SIGTERM 정리를 사용했으며 이번 실행은 TTL 만료 없이 끝났다. DOCA 런타임은 3.5.0098이다.

동시 context 수는 양쪽의 성공한 ready 이벤트와 각 OS process 생존, 명시적 stop/destroy를 대조했다. **DPU에서 실행한 `dpaeumgmt info status`의 processes는 전체 칩 합계로 사용하지 않았다.** 실제 host28/DPU0 상태에서 이 query는 processes0, host27/DPU1에서는 processes1을 출력했다.

## 정리와 재현 자료

DPU probe3개·host probe30개가 모두 종료됐다. 성공한 context30개의 stop/destroy/device close와 실패한 start3개의 destroy/device close가 모두 성공했다. 종료 후 probe PID가 양쪽 모두0이며 partition/group/DPA process도0으로 복원했다. 프로젝트 production source는 이번 실험에서 변경하지 않았다.

- 전체 단계와 검증 JSON: `bench-results/2026-09-26_dpa-process-limit.json`
- 원본 로그·probe·실행 스크립트 압축: `bench-results/2026-09-26_dpa-process-limit-raw.tar.gz`
- 실행 로그: `/tmp/dmesh-dpa-process-limit-20260926/run.log`
- 최소 probe 소스: `/tmp/dmesh-dpa-process-limit-20260926/probe.c`
- Host 29번째 실패: `/tmp/dmesh-dpa-process-limit-20260926/host/host-28-with-dpu.log`
- PF1 추가 생성 실패: `/tmp/dmesh-dpa-process-limit-20260926/host/host-pf1-with-dpu.log`
- DPU 29번째 실패: `/tmp/dmesh-dpa-process-limit-20260926/dpu-with-host28.log`
- Partition 복원: `/tmp/dmesh-dpa-process-limit-20260926/partition-restored.json`

## 공개 자료의 범위

링크로 연결한 보고서·CSV·그림·작은 결과 JSON은 Git에 포함한다. 코드로 표시한 원본 경로는 Git 추적 대상이 아니며, `/tmp`는 실험 당시 경로라 현재 존재를 보장하지 않는다. 실제 로컬 보존 파일은 [자료 보존 안내](README.md#자료-보존)를 참조한다.
