# Existing DPU-local SF: standalone DPA context test — 2026-09-26

**후속 확인:** SF는 PF base에서 `doca_dpa_device_extend()`로 확장해야 한다. 동일한 기존 SF의 extended context 생성·64B 메모리 할당/해제는 [후속 시험에서 성공했다](2026-09-26_dpu-local-sf-extended-test.md). 아래 실패는 standalone SF base 경로의 결과이며, SF에서 DPA를 사용할 수 없다는 결론이 아니다.

**기존 DPU-local SF에 EU 8개를 할당했지만 standalone DPA context 시작은 실패했다.** 현재 설정에서 단독 생성부터 실패하므로 28개 context 유지 후 추가 생성 시험은 진행하지 않았다. 이 실패를 28개 process 한도 초과의 증거로 해석하지 않는다.

## 대상과 범위

- 기존 representor: `en3f0pf0sf0` (`pci/0000:03:00.0/229612`)
- 실제 SF: `mlx5_2` / `enp3s0f0s0`, auxiliary `mlx5_core.sf.2`
- Controller 0, PF0, SF0, **VHCA 273**, DOCA function type 2(SF)
- 기존 SF 설정: active/attached, RoCE enabled, trust off. 변경하지 않았다.
- 기존 SF를 그대로 사용했으며 새 SF 생성·삭제, link/OVS/IP 설정, firmware 설정·reset은 수행하지 않았다.
- 초기 DPA process0·EU partition0 상태에서 SF에 EU0–7만 임시 배정했다.
- Probe는 exact IB device name으로 대상 SF를 선택했다. DPA thread·ring·Comch·DMA 전송은 생성하지 않고 `create/set_app/start`만 호출했다.

## 결과

| 단계 | 결과 |
|---|---|
| SF capability `doca_dpa_cap_is_supported` | DOCA_SUCCESS |
| EU partition 생성, VHCA273/EU0–7 | 성공, partition ID1 |
| `doca_dpa_create` | 성공 |
| `doca_dpa_set_app` | 성공 |
| `doca_dpa_start` | **DOCA_ERROR_DRIVER (21)** |
| 실패 context destroy / device close | 모두 DOCA_SUCCESS |

Firmware 오류:

```text
Failed to create PRM process. Status is 0x3, syndrome 0x775dd0.
```

이는 앞서 process 수 28개 경계에서 관측한 `Status0xf/syndrome0x269a7e`와 다른 오류다. 이번에는 다른 DPA context가 없는 상태에서 SF 자체의 process 시작이 거절되었다. 오류 syndrome의 공식 상세 의미는 이 테스트에서 해독하지 않았다. Capability 지원 응답만으로 SF 소유 base DPA process 시작까지 보장되지는 않는 것으로 관측됐다.

## 실제 변경·실행 명령

```sh
sudo /opt/mellanox/doca/tools/dpaeumgmt partition create -d mlx5_0 --vhca_list 273 --range_eus 0-7 --max_num_eu_group 1
sudo /tmp/dmesh-dpu-local-sf-existing-20260926/probe mlx5_2 5
sudo /opt/mellanox/doca/tools/dpaeumgmt partition destroy --id_partition 1 -d mlx5_0
```

추가로 sysfs, `ip link show`, `mlxdevm port show`, `dpaeumgmt partition query/info status`, probe의 devinfo-only 모드를 사용해 대상과 전후 상태를 확인했다. 장치 명령은 순차 실행했다. Probe의 5초 TTL은 정상 시작 후 대기/정리를 위한 것이며, SDK 호출 hang을 강제 종료한다는 보장은 아니다. 실제 SF start 실패는 즉시 반환했다.

## 종료 상태

- 이번에 만든 EU partition1만 제거하여 초기 partition0개로 복원했다.
- DPA status의 process/group 수는0이다.
- `mlxdevm port show`의 전체 JSON이 전후 동일하다.
- 기존 representor와 SF netdev 모두 UP/LOWER_UP 상태이며, MAC·master·flags·operstate가 전후 동일하다.
- Boot ID는 전후 동일하다. 이는 이번 테스트 도중 재부팅이 없었다는 확인이며 모든 순간의 네트워크 연결성을 증명하는 것은 아니다.
- 재부팅 이전의 연결 끊김 원인은 이번 테스트에서 조사하거나 확정하지 않았다.

[검증 JSON](2026-09-26_dpu-local-sf-dpa-test.json) · 원본 로그/소스 압축: `bench-results/2026-09-26_dpu-local-sf-dpa-test-raw.tar.gz` · SF probe 로그: `/tmp/dmesh-dpu-local-sf-existing-20260926/sf-direct.log` · 실험 원본: `/tmp/dmesh-dpu-local-sf-existing-20260926`

## 공개 자료의 범위

링크로 연결한 보고서·CSV·그림·작은 결과 JSON은 Git에 포함한다. 코드로 표시한 원본 경로는 Git 추적 대상이 아니며, `/tmp`는 실험 당시 경로라 현재 존재를 보장하지 않는다. 실제 로컬 보존 파일은 [자료 보존 안내](README.md#자료-보존)를 참조한다.
