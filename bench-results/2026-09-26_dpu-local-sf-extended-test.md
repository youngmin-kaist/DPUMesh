# DPU-local SF: PF base → extended DPA context — 2026-09-26

**PF `mlx5_0`에서 시작한 base context를 기존 DPU-local SF `mlx5_2`로 확장하는 데 성공했다.** Extended context의 handle 조회와 64B DPA heap 할당·해제, 전체 context/device 정리도 모두 `DOCA_SUCCESS`였다. 앞선 standalone SF start 실패를 SF의 DPA 사용 불가로 해석하면 안 된다.

## 대상과 실행 범위

- 실행 시각: 2026-09-26 07:14:46–07:14:54 UTC
- DOCA 3.5.0098
- PF: `mlx5_0`, PCI `0000:03:00.0`, VHCA **3**, function type PF
- 기존 DPU-local SF: `mlx5_2`, auxiliary `mlx5_core.sf.2`, VHCA **273**, function type SF
- SF netdev: `enp3s0f0s0`; representor: `en3f0pf0sf0`; controller0/PF0/SF0
- 장치 선택은 정확한 IB device 이름과 예상 function type/VHCA 일치를 검증했다.
- 초기/종료 EU partition은 모두 0개. 이번에는 partition 생성/할당/삭제를 하지 않았다.
- SF 생성/삭제, trust 변경, 네트워크/OVS/IP 설정, firmware 설정/reset은 수행하지 않았다.
- Application DPA thread, ring, Comch, RDMA/DMA 전송은 생성하지 않았다. EU affinity/scheduling 성능도 시험하지 않았다.

## 실제 API 경로와 결과

```c
doca_dpa_create(pf_dev, &base);
doca_dpa_set_app(base, DPU_mesh_dpa_app);
doca_dpa_start(base);
doca_dpa_device_extend(base, sf_dev, &ext);
doca_dpa_get_dpa_handle(ext, &handle);
doca_dpa_mem_alloc(ext, 64, &mem);
// teardown
doca_dpa_mem_free(ext, mem);
doca_dpa_destroy(ext);
doca_dpa_stop(base);
doca_dpa_destroy(base);
doca_dev_close(sf_dev);
doca_dev_close(pf_dev);
```

모든 호출이 `DOCA_SUCCESS`를 반환했다. Extended context는 반환 시 이미 started 상태이므로 별도 start/stop을 호출하지 않았다. 하위 extended context를 base보다 먼저 destroy했다. Probe exit code는 0, `passed=true`, `cleanup_ok=true`였다.

| 관측 시점 | `dpaeumgmt info status -d mlx5_0`의 processes |
|---|---:|
| 시작 전 | 0 |
| PF base만 시작 | 1 |
| PF base + SF extended + 64B 할당 | 1 |
| 모두 정리 후 | 0 |

두 중간 snapshot은 probe가 stdin gate에서 기다리는 동안 순차 조회했다. 해당 조회가 성공한 뒤에만 다음 단계로 진행했다.

이 결과는 **PF base를 SF로 확장해 사용할 수 있음**을 확인한다. PF 조회에서 process 수 증가는 없었다. 다만 이 조회만으로 전체 장치의 physical process 슬롯 계수를 직접 검증했다고 볼 수는 없고, 이번 시험에서는 28개를 유지한 상태에서 추가 생성을 시도하지 않았다. 따라서 별도 VHCA의 독립적인 process 28개 풀 확보나 기존 28개 제한 우회가 입증된 결과는 아니다. SF에 EU partition이 있는 경우의 thread 실행 가능 여부도 이번 메모리 API 시험의 범위 밖이다.

## 실행 명령과 보존 상태

Probe 실행:

```sh
sudo -n /tmp/dmesh-dpu-local-sf-extended-20260926/probe
```

Supervisor `run.py`가 probe를 실행하고 두 gate에서 아래 읽기 전용 조회 후 stdin newline을 보냈다.

```sh
sudo -n /opt/mellanox/doca/tools/dpaeumgmt info status -d mlx5_0
```

전후에는 `partition query -d mlx5_0`, `mlxdevm -j port show`, 두 netdev의 `ip -j -d link show`, sysfs 경로, boot ID를 읽었다. 장치 조회와 변경 가능 API 호출은 서로 겹치지 않게 순차 실행했다. Alarm 60초는 probe의 정상 cleanup 요청이며, SDK/driver 내부에서 멈춘 호출에 대한 강제 종료 보장은 아니다. 실제 probe는 약 8초 만에 정상 완료했다.

- 종료 후 process0, root group0, partition0.
- `mlxdevm port show` 전체 JSON 전후 동일.
- 두 netdev의 ifindex/ifname/flags/master/address/MTU/operstate/linkinfo 전후 동일, UP/LOWER_UP.
- Boot ID 전후 동일: `7ad2a82d-5327-4f94-8092-2bf9fdfe4035`.
- 재부팅 이전 연결 끊김의 원인은 이번 시험에서 판단하지 않았다.

## 근거와 원본

- [NVIDIA DOCA DPA: Extended DOCA DPA Context](https://networking-docs.nvidia.com/doca/sdk/doca-dpa) — PF base를 VF/SF로 확장, 이미 started 상태로 반환.
- 로컬 SDK `/opt/mellanox/doca/include/doca_dpa.h:451–477`, `:613–650`.
- NVIDIA 샘플 `/opt/mellanox/doca/samples/doca_dpa/dpa_common.c:857` — extended context를 먼저 destroy.
- [검증 JSON](2026-09-26_dpu-local-sf-extended-test.json)
- 원본 로그·소스 압축: `bench-results/2026-09-26_dpu-local-sf-extended-test-raw.tar.gz`
- [Standalone SF 실패 기록](2026-09-26_dpu-local-sf-dpa-test.md)

## 공개 자료의 범위

링크로 연결한 보고서·CSV·그림·작은 결과 JSON은 Git에 포함한다. 코드로 표시한 원본 경로는 Git 추적 대상이 아니며, `/tmp`는 실험 당시 경로라 현재 존재를 보장하지 않는다. 실제 로컬 보존 파일은 [자료 보존 안내](README.md#자료-보존)를 참조한다.
