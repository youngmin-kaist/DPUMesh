# gRPC Serialization Offload Benchmark (Standalone)

이 문서는 `DPUMesh` 메인 실행 경로와 분리해서, `gRPC` 폴더만으로 빌드/실행 가능한 벤치마크 환경과 실험 절차를 설명한다.

## 1. 측정 대상 (What)
- 단건 End-to-End latency:
  - `grpc_proto_serialize_hello()` 호출 전/후 시간
  - 단위: us
  - 지표: `avg`, `p50`, `p95`, `p99`, `min`, `max`
- 처리량:
  - `req/s`: 성공한 요청 수 / 측정 구간 wall-time
  - `MiB/s`: 성공한 encoded byte 총합 / 측정 구간 wall-time
- 안정성:
  - `success_iters`, `failed_iters`

## 2. 측정 목적 (Why)
- `p95/p99`:
  - 평균이 아닌 tail latency 확인. 버스트/큐잉/메모리 압박 시 변동 탐지에 필요.
- `req/s`, `MiB/s`:
  - 실제 서비스 관점의 처리 한계와 링크/메모리 대역폭 사용량 추정에 필요.
- success/fail:
  - 성능 수치가 유효한지(오류 누적 없는지) 검증하기 위한 최소 안전 지표.

## 3. 현재 측정 경계 (Scope)
- 포함:
  - Host flatten + H2D 복사 + DPA RPC 실행 + D2H 복사 + 결과 버퍼 resize
- 미포함:
  - 실제 gRPC 네트워크 송수신, TCP stack, 애플리케이션 로직
- 즉, 이 벤치마크는 "serialization offload 경로 자체"의 비용을 측정한다.

## 4. 사전 조건 (Prerequisites)
- DOCA SDK 설치 (`/opt/mellanox/doca` 기준, 필요 시 `DOCA_DIR`로 오버라이드)
- `pkg-config`에서 아래 항목 확인 가능해야 함:
  - `doca-common`
  - `doca-dpa`
  - `libflexio`
- BlueField/DPA 실행 가능한 환경
- 대상 DOCA device PCI BDF 확인 필요 (예: `03:00.0`)

## 5. 독립 빌드 (gRPC 폴더 단독)
- 빌드 스크립트: [build_standalone_bench.sh](/home/jihoon/DPUMesh/gRPC/build_standalone_bench.sh)
- 실행:
```bash
cd /home/jihoon/DPUMesh/gRPC
chmod +x build_standalone_bench.sh
./build_standalone_bench.sh
```
- 산출물:
  - 실행 파일: `gRPC/build_standalone/grpc_offload_bench`
  - DPA device archive: `gRPC/build_standalone/device/grpc_dpa_kernel.a`

참고:
- 이 빌드는 `DPUMesh/DPUMesh/meson.build`를 사용하지 않는다.
- `dpacc`를 `gRPC/build_standalone_bench.sh`에서 직접 호출한다.

## 6. 실행 방법
### 기본 실행
```bash
/home/jihoon/DPUMesh/gRPC/build_standalone/grpc_offload_bench \
  --pci 03:00.0
```

### 파라미터
- `--pci <BDF>`: 대상 DOCA 디바이스 (필수)
- `--iters <N>`: 측정 iteration 수 (기본 5000)
- `--warmup <N>`: warmup iteration 수 (기본 100)
- `--submit-batch <N>`: 한 번의 offload submit에 묶는 요청 수
- `--name-len <N>`: `HelloRequest.name` 길이
- `--scores <N>`: packed `uint32` element 개수
- `--seed <N>`: 점수 배열 시작값
- `--batch <N>`: runtime init 시 max batch 크기 (기본 64)

예시:
```bash
/home/jihoon/DPUMesh/gRPC/build_standalone/grpc_offload_bench \
  --pci 03:00.0 \
  --warmup 200 \
  --iters 20000 \
  --submit-batch 64 \
  --name-len 128 \
  --scores 512 \
  --seed 1 \
  --batch 64
```

### Ring Loop 실험 빌드
`run_dpu_arm_bench.sh`에서 ring loop 경로를 켜서 빌드할 수 있다.
```bash
cd /home/jihoon/DPUMesh/gRPC
./run_dpu_arm_bench.sh --ring-loop --pci 03:00.0
```

## 7. 실험 절차 (How)
1. 환경 고정
   - 동일한 PCI 디바이스, 동일한 바이너리, 동일한 CPU 주파수 정책 사용.
2. Warmup
   - 초기 page fault/cache 영향 제거를 위해 충분한 warmup (`>=100`) 적용.
3. 단일 변수 변화 실험
   - 한 번에 하나의 변수만 바꿔 병목 원인 분리.
4. 반복 실행
   - 각 조건당 최소 3회 반복 후 중앙값 비교.
5. 오류율 확인
   - `failed_iters > 0`이면 해당 측정값은 성능 비교에서 제외.

## 8. 권장 실험 매트릭스
### A. 메시지 크기 스케일링
- 목적: payload 크기 증가 시 latency/throughput 곡선 확인
- 방법:
  - `name_len`: `16, 64, 256, 1024`
  - `scores`: `16, 64, 256, 1024`
  - 나머지 파라미터 고정

### B. iteration 안정화
- 목적: 통계 노이즈 감소 및 tail 안정성 확인
- 방법:
  - `iters`: `5000, 20000, 50000`
  - 동일 workload로 반복

### C. batch capacity 영향
- 목적: 내부 task array 크기 설정이 경로에 미치는 영향 확인
- 방법:
  - `batch`: `32, 64, 128`
  - 단, 현재 벤치는 단건 submit 기반이라 영향이 작을 수 있음

## 9. 결과 해석 가이드
- `avg`만 보지 말고 `p95/p99`를 우선 확인.
- `MiB/s` 증가 없이 `req/s`만 증가하면 메시지 소형화 영향일 수 있음.
- `failed_iters`가 발생하면 먼저 기능 오류 해결 후 성능 비교 진행.

## 10. 재현성 체크리스트
- 동일 commit/바이너리인지 확인
- `DOCA_DIR`, `DPACC_MCPU_FLAG`, `DPA_APP_NAME` 환경 변수 기록
- 실행 커맨드 원문 저장
- 출력 리포트 원문 저장

## 11. 관련 코드 위치
- runtime API: [host_agent.h](/home/jihoon/DPUMesh/gRPC/host_agent.h)
- runtime 구현: [host_agent.cc](/home/jihoon/DPUMesh/gRPC/host_agent.cc)
- benchmark API: [offload_benchmark.h](/home/jihoon/DPUMesh/gRPC/offload_benchmark.h)
- benchmark 구현: [offload_benchmark.cc](/home/jihoon/DPUMesh/gRPC/offload_benchmark.cc)
- standalone main: [grpc_bench_main.cc](/home/jihoon/DPUMesh/gRPC/grpc_bench_main.cc)
- DPA kernel: [grpc_dpa_kernel.c](/home/jihoon/DPUMesh/gRPC/grpc_dpa_kernel.c)
