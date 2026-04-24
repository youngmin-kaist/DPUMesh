# gRPC DPA Serializer Patch Report

## 1. 현재 실행 경로

### 1.1 RPC batch path
- Host가 flat payload를 DPA scratch로 `H2D` 복사한다.
- Host가 `ProtoTask[]`를 DPA task array로 `H2D` 복사한다.
- Host가 `grpc_dpa_serialize_batch_rpc()`를 호출한다.
- DPA가 task별 protobuf/gRPC wire bytes를 생성한다.
- Host가 `ProtoCompletion[]`를 `D2H` 복사한다.
- 성공 item에 대해서만 output payload를 `D2H` 복사한다.

### 1.2 Persistent ring-worker path
- Host가 request ring에 `GrpcReqDesc`를 적재한다.
- DPA worker가 ring을 poll 하며 request를 가져간다.
- DPA worker가 output pool에 결과를 쓴 뒤 completion ring에 `GrpcCplDesc`를 적재한다.
- Host가 completion ring을 poll 하며 결과를 회수한다.

## 2. 확인된 버그
- packed repeated length가 fixed32로 기록되던 경로가 있었다.
- nested message length backfill이 fixed-width/비표준 방식으로 기록되던 경로가 있었다.
- nested message reserve/backfill 규약이 길이-가변(varint) 규약과 섞여 있었다.
- `grpc_dpa_batch_loop()`가 submit 실패를 무시했다.
- host batch worker가 compile-time `MAX_BATCH`에 의존해 runtime `max_batch`와 불일치할 수 있었다.

## 3. 확인 결과 사실이 아닌 의심
- `string/bytes` payload loop 자체는 중복 복사가 아니라 실제 payload emission 루프다.
- gRPC 5-byte prefix wrapping 자체는 현재 의도와 맞고, 이번 패치의 주된 문제는 prefix가 아니라 protobuf LEN encoding 규약 혼재였다.
- nested generic encoder 불일치로 보였던 golden test 실패는 encoder 버그가 아니라 테스트 fixture의 nested string offset 기준(root-relative vs child-relative) 오류였다.

## 4. 패치 그룹

### Patch Group 1: wire-format correctness
- 공용 encoder를 `grpc_wire_encode.[ch]`로 분리했다.
- packed repeated와 nested message의 length-delimited 경로를 canonical varint backfill로 통일했다.
- HelloRequest fast path와 generic fallback path를 같은 공용 encoder 안에서 분리했다.

### Patch Group 2: host-side orchestration cleanup
- RPC batch submit 경로를 `grpc_dpa_submit_batch_rpc_path()`로 분리했다.
- ring-worker batch loop가 submit 실패 시 per-item error completion을 생성하도록 바꿨다.
- runtime `max_batch` 기반 동적 buffer를 사용하도록 바꿨다.

### Patch Group 3: execution structure cleanup
- device file은 host/device 공용 encoder 호출 래퍼로 단순화했다.
- persistent worker는 한 번 reschedule마다 다건 burst를 처리하도록 바꿨다.
- RPC batch path와 ring-worker path의 역할을 코드 상에서 더 명확히 분리했다.

### Patch Group 4: selective-offload scaffold
- `grpc_selective_policy_runtime.*`를 추가했다.
- string/bytes field에 대해 threshold 기반 copy vs zero-copy candidate 정책 hook을 추가했다.
- 현재는 Host lowering 단계에서 decision/statistics만 기록한다.

### Patch Group 5: tests
- `grpc_serializer_golden_test.cc`를 추가했다.
- HelloRequest mixed case, packed multibyte length, nested message canonical length를 검증한다.
- 현재 환경에 protobuf runtime이 없어 golden/reference test만 수행한다.

## 5. 변경 파일
- `grpc_wire_encode.h`
- `grpc_wire_encode.c`
- `grpc_dpa_kernel.c`
- `dpa_batch_worker.c`
- `grpc_selective_policy_runtime.h`
- `grpc_selective_policy_runtime.cc`
- `host_agent.h`
- `host_agent.cc`
- `tests/grpc_serializer_golden_test.cc`
- `build_grpc_serializer_tests.sh`
- `build_standalone_bench.sh`
- `../DPUMesh/meson.build`

## 6. 지금 실제로 고쳐진 것
- protobuf LEN-delimited wire-format correctness를 canonical varint 기준으로 정리했다.
- nested message와 packed repeated의 기본 encoding 버그를 수정했다.
- submit 실패가 silent ignore 되던 문제를 고쳤다.
- batch loop가 runtime `max_batch`를 따르도록 고쳤다.
- 공용 serializer core를 분리해 device file reviewability를 높였다.
- golden test가 현재 수정 내용을 직접 검증한다.

## 7. 지금은 scaffold만 있는 것
- selective-offload 정책은 decision/statistics scaffold만 있다.
- zero-copy placement를 실제 task/memory placement에 반영하지는 않았다.
- specialized encoder path는 HelloRequest sample hook만 있다.
- DPA multi-thread/sharded execution은 실제 구현하지 않고, burst consume 구조만 열어 두었다.
- trusted host protobuf runtime과의 byte-for-byte 비교는 현재 환경 dependency 부재로 미구현이다.

## 8. 남은 TODO
- protobuf runtime 또는 descriptor-driven host serializer를 붙여 trusted serializer 비교를 추가해야 한다.
- selective-offload decision을 실제 placement/H2D policy와 연결해야 한다.
- specialized encoder generator를 schema/codegen과 연결해야 한다.
- persistent ring-worker path에 stage overlap과 async pipelining을 더 넣어야 한다.
- ring-worker completion lookup의 선형 탐색은 추후 request_id index map으로 바꾸는 편이 낫다.
