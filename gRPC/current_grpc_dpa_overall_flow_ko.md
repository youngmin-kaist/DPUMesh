# 현재 gRPC DPA Serializer 전체 동작 정리

## 1. 개요
이 저장소의 현재 gRPC serializer 경로는 크게 세 층으로 나뉜다.

1. Host-side lowering / orchestration
2. DPA-side protobuf wire byte emission
3. 검증 및 실행 경로 선택(RPC batch path / persistent ring-worker path)

핵심 목표는 다음과 같다.
- Host가 `HelloRequest`류 메시지를 flat representation으로 낮춘다.
- DPA는 protobuf/gRPC wire-format byte를 생성한다.
- Host는 결과를 회수하거나, persistent worker path에서는 shard별 ring을 통해 병렬 처리한다.
- serializer correctness는 shared encoder 하나로 유지한다.

---

## 2. 주요 파일과 역할

### 2.1 serializer core
- [grpc_wire_encode.h](/home/jihoon/DPUMesh/gRPC/grpc_wire_encode.h)
- [grpc_wire_encode.c](/home/jihoon/DPUMesh/gRPC/grpc_wire_encode.c)

역할:
- Host/DPA 공용 protobuf wire encoder core
- gRPC 5-byte prefix 포함 출력 생성
- canonical varint length-delimited encoding 유지
- specialized path와 generic fallback path를 한 곳에서 관리

현재 지원:
- `HelloRequestFlat` specialized fast path
- generic field loop fallback path
- packed repeated length canonical varint
- nested message length canonical varint

### 2.2 DPA device entry
- [grpc_dpa_kernel.c](/home/jihoon/DPUMesh/gRPC/grpc_dpa_kernel.c)

역할:
- DPA RPC batch entry 제공
- persistent ring-worker entry 제공
- 실제 byte emission은 직접 구현하지 않고 `grpc_wire_serialize_one()` 호출

### 2.3 Host-side DPA submit / worker glue
- [dpa_batch_worker.c](/home/jihoon/DPUMesh/gRPC/dpa_batch_worker.c)

역할:
- RPC batch path의 H2D / RPC / D2H orchestration
- ring-worker host loop helper
- submit 실패 시 per-item error completion 생성

### 2.4 Host runtime / lowering / ring orchestration
- [host_agent.h](/home/jihoon/DPUMesh/gRPC/host_agent.h)
- [host_agent.cc](/home/jihoon/DPUMesh/gRPC/host_agent.cc)

역할:
- `demo::HelloRequest`를 flat arena로 lowering
- task 배열 구성
- runtime init / destroy
- RPC batch path 호출
- persistent ring path에서 shard routing, req/cpl ring 관리, completion 회수

### 2.5 Shard routing helper
- [grpc_shard_routing.h](/home/jihoon/DPUMesh/gRPC/grpc_shard_routing.h)
- [grpc_shard_routing.cc](/home/jihoon/DPUMesh/gRPC/grpc_shard_routing.cc)

역할:
- `request_id` 기반 stable shard 선택
- batch 요청을 shard별로 분할하는 dispatch plan 생성
- out-of-order completion matching을 위한 request_id hash map 제공

### 2.6 정책 scaffold
- [grpc_selective_policy_runtime.h](/home/jihoon/DPUMesh/gRPC/grpc_selective_policy_runtime.h)
- [grpc_selective_policy_runtime.cc](/home/jihoon/DPUMesh/gRPC/grpc_selective_policy_runtime.cc)

역할:
- string/bytes field의 copy vs zero-copy candidate decision scaffold
- 현재는 lowering 시점 decision/statistics 기록만 수행
- 실제 placement/H2D policy까지는 아직 연결되지 않음

---

## 3. 데이터 모델

### 3.1 Host-side 입력 메시지
현재 중심 입력은 [host_agent.h](/home/jihoon/DPUMesh/gRPC/host_agent.h)에 있는 `demo::HelloRequest`다.

구성:
- `id_`
- `name_`
- `scores_`

### 3.2 Flat representation
실제 DPA serializer 입력은 [proto_meta.h](/home/jihoon/DPUMesh/gRPC/proto_meta.h)의 `HelloRequestFlat`이다.

구성:
- `uint64_t id`
- `DpaStringRef name`
- `DpaU32ArrayRef scores`

의미:
- 문자열/배열 payload는 object ABI를 직접 해석하지 않고
- `offset + len/count` 형태의 flat reference로 표현한다.
- 따라서 DPA는 protobuf runtime object ABI internals에 의존하지 않는다.

### 3.3 Task / Completion
- `ProtoTask`
- `ProtoCompletion`

`ProtoTask`는 다음 정보를 가진다.
- 어떤 descriptor를 쓸지 (`desc_id`)
- host 메시지 주소 / 길이
- host output buffer 주소 / cap
- DPA msg/out 주소
- `request_id`

`ProtoCompletion`은 다음을 가진다.
- `request_id`
- `encoded_len`
- `status`

---

## 4. serializer correctness core
핵심 함수는 [grpc_wire_encode.c](/home/jihoon/DPUMesh/gRPC/grpc_wire_encode.c)의 아래 함수다.

- `grpc_wire_serialize_one()`

동작 순서:
1. `ProtoTask.desc_id`로 descriptor를 찾는다.
2. message payload를 `out_base + 5`부터 쓴다.
3. specialized 가능하면 specialized encoder 사용
4. 아니면 generic encoder 사용
5. payload 길이를 계산한다.
6. gRPC 5-byte prefix를 앞에 기록한다.
7. `ProtoCompletion`을 채운다.

### 4.1 specialized path
현재 specialized path는 `HelloRequest`에 대해서만 있다.

함수:
- `emit_hello_request_specialized()`

특징:
- field layout을 이미 알고 있으므로 `find_desc + field-loop + switch`를 건너뛴다.
- `id`, `name`, `scores`를 직접 처리한다.

### 4.2 generic fallback path
함수:
- `emit_message_generic()`
- `emit_field_generic()`

특징:
- descriptor 기반 field loop
- `FK_U32`, `FK_U64`, `FK_BOOL`, `FK_STRING`, `FK_BYTES`, `FK_MESSAGE`, packed repeated 등을 처리

### 4.3 length-delimited correctness
현재 중요한 correctness 포인트:
- `string/bytes` length는 canonical varint
- packed repeated length는 canonical varint
- nested message length도 canonical varint

이전의 prototype에서 있던 다음 문제는 수정된 상태다.
- fixed32 length 사용
- nested reserve/backfill 규약 혼재

---

## 5. Host-side lowering
핵심 함수:
- [host_agent.cc](/home/jihoon/DPUMesh/gRPC/host_agent.cc)의 `flatten_hello_request()`

동작:
1. `FlatArena` 위에 `HelloRequestFlat`를 배치한다.
2. `id`를 scalar로 기록한다.
3. `name`이 있으면 arena 뒤쪽에 문자열 payload를 복사하고 `offset/len` 저장
4. `scores`가 있으면 arena 뒤쪽에 배열 payload를 복사하고 `offset/count` 저장
5. 최종 flat message 길이를 계산한다.

선택 정책 hook:
- `grpc_selective_policy_decide_string()`
- 현재는 `name`에 대해 threshold 정책을 적용하고 stats를 기록한다.

즉, selective-offload는 아직 scaffold 수준이지만, decision 위치는 lowering 시점으로 잡혀 있다.

---

## 6. 실행 경로 1: RPC batch path
이 경로는 ring-worker를 쓰지 않는 기본 batch offload 경로다.

핵심 함수:
- [host_agent.cc](/home/jihoon/DPUMesh/gRPC/host_agent.cc)의 `grpc_proto_serialize_hello_batch()`
- [dpa_batch_worker.c](/home/jihoon/DPUMesh/gRPC/dpa_batch_worker.c)의 `grpc_dpa_submit_batch()`
- [grpc_dpa_kernel.c](/home/jihoon/DPUMesh/gRPC/grpc_dpa_kernel.c)의 `grpc_dpa_serialize_batch_rpc()`

동작 순서:
1. Host가 `HelloRequest` batch를 flat task 배열로 변환한다.
2. 각 task payload를 `doca_dpa_h2d_memcpy()`로 DPA msg scratch에 복사한다.
3. `ProtoTask[]`를 DPA task array로 복사한다.
4. `grpc_dpa_serialize_batch_rpc()`를 호출한다.
5. DPA가 task별로 `grpc_wire_serialize_one()`을 수행한다.
6. Host가 `ProtoCompletion[]`을 `D2H`로 읽는다.
7. 성공 item만 output bytes를 `D2H`로 회수한다.

특징:
- 구현이 단순하다.
- H2D -> RPC -> D2H가 직렬화돼 있어 overlap은 약하다.
- persistent worker path와는 별개다.

---

## 7. 실행 경로 2: persistent ring-worker path
이 경로는 `GRPC_DPA_ENABLE_RING_LOOP`가 켜진 경우의 경로다.

현재는 single worker가 아니라 **sharded multi-worker** 구조로 바뀌어 있다.

### 7.1 shard model
- shard key: `request_id`
- shard 선택 함수: `grpc_pick_ring_shard()`
- Host는 batch를 shard별로 분해한 뒤 각 shard ring에 넣는다.

이 방식의 이유:
- deterministic routing
- non-contiguous request_id 지원
- 외부 API 변경 최소화

### 7.2 shard별 독립 자원
각 shard는 아래를 독립적으로 가진다.
- `GrpcRingCtrl`
- request ring
- completion ring
- msg pool
- out pool
- DPA thread

즉, shard 간 hot-path 공유 mutable state를 피한다.

### 7.3 Host submit 경로
핵심 함수:
- [host_agent.cc](/home/jihoon/DPUMesh/gRPC/host_agent.cc)의 `submit_batch_via_dpa_ring()`

동작:
1. batch task에 대해 `GrpcShardDispatchPlan` 생성
2. 각 task의 shard 결정
3. shard별 req ring slot 확보
4. task payload를 해당 shard의 msg pool에 H2D 복사
5. shard req ring descriptor 갱신
6. shard ring control tail 갱신

실패 처리:
- ring full
- H2D 실패
- ring sync 실패
발생 시 그 item만 error completion으로 처리한다.
다른 shard item은 그대로 진행한다.

### 7.4 DPA worker 경로
핵심 함수:
- [grpc_dpa_kernel.c](/home/jihoon/DPUMesh/gRPC/grpc_dpa_kernel.c)의 `grpc_dpa_worker_main()`

동작:
1. 자기 shard의 req ring / cpl ring / pool만 참조
2. burst budget 내에서 req를 여러 개 소비
3. 각 요청에 대해 `grpc_wire_serialize_one()` 호출
4. 결과를 shard-local out pool에 기록
5. completion ring에 결과 기록
6. `doca_dpa_dev_thread_reschedule()` 호출

특징:
- single worker serial path가 아니라 shard별 병렬 worker 구조
- 각 worker는 자기 shard 자원만 건드리므로 cross-shard lock이 없다.

### 7.5 Host completion 경로
동작:
1. Host는 shard별 cpl ring을 poll
2. completion은 shard별로 제각각 도착 가능
3. 최종 매칭은 `request_id -> task index` hash map으로 수행
4. 성공 item만 해당 shard out pool에서 output bytes를 D2H 복사
5. completion slot을 free 처리

중요한 점:
- completion 순서가 submit 순서와 같다고 가정하지 않는다.
- out-of-order across shards도 안전하게 처리한다.

---

## 8. shard-safe completion matching
기존 single-worker prototype의 취약점 중 하나는 completion ordering 가정이었다.

현재는 [grpc_shard_routing.cc](/home/jihoon/DPUMesh/gRPC/grpc_shard_routing.cc)의 아래 helper를 쓴다.
- `grpc_build_request_index_map()`
- `grpc_lookup_request_index()`

의미:
- batch 전체의 request_id를 hash map으로 등록
- shard 어느 쪽 completion이 먼저 와도 정확한 원 task index를 찾음

이로써 아래 문제가 사라진다.
- submit ordering == completion ordering 가정
- shard 간 completion interleaving 시 잘못된 output 매칭

---

## 9. runtime init / destroy

### 9.1 init
함수:
- `grpc_proto_runtime_init()`
- `grpc_proto_runtime_init_ex()`

현재 구조:
- compatibility wrapper: `grpc_proto_runtime_init()`
- 실제 shard 수를 받는 함수: `grpc_proto_runtime_init_ex(..., shard_count)`

init에서 하는 일:
1. DPA descriptor/task/completion shared objects 할당
2. Host scratch buffer 준비
3. descriptor blob push
4. shard 수만큼 아래를 반복
   - shard worker arg alloc
   - shard ring ctrl alloc
   - shard req/cpl ring alloc
   - shard msg/out pool alloc
   - host mirror ring vector 초기화
   - shard DPA thread create/start/run

### 9.2 destroy
- shard별 shutdown 기록
- shard별 DPA thread stop/destroy
- shard별 ring/pool memory free
- shared scratch/task/completion memory free

---

## 10. 테스트 구조

### 10.1 serializer correctness test
- [grpc_serializer_golden_test.cc](/home/jihoon/DPUMesh/gRPC/tests/grpc_serializer_golden_test.cc)

검증하는 것:
- fixed golden cases
- random/property cases
- canonical varint length correctness
- nested / packed repeated correctness
- specialized path / generic fallback correctness

실행:
```bash
bash /home/jihoon/DPUMesh/gRPC/build_grpc_serializer_tests.sh
```

### 10.2 shard functional test
- [grpc_sharded_worker_test.cc](/home/jihoon/DPUMesh/gRPC/tests/grpc_sharded_worker_test.cc)

검증하는 것:
- 2-shard dispatch correctness
- 4-shard dispatch correctness
- out-of-order completion matching correctness
- one-shard failure isolation

이 테스트는 실제 DPA HW 병렬 실행 성능을 보는 것이 아니라,
현재 host-side sharding 논리의 correctness invariant를 보는 테스트다.

---

## 11. 현재 한계
1. 실제 HW EU/core affinity pinning은 명시적으로 보장하지 않는다.
- logical sharding은 구현됨
- 하드웨어 pinning은 runtime에 맡김

2. shard별 pool capacity를 `max_batch` 기준으로 할당한다.
- skewed routing을 안전하게 처리하는 대신 메모리 사용량이 늘어난다.

3. RPC batch path는 그대로 단일 경로다.
- 이번 변경은 persistent ring-worker path 병렬화에 집중했다.

4. selective-offload는 아직 scaffold 수준이다.
- decision 위치는 lowering 시점에 잡혀 있지만
- 실제 zero-copy placement와 연결되지는 않았다.

5. specialized encoder는 sample 수준이다.
- `HelloRequest` fast path만 있음
- 다른 schema specialization은 아직 codegen 수준으로 확장되지 않음

---

## 12. 한 줄 요약
현재 구조는
- serializer correctness는 shared encoder 하나로 고정하고
- Host lowering은 flat object ABI로 단순화하며
- RPC batch path는 기존처럼 유지하고
- persistent ring-worker path는 `request_id` hash 기반 shard routing + shard별 독립 ring/pool/thread 구조로 병렬화한 상태다.

즉, 지금 repo는
- wire-format correctness
- shard-safe completion correctness
- future specialization / selective-offload 확장성
을 분리해서 유지하는 방향으로 정리되어 있다.
