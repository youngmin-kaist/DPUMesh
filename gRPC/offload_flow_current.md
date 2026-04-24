# gRPC Serialization Offload Flow (Current Version)

이 문서는 현재 `DPUMesh/gRPC` 코드의 serialization offload 흐름을 코드 기준으로 정리한다.

## 1. 구성 요소

- Host/ARM Control Plane
  - `host_agent.cc`: 런타임 초기화/종료, 요청 flatten, 배치 submit, 결과 수집
  - `dpa_batch_worker.c`: Host에서 DPA RPC submit + h2d/d2h memcpy
  - `offload_benchmark.cc`: 벤치 workload 생성/반복 측정
  - `grpc_bench_main.cc`: 디바이스 오픈, DPA create/start, runtime init, benchmark 실행
- DPA Device Plane
  - `grpc_dpa_kernel.c`: protobuf field encoding, gRPC header wrapping, batch RPC

## 2. 데이터 모델

- Proto metadata
  - 파일: `proto_meta.h`
  - `ProtoDescBlob`, `ProtoMessageDesc`, `ProtoFieldDesc`로 메시지 스키마를 DPA에 전달
- Task/Completion
  - `ProtoTask`: 입력 메시지/출력 버퍼 주소 및 길이
  - `ProtoCompletion`: `request_id`, `encoded_len`, `status`
- Runtime 컨텍스트
  - 파일: `host_agent.h`
  - `GrpcProtoRuntime`가 DPA 메모리 주소, scratch pool, offload context를 보유

## 3. 초기화 흐름

### 3.1 프로세스 초기화

1. `grpc_bench_main.cc`
   - `doca_dev_open` (PCI 기반)
   - `doca_dpa_create`
   - `doca_dpa_set_app(DPU_mesh_dpa_app)`
   - `doca_dpa_start`
2. `grpc_proto_runtime_init()` 호출

### 3.2 Runtime 초기화 (`grpc_proto_runtime_init`)

1. DPA 메모리 할당
   - `GrpcDpaWorkerArg`
   - `ProtoDescBlob`
   - `ProtoTask[max_batch]`
   - `ProtoCompletion[max_batch]`
   - `msg scratch pool` (slot 기반)
   - `out scratch pool` (slot 기반)
2. Host scratch 벡터 준비
   - `host_flat_scratch`
   - `host_out_scratch`
3. `grpc_dpa_offload_init()`로 worker arg 기본값 DPA에 복사
4. `grpc_dpa_push_desc_blob()`로 descriptor blob DPA에 복사

### 3.3 Ring-loop 빌드 분기 (`GRPC_DPA_ENABLE_RING_LOOP`)

추가로 다음 단계가 실행됨:

1. DPA ring 메모리 할당
   - `GrpcRingCtrl`
   - `GrpcReqDesc[ring_depth]`
   - `GrpcCplDesc[ring_depth]`
2. `GrpcDpaWorkerArg`에 ring/pool 주소 및 slot 정보 반영 후 재복사
3. Host ring mirror 초기화 + DPA 메모리로 동기화
4. DPA thread 생성/시작/실행
   - `doca_dpa_thread_create`
   - `doca_dpa_thread_set_func_arg(..., grpc_dpa_worker_main, worker_arg_addr)`
   - `doca_dpa_thread_start`
   - `doca_dpa_thread_run`

## 4. 요청 처리 흐름

## 4.1 공통 전처리 (batch 요청 준비)

`grpc_proto_serialize_hello_batch()` 내부:

1. `prepare_batch_tasks()` 호출
2. 각 요청마다:
   - `FlatArena`로 `HelloRequest`를 flat 메모리로 직렬화
   - `ProtoTask` 채움
     - `desc_id`, `request_id`
     - host msg/out 주소
     - dpa msg/out slot 주소

`grpc_proto_serialize_hello()`는 위 batch API를 1건 래핑해서 사용.

## 4.2 Direct submit 경로 (기본)

컴파일 시 `GRPC_DPA_ENABLE_RING_LOOP`가 꺼져 있으면:

1. `grpc_dpa_submit_batch()` 호출
2. Host -> DPA
   - 각 task payload `doca_dpa_h2d_memcpy`
   - task array `doca_dpa_h2d_memcpy`
3. DPA RPC 실행
   - `doca_dpa_rpc(grpc_dpa_serialize_batch_rpc, worker_arg_addr, num_tasks)`
4. DPA -> Host
   - completion array `doca_dpa_d2h_memcpy`
   - 성공 task별 encoded bytes `doca_dpa_d2h_memcpy`
5. Host에서 `encoded_batch` 구성

## 4.3 Ring-loop 경로

컴파일 시 `GRPC_DPA_ENABLE_RING_LOOP`가 켜져 있으면:

1. Host enqueue
   - req ring slot에 `GrpcReqDesc` 작성 (`valid=1`)
   - 해당 msg slot으로 payload `h2d memcpy`
   - `req_tail` 갱신 후 `GrpcRingCtrl` 동기화
2. DPA resident thread (`grpc_dpa_worker_main`)가 poll
   - `req_head != req_tail` 확인
   - req desc consume
   - msg/out slot 주소로 serialization 수행
   - cpl ring slot에 `GrpcCplDesc` 작성 (`valid=1`)
   - `req_head`, `cpl_tail` 갱신
   - `doca_dpa_dev_thread_reschedule()`
3. Host completion poll
   - `cpl_head != cpl_tail` 될 때까지 ctrl d2h poll
   - cpl desc d2h
   - status 성공이면 out slot에서 encoded bytes d2h
   - cpl slot clear + `cpl_head` 갱신 동기화

## 5. DPA 인코딩 상세 (`grpc_dpa_kernel.c`)

### 5.1 엔트리

- RPC 엔트리: `grpc_dpa_serialize_batch_rpc(worker_arg_addr, num_tasks)`
  - `ProtoTask[]` 순회
  - 각 task마다 `dpa_serialize_one()` 실행

### 5.2 단건 인코딩

`dpa_serialize_one()`:

1. `desc_id`로 메시지 descriptor 조회
2. 출력 버퍼 앞 5바이트는 gRPC header 공간으로 예약
3. `emit_message()`로 필드별 protobuf encoding
4. payload 길이 계산 후 `put_grpc_header()`로 5바이트 header 작성
5. completion에 `encoded_len`/`status` 기록

### 5.3 필드 인코딩

`emit_field()`가 kind별 처리:

- `FK_U32`, `FK_U64`, `FK_BOOL`: varint
- `FK_STRING`, `FK_BYTES`: tag + len(varint) + raw bytes copy
- `FK_REPEATED_U32_PACKED`, `FK_REPEATED_U64_PACKED`:
  - packed payload를 element loop에서 varint로 연속 인코딩
  - length는 현재 fixed32 backfill 사용
- `FK_MESSAGE`: child message 재귀 인코딩

## 6. 종료 흐름 (`grpc_proto_runtime_destroy`)

1. Ring-loop 모드면:
   - `shutdown` 플래그를 ring ctrl에 반영
   - DPA thread `stop/destroy`
2. 모든 DPA 메모리 free
   - ring ctrl/req/cpl
   - scratch pools
   - worker arg / desc / task array / completion array
3. host scratch/ring 벡터 clear

## 7. 벤치마크 실행 흐름

파일: `offload_benchmark.cc`

1. 고정 `HelloRequest` 생성
2. warmup 반복
3. measure 반복
   - `submit_batch_size` 단위로 offload submit
   - 경과 시간 측정
   - success/fail/bytes 집계
4. latency percentile + throughput 계산

## 8. 컴파일 플래그 영향

- `GRPC_DPA_ENABLE_RING_LOOP`
  - OFF: Host가 매 submit 시 RPC 호출
  - ON: DPA resident thread + req/cpl ring 방식
- `GRPC_DPA_MAX_ENCODED_BUF_SIZE`
  - 메시지 1건당 out slot 최대 크기
- `GRPC_DPA_MAX_FLAT_MSG_SIZE`
  - 메시지 1건당 flat input slot 최대 크기

## 9. 요약

현재 버전은 다음 두 경로를 모두 지원한다.

1. **Direct Batch RPC**: 구현 단순, 동작 추적 용이
2. **Ring-loop Resident Thread**: host enqueue / DPA 상주 처리 / completion dequeue 구조

실험 시에는 동일 workload에서 `GRPC_DPA_ENABLE_RING_LOOP` ON/OFF를 비교해
- 요청당 고정 오버헤드
- tail latency
- req/s 변화를 확인하는 것이 권장된다.
