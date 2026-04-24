# DPUMesh/gRPC Serialization Offload 코드 Flow

이 문서는 현재 워크스페이스의 `DPUMesh/gRPC` serialization offload 흐름을 코드 기준으로 단계별 정리한 것이다.

작성 기준 코드:

- Standalone gRPC offload runtime: `gRPC/host_agent.cc`, `gRPC/dpa_batch_worker.c`, `gRPC/grpc_dpa_kernel.c`, `gRPC/grpc_wire_encode.c`
- DPUMesh demo integration: `gRPC/dmesh_grpc_api.cc`, `DPUMesh/host_worker.c`, `DPUMesh/comch_consumer.c`, `DPUMesh/grpc_demo_workload.c`
- 공통 데이터 구조: `gRPC/proto_meta.h`, `DPUMesh/comch_common.h`

주의: 이 문서는 코드 flow 문서이다. protobuf 호환성 자체는 `gRPC/tests/grpc_serializer_golden_test.cc` 같은 golden test 결과로만 주장해야 한다.

## 1. 전체 구조

현재 serialization offload는 크게 두 진입 경로가 있다.

1. Standalone benchmark 경로
   - `gRPC/grpc_bench_main.cc`
   - `grpc_proto_runtime_init()`
   - `grpc_proto_run_simple_benchmark()`
   - `grpc_proto_serialize_hello_batch()`
   - DPA RPC 또는 DPA resident ring worker

2. DPUMesh demo 통합 경로
   - Host `DPUMesh/host_worker.c`가 COMCH datapath 메시지로 batch request 전송
   - DPU/ARM `DPUMesh/comch_consumer.c`가 request를 수신
   - `gRPC/dmesh_grpc_api.cc`가 C API wrapper로 `GrpcProtoRuntime` 호출
   - DPA가 gRPC-framed protobuf bytes 생성
   - DPU/ARM이 metadata 또는 inline payload response를 Host로 회신

두 경로 모두 실제 gRPC serialization offload 핵심은 `GrpcProtoRuntime`과 DPA-side `grpc_wire_serialize_one()`에 모인다.

## 2. 핵심 데이터 모델

### 2.1 Host 입력 모델

파일: `gRPC/host_agent.h`

- `demo::HelloRequest`
  - `id_`: `uint64_t`
  - `name_`: `std::string`
  - `scores_`: `std::vector<uint32_t>`

DPUMesh C 경로에서는 `gRPC/dmesh_grpc_api.h`의 `dmesh_grpc_hello_request_view`가 같은 값을 C view로 넘긴다.

### 2.2 DPA flat input 모델

파일: `gRPC/proto_meta.h`

- `HelloRequestFlat`
  - `id`
  - `DpaStringRef name`
  - `DpaU32ArrayRef scores`

`DpaStringRef`, `DpaU32ArrayRef`는 DPA flat message base 기준 offset과 길이/count만 갖는다. Host는 실제 string/array bytes를 같은 flat arena에 복사하고, DPA는 offset으로 찾아 읽는다.

### 2.3 Descriptor 모델

파일: `gRPC/proto_meta.h`, 생성 위치: `gRPC/host_agent.cc::build_desc_blob()`

- `ProtoDescBlob`
- `ProtoMessageDesc`
- `ProtoFieldDesc`

현재 runtime 초기화 시 `desc_id = 1`인 `HelloRequest` descriptor가 DPA 메모리로 복사된다. 필드는 다음 순서다.

1. field 1: `id`, `FK_U64`
2. field 2: `name`, `FK_STRING`
3. field 3: `scores`, `FK_REPEATED_U32_PACKED`

### 2.4 Submit/Completion 모델

파일: `gRPC/proto_meta.h`

- `ProtoTask`
  - `desc_id`
  - Host input/output 주소
  - DPA input/output 주소
  - output capacity
  - `request_id`
- `ProtoCompletion`
  - `request_id`
  - `encoded_len`
  - `status`

`status == 0`이면 성공이고, 음수 값은 DPA encoder 또는 submit 경로 오류를 뜻한다.

## 3. Runtime 초기화 Flow

### Step 1. DOCA/DPA bootstrap

Standalone benchmark 기준 파일: `gRPC/grpc_bench_main.cc`

1. CLI에서 PCI BDF와 workload 옵션을 파싱한다.
2. `doca_devinfo_create_list()`로 device list를 얻는다.
3. `doca_devinfo_is_equal_pci_addr()`로 대상 PCI device를 찾는다.
4. `doca_dev_open()`으로 `doca_dev`를 연다.
5. `doca_dpa_create()`로 DPA context를 만든다.
6. `doca_dpa_set_app(dpa, DPU_mesh_dpa_app)`로 dpacc 산출 DPA app을 연결한다.
7. `doca_dpa_start()`로 DPA runtime을 시작한다.
8. `grpc_proto_runtime_init(&rt, dpa, nullptr, max_batch)`를 호출한다.

DPUMesh demo 통합 경로에서는 `gRPC/dmesh_grpc_api.cc::dmesh_grpc_offload_init()`이 이미 준비된 `objs->dpa_thread->dpa`와 `objs->dpa_thread->thread`를 받아 같은 runtime init을 호출한다.

### Step 2. `GrpcProtoRuntime` 기본값 설정

파일: `gRPC/host_agent.cc::grpc_proto_runtime_init_ex()`

1. `rt`, `dpa`, `max_batch`, `shard_count`를 검증한다.
2. `GrpcProtoRuntime`을 zero initialize한다.
3. `max_batch`, `ring_depth = max_batch + 1`, `shard_count`를 기록한다.
4. selective policy threshold를 설정한다.
   - `string_threshold = GRPC_DPA_SELECTIVE_STRING_THRESHOLD`
   - `bytes_threshold = GRPC_DPA_SELECTIVE_STRING_THRESHOLD`
5. `grpc_selective_policy_stats_reset()`으로 정책 통계를 초기화한다.

현재 `flatten_hello_request()`는 string placement decision을 기록하지만, 실제 zero-copy candidate라도 flat arena copy path를 사용한다. 즉, selective policy는 현재 flow에서 통계/결정 모델이며 실제 DMA zero-copy 전환은 아직 적용되지 않았다.

### Step 3. DPA memory allocation

파일: `gRPC/host_agent.cc::grpc_proto_runtime_init_ex()`

`doca_dpa_mem_alloc()`으로 다음 DPA-resident 버퍼를 할당한다.

1. `GrpcDpaWorkerArg`
2. `ProtoDescBlob`
3. `ProtoTask[max_batch]`
4. `ProtoCompletion[max_batch]`
5. flat input scratch pool
   - 크기: `GRPC_DPA_MAX_FLAT_MSG_SIZE * max_batch`
6. encoded output scratch pool
   - 크기: `GRPC_DPA_MAX_ENCODED_BUF_SIZE * max_batch`

Host side에는 같은 batch 크기 기준으로 다음 vector를 준비한다.

- `host_flat_scratch`
- `host_out_scratch`

### Step 4. DPA worker arg와 descriptor push

파일: `gRPC/dpa_batch_worker.c`, `gRPC/host_agent.cc`

1. `grpc_dpa_offload_init()` 호출
   - `GrpcDpaOffloadCtx`를 초기화한다.
   - `GrpcDpaWorkerArg`에 descriptor/task/completion array 주소와 `max_batch`를 채운다.
   - `doca_dpa_h2d_memcpy()`로 worker arg를 DPA 메모리에 복사한다.
2. `build_desc_blob()` 호출
   - 현재 `HelloRequest` descriptor를 host에서 생성한다.
3. `grpc_dpa_push_desc_blob()` 호출
   - `ProtoDescBlob`을 DPA 메모리로 복사한다.

## 4. Optional Ring-loop 초기화 Flow

컴파일 시 `GRPC_DPA_ENABLE_RING_LOOP`가 켜져 있으면 direct RPC submit 대신 DPA resident worker thread와 ring을 사용한다.

파일: `gRPC/host_agent.cc`, `gRPC/grpc_dpa_kernel.c`, `gRPC/grpc_shard_routing.cc`

### Step 1. Per-shard DPA resource 할당

`grpc_proto_runtime_init_ex()`는 `shard_count`만큼 `GrpcRingShardRuntime`을 만든다. 각 shard마다 다음 DPA memory를 따로 할당한다.

1. shard 전용 `GrpcDpaWorkerArg`
2. `GrpcRingCtrl`
3. `GrpcReqDesc[ring_depth]`
4. `GrpcCplDesc[ring_depth]`
5. shard 전용 message pool
6. shard 전용 output pool

### Step 2. Ring metadata 초기화

각 shard마다:

1. 공통 `rt->offload.worker_arg`를 복사한다.
2. shard id/count와 ring/pool 주소를 추가로 채운다.
3. `doca_dpa_h2d_memcpy()`로 shard worker arg를 DPA에 복사한다.
4. Host mirror인 `host_ring_ctrl`, `host_req_ring`, `host_cpl_ring`을 zero initialize한다.
5. ring ctrl/req/cpl mirror를 DPA에 복사한다.

### Step 3. DPA resident worker 시작

각 shard마다:

1. `doca_dpa_thread_create()`
2. `doca_dpa_thread_set_func_arg(shard.thread, grpc_dpa_worker_main, shard.dpa_worker_arg_addr)`
3. `doca_dpa_thread_start()`
4. `doca_dpa_thread_run()`

DPA thread entry는 `gRPC/grpc_dpa_kernel.c::grpc_dpa_worker_main()`이다.

## 5. Host Batch Prepare Flow

파일: `gRPC/host_agent.cc::grpc_proto_serialize_hello_batch()`

### Step 1. API validation

`grpc_proto_serialize_hello_batch()`는 다음을 확인한다.

1. `rt != nullptr`
2. `rt->max_batch != 0`
3. request list가 비어 있지 않음
4. request id 개수와 request 개수가 같음
5. `reqs.size() <= rt->max_batch`
6. host scratch vector 크기가 batch를 담기에 충분함

단건 API `grpc_proto_serialize_hello()`는 batch API를 request 1개짜리로 감싼 wrapper다.

### Step 2. `prepare_batch_tasks()`

파일: `gRPC/host_agent.cc`

각 request마다 다음을 수행한다.

1. batch slot `i`에 해당하는 `host_flat_scratch` 영역으로 `FlatArena`를 만든다.
2. request id 중복을 O(n^2)로 검사한다.
3. `flatten_hello_request()`를 호출한다.
4. `ProtoTask`를 채운다.

### Step 3. `flatten_hello_request()`

파일: `gRPC/host_agent.cc`

1. `FlatArena`에 `HelloRequestFlat` 객체를 배치한다.
2. `id` 값을 직접 기록한다.
3. `name`이 비어 있지 않으면:
   - `grpc_selective_policy_decide_string()`으로 placement decision을 만든다.
   - `grpc_selective_policy_stats_record()`로 통계를 누적한다.
   - 2GB bound를 넘는지 검사한다.
   - name bytes를 arena에 복사한다.
   - `flat->name.offset`, `flat->name.len`을 기록한다.
4. `scores`가 비어 있지 않으면:
   - `uint32_t` alignment에 맞춰 arena 공간을 잡는다.
   - scores bytes를 arena에 복사한다.
   - `flat->scores.offset`, `flat->scores.count`를 기록한다.
5. 최종 arena offset을 `msg_len`으로 반환한다.

### Step 4. `ProtoTask` 채우기

각 task는 다음 주소 관계를 가진다.

- `host_msg_addr`: host flat scratch slot
- `host_msg_len`: flat arena 사용량
- `host_out_addr`: host output scratch slot
- `host_out_cap`: `GRPC_DPA_MAX_ENCODED_BUF_SIZE`
- `dpa_msg_addr`: DPA msg scratch/pool slot
- `dpa_out_addr`: DPA output scratch/pool slot
- `dpa_out_cap`: `GRPC_DPA_MAX_ENCODED_BUF_SIZE`
- `desc_id`: 현재 `1`
- `request_id`: caller가 준 request id

## 6. Direct Batch RPC Submit Flow

조건: `GRPC_DPA_ENABLE_RING_LOOP`가 꺼진 build

파일: `gRPC/host_agent.cc`, `gRPC/dpa_batch_worker.c`, `gRPC/grpc_dpa_kernel.c`

### Step 1. Host가 `grpc_dpa_submit_batch()` 호출

`grpc_proto_serialize_hello_batch()`는 준비된 `ProtoTask[]`와 `ProtoCompletion[]`을 `grpc_dpa_submit_batch()`에 넘긴다.

### Step 2. Host input payload H2D copy

파일: `gRPC/dpa_batch_worker.c::copy_task_payloads_to_dpa()`

각 task마다:

1. `tasks[i].host_msg_addr`에서
2. `tasks[i].dpa_msg_addr`로
3. `tasks[i].host_msg_len` 바이트를 `doca_dpa_h2d_memcpy()`로 복사한다.

### Step 3. Task array H2D copy

파일: `gRPC/dpa_batch_worker.c::grpc_dpa_submit_batch_rpc_path()`

Host `ProtoTask[]` 전체를 `ctx->worker_arg.task_array_addr`로 복사한다.

### Step 4. DPA RPC 호출

Host는 다음 RPC를 호출한다.

```c
doca_dpa_rpc(ctx->dpa,
             (doca_dpa_func_t *)grpc_dpa_serialize_batch_rpc,
             &rpc_ret,
             ctx->dpa_worker_arg_addr,
             num_tasks);
```

DPA entry는 `gRPC/grpc_dpa_kernel.c::grpc_dpa_serialize_batch_rpc()`이다.

### Step 5. DPA batch loop

DPA RPC entry는:

1. `GrpcDpaWorkerArg`를 DPA 주소에서 읽는다.
2. descriptor blob 주소를 얻는다.
3. task array 주소를 얻는다.
4. completion array 주소를 얻는다.
5. `num_tasks <= max_batch`인지 검사한다.
6. 각 task마다 `grpc_wire_serialize_one(blob, &tasks[i], &cpls[i], NULL)`을 호출한다.
7. 모든 task 처리 후 `0`을 반환한다.

### Step 6. Completion D2H copy

Host는 DPA completion array를 `ProtoCompletion[]` host buffer로 복사한다.

### Step 7. Encoded output D2H copy

파일: `gRPC/dpa_batch_worker.c::copy_task_outputs_from_dpa()`

각 task마다:

1. completion `status != 0`이면 output copy를 건너뛴다.
2. `encoded_len > host_out_cap`이면 `DOCA_ERROR_INVALID_VALUE`를 반환한다.
3. DPA output slot에서 host output scratch slot으로 `encoded_len`만큼 `doca_dpa_d2h_memcpy()`를 수행한다.

### Step 8. Host vector 구성

`grpc_proto_serialize_hello_batch()`는 completion status가 성공인 task에 대해:

1. `host_out_scratch + i * GRPC_DPA_MAX_ENCODED_BUF_SIZE`
2. `encoded_len`

범위를 `encoded_batch[i]`에 복사한다. 실패 task는 빈 vector가 된다.

## 7. Ring-loop Submit Flow

조건: `GRPC_DPA_ENABLE_RING_LOOP`가 켜진 build

파일: `gRPC/host_agent.cc::submit_batch_via_dpa_ring()`, `gRPC/grpc_dpa_kernel.c::grpc_dpa_worker_main()`

### Step 1. Host shard dispatch plan 생성

1. `grpc_build_shard_dispatch_plan()`이 request id hash로 shard를 고른다.
2. `grpc_build_request_index_map()`이 completion request id를 원래 task index로 되돌리기 위한 map을 만든다.
3. request id가 중복되면 map 생성이 실패한다.

### Step 2. Host enqueue

각 shard의 task index 목록을 순회한다.

1. `next_tail = (req_tail + 1) % depth` 계산
2. `next_tail == req_head`이면 ring full로 보고 해당 task completion을 status `-100`으로 채운다.
3. 현재 `req_tail` slot의 `GrpcReqDesc`를 채운다.
   - `request_id`
   - `desc_id`
   - `msg_slot`
   - `out_slot`
   - `msg_len`
   - `out_cap`
   - `valid = 1`
4. host flat input payload를 shard DPA msg pool slot으로 `doca_dpa_h2d_memcpy()`한다.
5. req desc를 DPA req ring slot으로 복사한다.
6. host mirror의 `req_tail`을 갱신한다.
7. ring ctrl을 DPA로 복사한다.

### Step 3. DPA resident worker polling

`grpc_dpa_worker_main()`은 loop마다:

1. `__dpa_thread_window_read_inv()`를 호출한다.
2. `ctrl->shutdown`이 set이면 종료한다.
3. `processed < max_batch` 동안 burst 처리한다.
4. `req_head == req_tail`이면 처리할 request가 없으므로 break한다.
5. completion ring이 full이면 break한다.
6. `req_ring[req_head].valid == 0`이면 아직 publish되지 않은 것으로 보고 break한다.
7. `GrpcReqDesc`를 `ProtoTask`로 변환한다.
   - DPA msg 주소: `arg->msg_pool_addr + msg_slot * msg_slot_size`
   - DPA out 주소: `arg->out_pool_addr + out_slot * out_slot_size`
8. `grpc_wire_serialize_one()`을 호출한다.
9. `GrpcCplDesc`에 request id, encoded len, status, out slot, valid를 기록한다.
10. request desc valid를 0으로 clear한다.
11. `req_head`와 `cpl_tail`을 갱신한다.
12. loop 끝에서 `doca_dpa_dev_thread_reschedule()`을 호출한다.

### Step 4. Host completion poll

Host는 shard별로:

1. `completed_per_shard >= posted_per_shard`이면 해당 shard는 skip한다.
2. `cpl_head == cpl_tail`이면 DPA ring ctrl을 D2H copy하여 tail 변화를 poll한다.
3. completion이 있으면 `GrpcCplDesc`를 D2H copy한다.
4. request id map으로 원래 task index를 찾는다.
5. `ProtoCompletion`을 host completion vector에 기록한다.
6. status가 성공이면 DPA out pool에서 host output scratch로 `encoded_len`만큼 D2H copy한다.
7. cpl desc valid를 0으로 clear하고 DPA cpl ring slot에 반영한다.
8. host mirror의 `cpl_head`를 증가시키고 ring ctrl을 DPA로 복사한다.

## 8. DPA Encoder Flow

파일: `gRPC/grpc_wire_encode.c`

DPA RPC path와 ring-loop path 모두 최종적으로 `grpc_wire_serialize_one()`을 호출한다.

### Step 1. Descriptor lookup

1. `task->desc_id`로 `ProtoDescBlob.msgs[]`를 선형 검색한다.
2. descriptor가 없으면 completion status `-1`을 기록하고 실패한다.

### Step 2. Output capacity 검사

1. `task->dpa_out_cap < 5`이면 gRPC header를 쓸 수 없으므로 status `-2`를 기록한다.
2. output base는 `task->dpa_out_addr`이다.
3. payload emission 시작 위치는 `out_base + 5`이다.

### Step 3. Specialized encoder dispatch

`emit_message_dispatch()`는 다음 순서로 동작한다.

1. stats가 있으면 specialized attempt를 증가시킨다.
2. `desc_id == 1`이고 field count가 3이면 `emit_hello_request_specialized()`를 먼저 시도한다.
3. specialized encoder가 성공하면 그대로 반환한다.
4. 실패하거나 조건이 맞지 않으면 `emit_message_generic()`으로 fallback한다.

현재 DPA kernel은 stats 포인터를 `NULL`로 넘기므로 runtime 통계는 누적하지 않는다.

### Step 4. `HelloRequest` specialized encode

`emit_hello_request_specialized()`는 고정 schema를 직접 읽는다.

1. `id != 0`이면 field 1 varint tag와 value를 쓴다.
2. `name.len != 0`이면 field 2 length-delimited tag, len varint, raw bytes를 쓴다.
3. `scores.count != 0`이면 field 3 packed repeated tag를 쓴다.
4. scores packed payload length 자리는 최대 5바이트를 예약한다.
5. 각 score를 varint로 쓴다.
6. 실제 packed payload length를 varint로 compact backfill한다.

### Step 5. Generic fallback encode

`emit_message_generic()`은 descriptor field 순서대로 `emit_field_generic()`을 호출한다. 지원 field kind는 현재 다음과 같다.

- `FK_U32`
- `FK_U64`
- `FK_BOOL`
- `FK_STRING`
- `FK_BYTES`
- `FK_REPEATED_U32_PACKED`
- `FK_REPEATED_U64_PACKED`
- `FK_MESSAGE`

length-delimited nested message와 packed repeated field는 length를 나중에 compact backfill하기 위해 5바이트를 임시 예약한다.

### Step 6. gRPC 5-byte header 작성

payload emission이 끝나면:

1. `payload_len = out_end - (out_base + 5)` 계산
2. `payload_len > PROTO_MAX_LEN_DELIMITED`이면 status `-3`
3. `put_grpc_header(out_base, payload_len)` 호출
   - byte 0: compression flag `0`
   - byte 1..4: payload length big-endian
4. completion에 다음을 기록한다.
   - `request_id`
   - `encoded_len = payload_len + 5`
   - `status = 0`

따라서 DPA output은 protobuf payload만이 아니라 gRPC message frame 전체다.

## 9. DPUMesh Demo 통합 Flow

### Step 1. Host worker가 demo request 생성

파일: `DPUMesh/host_worker.c::send_demo_grpc_request()`, `DPUMesh/grpc_demo_workload.c`

1. demo bookkeeping counter와 seen bitmap을 초기화한다.
2. `build_demo_grpc_expected_request_ids()`로 기대 request id 목록을 만든다.
3. request id map에 `request_id -> expected index`를 넣는다.
4. 전체 batch를 `DMESH_GRPC_OFFLOAD_MAX_BATCH` 단위 chunk로 나눈다.
5. 각 chunk에 대해 `build_demo_grpc_batch_request_range()`를 호출한다.
6. `dmesh_comch_send_datapath_msg()`로 `DMESH_MSG_GRPC_HELLO_BATCH_REQ`를 DPU로 보낸다.

### Step 2. DPU/ARM consumer callback이 batch request 수신

파일: `DPUMesh/comch_consumer.c::consumer_recv_task_comp_cb()`

1. COMCH recv task에서 buffer data pointer와 length를 가져온다.
2. `type == DMESH_MSG_GRPC_HELLO_BATCH_REQ`이고 `objs->gcfg.mode == DPU_MODE`이면 `handle_demo_grpc_batch_req()`를 호출한다.
3. callback 끝에서 recv task를 다시 submit한다.

### Step 3. DPU/ARM이 request payload를 C view로 변환

파일: `DPUMesh/comch_consumer.c::handle_demo_grpc_batch_req()`

1. `dmesh_grpc_batch_req_get_layout()`으로 batch layout을 검증하고 desc/payload 위치를 얻는다.
2. batch count limit과 response capacity를 검사한다.
3. request id를 모아 `grpc_demo_cached_request_map`에 넣는다.
4. scores를 `grpc_demo_scores_scratch`에 복사한다.
5. 각 entry를 `dmesh_grpc_hello_request_view`로 변환한다.
   - `id`
   - `name` pointer와 length
   - `scores` pointer와 count
6. 각 output slot은 `grpc_demo_cached_encoded_buf + i * encoded_cap`으로 지정한다.

### Step 4. DPU/ARM C wrapper가 gRPC runtime 호출

파일: `gRPC/dmesh_grpc_api.cc::dmesh_grpc_serialize_hello_batch()`

1. `dmesh_grpc_hello_request_view[]`를 `std::vector<demo::HelloRequest>`로 변환한다.
2. request id 배열도 `std::vector<uint32_t>`로 복사한다.
3. `grpc_proto_serialize_hello_batch()`를 호출한다.
4. 성공 시 `encoded_batch`와 `ProtoCompletion[]`을 caller가 준 `dmesh_grpc_encoded_buf[]`로 복사한다.

이후 흐름은 5-8장과 동일하게 Host prepare, DPA submit, DPA encode, completion copy를 탄다.

### Step 5. DPU/ARM이 Host response metadata 작성

파일: `DPUMesh/comch_consumer.c::handle_demo_grpc_batch_req()`

1. `DMESH_MSG_GRPC_HELLO_BATCH_RESP` header를 만든다.
2. 각 item에 다음 metadata를 채운다.
   - `request_id`
   - `status`
   - `encoded_len`
   - `region_id`
   - `owner_id`
   - `offset`
   - `capacity`
   - `generation`
3. 성공 output은 DPU-side cache buffer 기준 offset/length로 기록된다.
4. `grpc_demo_legacy_bounceback`이 켜져 있으면 encoded bytes를 response payload에 inline으로 붙인다.
5. `dmesh_comch_send_datapath_msg()`로 batch response를 Host에 보낸다.
6. inline payload가 아니거나 item status가 실패이면 `DMESH_MSG_GRPC_VALIDATE_RESP`를 즉시 보낸다.
7. legacy bounceback mode의 성공 item은 여기서 validate response를 즉시 보내지 않는다. Host가 inline payload를 DMA ring에 publish한 뒤 DPU DPA DMA manager가 복사 완료를 알리면 `DPUMesh/dpa.c::dmesh_demo_validate_dma_completion()`에서 cache buffer와 DMA 결과를 비교하고 validate response를 보낸다.

### Step 6. Host가 response 처리

파일: `DPUMesh/comch_consumer.c::handle_demo_grpc_batch_resp()`

1. `DMESH_MSG_GRPC_HELLO_BATCH_RESP`를 수신한다.
2. header length와 `payload_len`을 검증한다.
3. 각 item의 request id가 기대 목록에 있는지 확인한다.
4. item status와 encoded length를 누적한다.
5. legacy bounceback inline payload mode이면:
   - inline encoded bytes를 host DMA ring staging buffer에 publish한다.
   - `dma_desc.valid = 1`로 DMA descriptor를 공개한다.
6. metadata-only mode이면:
   - region/owner metadata가 유효한지 확인한다.
   - 실제 encoded bytes copy는 inline payload path처럼 Host DMA ring으로 publish하지 않는다.
7. 모든 chunk를 받으면 `grpc_demo_response_received`를 set한다.

### Step 7. Legacy bounceback DMA validation

파일: `DPUMesh/device/dpa_kernel.c::poll_desc_ring()`, `DPUMesh/dpa.c::dmesh_demo_validate_dma_completion()`

legacy bounceback mode에서 성공 item은 다음 추가 경로를 탄다.

1. Host가 inline encoded bytes를 host DMA staging buffer에 복사한다.
2. Host가 DMA ring descriptor에 host mmap, address, size, request id를 채우고 `valid = 1`로 공개한다.
3. DPU DPA thread의 `poll_desc_ring()`이 host memory의 descriptor ring을 poll한다.
4. descriptor가 valid이면 DPA DMA copy를 submit한다.
5. DMA immediate data로 `COMCH_MSG_TYPE_DMA_COMPLETED`를 보낸다.
6. DPU/ARM `DPUMesh/dpa.c::dmesh_doca_dpa_msgq_recv_cb()`가 DMA completion을 받는다.
7. `dmesh_demo_validate_dma_completion()`이 DPU cache의 expected encoded bytes와 DMA destination buffer의 actual bytes를 비교한다.
8. 비교 결과를 `DMESH_MSG_GRPC_VALIDATE_RESP`로 Host에 보낸다.

### Step 8. Host가 validation/완료를 기다림

파일: `DPUMesh/host_worker.c::wait_for_demo_grpc_completion()`

Host는 다음 조건이 모두 만족될 때까지 PE를 progress한다.

1. 모든 batch response chunk 수신
2. 모든 request에 대해 validation response 수신
3. legacy bounceback mode이면 expected DMA completion 수만큼 DMA completion 수신

완료 후 `evaluate_demo_grpc_result()`가 missing response, validation failure, unexpected response, DMA publish failure를 검사한다.

## 10. 종료 Flow

파일: `gRPC/host_agent.cc::grpc_proto_runtime_destroy()`

1. Ring-loop build이면:
   - 각 shard ring ctrl의 `shutdown = 1`을 DPA로 복사한다.
   - 각 DPA thread를 `doca_dpa_thread_stop()` 후 `doca_dpa_thread_destroy()`한다.
   - shard별 out pool, msg pool, cpl ring, req ring, ring ctrl, worker arg를 free한다.
2. 공통 DPA memory를 free한다.
   - output scratch
   - input scratch
   - completion array
   - task array
   - descriptor blob
   - worker arg
3. Host scratch vectors와 host ring mirrors를 clear한다.

Standalone benchmark는 이어서 `doca_dpa_stop()`, `doca_dpa_destroy()`, `doca_dev_close()`를 호출한다.

## 11. 현재 코드상 중요한 분기

### Direct RPC path

- build flag: `GRPC_DPA_ENABLE_RING_LOOP` 없음
- 매 submit마다 payload H2D copy, task array H2D copy, DPA RPC, completion D2H copy, output D2H copy를 수행한다.
- 구현이 단순하고 추적하기 쉽다.

### DPA resident ring-loop path

- build flag: `GRPC_DPA_ENABLE_RING_LOOP`
- Host는 request ring에 enqueue하고 completion ring을 poll한다.
- DPA thread가 `grpc_dpa_worker_main()`에서 상주 polling한다.
- `GRPC_DPA_RING_SHARD_COUNT`가 1보다 크면 request id hash로 shard를 나눈다.

### DPUMesh legacy bounceback vs metadata-only

- `grpc_demo_legacy_bounceback = true`
  - DPU/ARM response에 encoded bytes가 inline으로 실린다.
  - Host는 inline bytes를 DMA ring staging buffer에 publish한다.
- `grpc_demo_legacy_bounceback = false`
  - response는 DPU cache region metadata 중심이다.
  - Host는 inline payload DMA publish를 건너뛴다.

## 12. 미해결 ownership 및 memory-ordering 가정

현재 코드 flow를 읽을 때 명시적으로 보아야 할 가정은 다음과 같다.

1. Direct RPC path
   - Host는 payload와 task array를 DPA에 복사한 뒤 RPC를 호출한다.
   - RPC return 이후 completion/output D2H copy를 수행한다.
   - 이 경로는 RPC 호출 경계가 serialization 완료 시점이라는 가정에 의존한다.

2. Ring-loop request publish
   - Host는 payload H2D copy, req desc H2D copy, ring ctrl H2D copy 순서로 publish한다.
   - DPA는 `__dpa_thread_window_read_inv()` 후 ctrl/head/tail/valid를 읽는다.
   - 별도 atomic acquire/release primitive는 보이지 않으므로, 현재 코드는 DOCA memcpy 순서와 DPA window invalidation이 visibility를 보장한다는 가정에 의존한다.

3. Ring-loop completion publish
   - DPA는 cpl desc를 쓰고 `ctrl->cpl_tail`을 갱신한다.
   - Host는 ring ctrl을 D2H copy해서 tail 변화를 본 뒤 cpl desc를 D2H copy한다.
   - completion desc와 tail 갱신의 관측 순서도 DOCA/DPA memory visibility 모델에 의존한다.

4. DPU cache output lifetime
   - DPUMesh metadata-only response는 encoded bytes가 `grpc_demo_cached_encoded_buf`에 남아 있다는 전제하에 region/offset/len/generation metadata를 보낸다.
   - Host 쪽에서 DPU cache slot을 명시적으로 release하는 ack flow는 이 문서 범위의 코드에는 보이지 않는다.
   - 다음 generation이 같은 scratch를 덮어쓰기 전까지 consumer가 metadata를 해석해야 한다는 lifetime 가정이 있다.

5. Legacy bounceback DMA ring ownership
   - Host는 inline payload를 host DMA staging buffer에 복사한 뒤 `dma_desc.valid = 1`로 publish한다.
   - descriptor consumer가 valid를 보고 소유권을 가져간다는 기존 DMA ring 계약에 의존한다.

## 13. 빠른 호출 순서 요약

Standalone direct RPC 기준:

```text
grpc_bench_main.cc
  -> grpc_proto_runtime_init()
    -> grpc_dpa_offload_init()
    -> grpc_dpa_push_desc_blob()
  -> grpc_proto_run_simple_benchmark()
    -> grpc_proto_serialize_hello_batch()
      -> prepare_batch_tasks()
        -> flatten_hello_request()
      -> grpc_dpa_submit_batch()
        -> copy_task_payloads_to_dpa()
        -> copy ProtoTask[] to DPA
        -> doca_dpa_rpc(grpc_dpa_serialize_batch_rpc)
          -> grpc_wire_serialize_one()
        -> copy ProtoCompletion[] to Host
        -> copy encoded outputs to Host
      -> build encoded_batch vectors
  -> grpc_proto_runtime_destroy()
```

Ring-loop 기준:

```text
grpc_proto_serialize_hello_batch()
  -> prepare_batch_tasks()
  -> submit_batch_via_dpa_ring()
    -> grpc_build_shard_dispatch_plan()
    -> write payload to shard msg pool
    -> write GrpcReqDesc
    -> publish req_tail via GrpcRingCtrl

grpc_dpa_worker_main()
  -> poll req_head/req_tail
  -> convert GrpcReqDesc to ProtoTask
  -> grpc_wire_serialize_one()
  -> write GrpcCplDesc
  -> advance req_head/cpl_tail

submit_batch_via_dpa_ring()
  -> poll cpl_tail
  -> copy GrpcCplDesc
  -> copy encoded output
  -> clear completion slot
  -> build encoded_batch vectors
```

DPUMesh demo 통합 기준:

```text
run_host_worker()
  -> send_demo_grpc_request()
    -> build_demo_grpc_batch_request_range()
    -> dmesh_comch_send_datapath_msg(DMESH_MSG_GRPC_HELLO_BATCH_REQ)

consumer_recv_task_comp_cb() on DPU_MODE
  -> handle_demo_grpc_batch_req()
    -> dmesh_grpc_batch_req_get_layout()
    -> build dmesh_grpc_hello_request_view[]
    -> dmesh_grpc_serialize_hello_batch()
      -> grpc_proto_serialize_hello_batch()
        -> DPA offload path
    -> dmesh_comch_send_datapath_msg(DMESH_MSG_GRPC_HELLO_BATCH_RESP)
    -> immediate DMESH_MSG_GRPC_VALIDATE_RESP for metadata-only or failed items
    -> legacy success validation later via DMA completion path

consumer_recv_task_comp_cb() on HOST_MODE
  -> handle_demo_grpc_batch_resp()
  -> legacy inline payload publish to host DMA ring
  -> handle_demo_grpc_validate_resp()
  -> wait_for_demo_grpc_completion()
  -> evaluate_demo_grpc_result()
```
