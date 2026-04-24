# gRPC DPA serializer staged path 패치 계획

## 1. 범위
이 문서는 현재 `dpa_batch_worker.c`와 `grpc_dpa_kernel.c`를 중심으로, 현재 prototype을 더 올바르고 reviewable한 staged DPA serializer path로 정리하기 위한 패치 계획이다.

이번 계획의 목표는 다음과 같다.

- protobuf / gRPC wire-format correctness 강화
- host-side orchestration 정리
- selective-offload policy 추상화 추가
- DPA-friendly execution structure 정리
- 향후 specialized encoder path를 위한 hook 분리

이번 단계는 **계획 문서**만 정리하며, 실제 코드 패치는 다음 단계에서 수행한다.

---

## 2. 현재 실행 경로
### 2.1 RPC batch path
현재 기본 경로는 사실상 아래 순서다.

1. Host가 `ProtoTask[]`를 준비한다.
2. `copy_task_payloads_to_dpa()`가 각 task payload를 순차적으로 H2D 복사한다.
3. Host가 `ProtoTask[]` 자체를 다시 H2D로 복사한다.
4. `doca_dpa_rpc(grpc_dpa_serialize_batch_rpc)`를 호출한다.
5. DPA가 `grpc_dpa_serialize_batch_rpc()` 안에서 `for (i=0; i<num_tasks; ++i)`로 batch를 순차 처리한다.
6. Host가 `ProtoCompletion[]`를 D2H로 복사한다.
7. Host가 각 성공 item의 encoded output을 D2H로 다시 복사한다.

즉 현재 path는 다음 synchronous chain이다.

- payload H2D
- task-array H2D
- RPC
- completion D2H
- output D2H

### 2.2 Persistent ring-worker path
`GRPC_DPA_ENABLE_RING_LOOP`를 켜면 별도 resident worker path가 있다.

1. Host가 request ring에 `GrpcReqDesc`를 적재
2. DPA global thread `grpc_dpa_worker_main()`이 ring을 poll
3. request 하나를 꺼내 `dpa_serialize_one()` 수행
4. completion ring에 `GrpcCplDesc`를 적재
5. Host가 completion ring을 poll하고 encoded output을 D2H로 가져옴

하지만 이 경로도 실제로는 다음 문제가 있다.

- 한 번에 request 1개씩 처리
- 내부 serializer는 generic field-loop + switch 기반
- task submit 에러 처리와 completion semantics가 충분히 명확하지 않음
- staged overlap/pipeline이 거의 없음

---

## 3. 확인된 문제와 판단
### 3.1 Host-side 문제
#### A. synchronous H2D -> RPC -> D2H 체인
**확인됨.**
현재 `grpc_dpa_submit_batch()`는 stage overlap 없이 완전히 동기식이다.

문제점:
- payload copy, RPC 실행, completion copy, output copy가 직렬화됨
- batch pipeline/overlap 여지가 없음
- latency hiding 불가

판단:
- 이번 패치에서는 full async pipeline 구현까지는 가지 않고,
- RPC batch path와 ring-worker path를 명확히 분리하고,
- 후속 stage overlap을 위한 interface를 먼저 정리하는 것이 적절함

#### B. `grpc_dpa_batch_loop()`가 submit 에러를 무시
**확인됨.**
```c
(void)grpc_dpa_submit_batch(ctx, tasks, n, cpls);
```
- 실패해도 completion push를 계속 시도함
- `cpls[]`가 초기화되지 않은 상태일 위험이 있음

판단:
- 즉시 수정 필요
- 실패 시 completion status를 명시적으로 채워 넣고 push하거나,
- batch 전체를 에러 completion으로 정규화해야 함

#### C. `MAX_BATCH` vs runtime `max_batch` 불일치
**확인됨.**
- `dpa_batch_worker.c`는 `#define MAX_BATCH 64`
- runtime은 `ctx->worker_arg.max_batch`를 별도로 가짐

위험:
- host runtime이 64보다 큰 값을 허용하면 ring loop가 truncation 또는 overflow 위험
- 코드 review 관점에서 의도 파악이 어려움

판단:
- compile-time constant와 runtime max를 분리해야 함
- worker stack allocation도 runtime max와 연동 가능하도록 바꿔야 함

#### D. selective copy vs offload policy 부재
**확인됨.**
현재는 문자열/bytes/packed 등 모든 field를 일괄 serialize 대상으로 본다.

판단:
- 이번 패치에서 완전한 Cornflakes-style object model까지는 어렵다
- 하지만 최소한 host lowering 단계에 policy abstraction을 도입해야 함
- threshold 기반 copy/zero-copy decision hook만 먼저 scaffold

### 3.2 Device-side 문제
#### E. mixed LEN-delimited encoding path 불일치
**확인됨.**
현재 device file에는 LEN-delimited 계열이 세 가지 방식으로 섞여 있다.

- string/bytes: canonical varint length
- nested message: 5-byte 공간 reserve 후 `backfill_len_fixed32()`
- packed repeated: 4-byte fixed32 length

이것은 protobuf wire-format 관점에서 일관되지 않다.

판단:
- **confirmed bug**
- protobuf wire-format 기준으로 LEN-delimited는 canonical varint length가 맞다
- fixed32/fixed5는 prototype 최적화 실험으로는 이해되지만 기본 경로로는 부적절

#### F. nested-message backfill correctness
**확인됨.**
`FK_MESSAGE`는 tag 뒤에 5바이트 reserve 후, 실제 backfill은 `backfill_len_fixed32()`를 호출한다.
즉 5바이트 reserve / 4바이트 backfill 조합이다.

문제:
- protobuf canonical encoding과 맞지 않음
- reserve size와 backfill encoding 규약이 불일치
- encoded payload length 계산과 nested correctness를 reviewer가 신뢰하기 어렵다

판단:
- **confirmed bug**
- nested는 canonical LEN varint backfill로 통일해야 함

#### G. `grpc_dpa_serialize_batch_rpc()`의 serial batch loop
**확인됨.**
현재는 batch 전체를 순차 loop로 처리한다.

판단:
- correctness 기준에서는 허용 가능
- 다만 stage 분리를 위해 RPC batch path와 persistent worker path를 분리하는 것이 우선
- multi-thread/sharded execution은 이번에는 scaffold만 추가

#### H. persistent worker가 one-request-at-a-time
**확인됨.**
`grpc_dpa_worker_main()`은 request 하나를 consume하고 바로 reschedule한다.

판단:
- batch dequeuing / bounded burst consume가 필요
- full sharding까지는 하지 않더라도, worker가 한 번에 여러 request를 처리할 수 있는 구조 hook 필요

#### I. `find_desc + field-loop + switch` 오버헤드
**확인됨.**
현재 DPA serializer는 generic interpreter 구조다.

판단:
- generic fallback으로는 유지
- specialized encoder stub hook을 명시적으로 분리해야 함
- dispatch layer를 분리해 reviewability를 높이는 것이 맞음

#### J. explicit memory-placement policy 부재
**확인됨.**
현재 `ProtoTask`와 device serializer는 message/data placement 의미를 명시하지 않는다.

판단:
- `copy vs external ref` 정도의 host-side placement metadata는 먼저 추가해야 함
- DPA는 ABI 내부 해석기가 아니라 emit engine이 되어야 함

---

## 4. wire-format 관련 결론
### 4.1 confirmed bugs
다음은 실제 버그로 보고 수정해야 한다.

1. packed repeated length를 fixed32로 쓰는 기본 경로
2. nested message length를 fixed32로 backfill하는 기본 경로
3. nested message의 reserve size(5)와 backfill 규약(4-byte fixed32) 불일치

### 4.2 verified/disproved suspicions
- `string/bytes` payload copy loop 자체는 정상 동작으로 보임
  - `put_varint_checked()`는 length/tag encode 용이고, payload 바이트 복사는 별도 loop가 필요한 구조다
- `bool` field는 현재 단순 `1`만 쓰므로 encoding 자체는 문제 없음
- grpc header 5-byte wrapping 자체는 현재 구현 의도와 일치함

### 4.3 기본 방향
- 기본 serializer path는 protobuf canonical LEN varint encoding으로 통일
- fixed-width backfill 실험은 debug/experimental path로만 남기거나 제거
- wire-format golden test를 우선 추가

---

## 5. 제안 아키텍처
### 5.1 path 분리
현재는 RPC batch path와 resident ring-worker path가 섞여 보인다. 다음처럼 분리하는 것이 좋다.

1. **Generic RPC batch path**
- correctness/fallback path
- host lowers tasks
- DPA generic emit
- fully synchronous allowed

2. **Persistent worker path**
- datapath-oriented path
- request ring / completion ring
- bounded burst consume
- future sharding hook

이 둘은 공용 emitter를 쓰되 orchestration은 분리한다.

### 5.2 serializer layer 분리
현재 `grpc_dpa_kernel.c` 내부를 다음 계층으로 나누는 것이 적절하다.

1. length/varint helpers
2. generic field emitters
3. generic message interpreter
4. specialized dispatch hook
5. RPC entry / persistent worker entry

### 5.3 selective-offload scaffolding
이번 패치에서는 최소한 아래 abstraction을 추가한다.

- `FieldPlacementPolicy`
- `FieldPlacementDecision`
- threshold 기반 default policy
- string/bytes field에 대해
  - copy
  - external ref / zero-copy candidate
  두 표현을 담을 수 있는 host-side metadata scaffold

이번 단계에서는 DPA zero-copy full path를 구현하지 않더라도,
**policy를 lowering 단계에 적용하는 위치**는 먼저 고정한다.

---

## 6. 패치 그룹 제안
### Patch Group 1. Wire-format correctness + golden tests
범위:
- `grpc_dpa_kernel.c`
- 새 host reference serializer / golden tests

내용:
- LEN-delimited encoding을 canonical varint로 통일
- nested message backfill 수정
- packed repeated length encoding 수정
- sample messages에 대해 byte-for-byte golden test 추가

리뷰 포인트:
- 작은 patch로 쪼갤 수 있음
- correctness 확보 우선

### Patch Group 2. Host-side submit/completion/error propagation 정리
범위:
- `dpa_batch_worker.c`
- `host_agent.cc`
- 필요한 경우 `dmesh_grpc_api.cc`

내용:
- `grpc_dpa_batch_loop()` submit error 무시 제거
- batch item별 completion status propagation 정리
- `MAX_BATCH`와 runtime `max_batch` 정렬
- mixed batch 실패 semantics 정리

리뷰 포인트:
- orchestration correctness 개선
- behavior가 명확해짐

### Patch Group 3. Execution path 분리와 staged hooks 추가
범위:
- `dpa_batch_worker.c`
- `grpc_dpa_kernel.c`
- `host_agent.cc`

내용:
- RPC batch path 함수와 persistent worker path 함수 명확히 분리
- persistent worker의 bounded burst consume 추가
- future async pipeline/sharding hook 주석 및 interface 추가

리뷰 포인트:
- full rewrite 없이 구조를 읽기 쉽게 만듦

### Patch Group 4. Selective-offload policy scaffold
범위:
- `host_agent.h/.cc`
- 필요 시 `proto_meta.h`
- 새 policy header/source

내용:
- threshold-configurable policy abstraction 추가
- string/bytes field placement decision hook 추가
- 현재는 copy fallback 유지, external ref metadata만 scaffold

리뷰 포인트:
- 아직 full zero-copy는 아님
- 이후 specialization / Cornflakes-style 확장을 위한 토대

### Patch Group 5. Specialized encoder hooks
범위:
- `grpc_dpa_kernel.c`
- 새 dispatch header/source

내용:
- `find_desc + field-loop + switch` generic path는 fallback으로 유지
- message-specific specialized encoder 등록 hook 분리
- sample `HelloRequest` specialized stub 추가

리뷰 포인트:
- 성능 path를 명확히 분리
- generic fallback 유지

### Patch Group 6. Docs / architecture note / TODO boundary
범위:
- 새 md 문서
- 테스트 README

내용:
- current execution paths
- confirmed bugs
- verified/disproved suspicions
- what is fixed now vs scaffolded
- remaining TODOs

---

## 7. 테스트 계획
### 7.1 필수 테스트
1. scalar only
2. string / bytes
3. packed repeated u32
4. packed repeated u64
5. nested message
6. mixed batch with success/failure
7. non-contiguous request_id bookkeeping

### 7.2 golden validation 방식
이상적인 기준은 trusted host protobuf serializer다.

현재 repo 환경에서 protobuf runtime availability를 먼저 확인해야 한다.

- 가능하면 standard protobuf serializer와 byte-for-byte compare
- 환경 제약이 있으면 host reference serializer를 intermediate oracle로 두고, 이후 protobuf-backed golden test를 추가

### 7.3 path별 테스트
- generic RPC batch path
- persistent worker path
- specialized stub path

---

## 8. 지금 당장 수정해야 하는 우선순위
1. wire-format bug 수정
2. `grpc_dpa_batch_loop()` 에러 전파 수정
3. `MAX_BATCH` / runtime max_batch 정리
4. RPC path vs ring-worker path 분리
5. selective-offload policy scaffold
6. specialized encoder hook scaffold

---

## 9. 이번 계획의 구현 원칙
- 시스템 전체 재작성 금지
- 작은 패치 그룹으로 분리
- correctness 먼저, performance scaffold는 그 다음
- generic fallback 유지
- specialization path는 hook만 분리하고 과도한 구현은 다음 단계로 넘김
- DPA가 protobuf object ABI 내부 해석기가 되지 않도록 유지

---

## 10. 다음 단계에서 제출할 최종 보고서 구조
실제 패치 후 최종 보고서는 아래 구조로 정리한다.

1. current execution paths
2. confirmed bugs
3. verified/disproved suspicions
4. patch groups
5. changed files
6. what is fixed now
7. what is scaffolded
8. remaining TODOs
