# DPUMesh native API 개편 계획

상태: 설계·구현 계획. 이 문서는 구현 완료를 뜻하지 않는다.

우선순위 수정(2026-09-25): 사용자 요청에 따라 channel/listen 분리, 명시적 listen,
비동기 connect/close 및 lifecycle event를 먼저 구현한다. TX reservation/submit 계약 개편,
RX lease API 및 일반적인 buffer 회수 개선은 후속 단계로 미룬다. 아래 TX/RX 계약은
후속 설계안이며 첫 lifecycle 변경의 공개 API 완료 조건이 아니다. 다만 close 자체의
안전한 transport 정리와 in-flight DMA 접근 종료 확인은 lifecycle 구현에 포함한다.
추가 결정: host↔DPU Comch는 channel당 하나로 공유한다. Flow별 Comch와 listener별
추가 Comch를 만드는 이전 제안은 폐기한다. Listener와 connection은 이 session 위의 논리 객체다.
현재 첫 구현 범위: channel당 Comch session과 flow ID/generation 기반 제어 메시지 분배만
반영한다. DPA thread, DMA ring 및 local MsgQ는 flow별로 유지한다. DPA/ring 공유 검토안은
후속으로 미룬다. 이번 transport 기반 단계는 공개 ABI 5를 유지하며, 명시적 listen과 공개
async connect/close 및 ABI 6은 이후 단계다. 현재의 동기 setup/close와 서비스 env 동작도
이 첫 단계에서는 유지한다.

기준: 2026-09-25, host repository `03e1b9c`, `linkerd2-proxy` submodule
`1d9e0b0963ee445f4ff94e232d0acc8f3ba351a4`. 기존 공개 ABI는 5다.

## 1. 목표와 초기 범위

DPUMesh는 노드별 DPU proxy를 통해 microservice 통신을 제공한다. Native API는
서비스를 대상으로 하는 reliable full-duplex byte stream, 비동기 progress,
등록 메모리의 명시적인 소유권을 제공한다. HTTP/2 framing과 RPC semantics는
기존 gRPC 계층에 둔다. DMA descriptor와 물리 connection slot은 구현 세부사항이다.

이번 개편의 기본 결정:

- Channel, EQ, 연결, 등록 TX pool, RX lease 모델을 유지한다.
- 공개 QP는 opaque `dmesh_conn_t`로 바꾸고 channel도 opaque로 만든다.
- 초기에는 channel당 listener 하나, listener가 지정한 EQ 집합, 자동 수락을 지원한다.
- 연결 하나는 하나의 EQ에 고정한다. RPC마다 connection을 만들지 않는다.
- TX는 library-owned buffer의 소유권 전달형으로 유지한다. 전송마다 완료 이벤트를
  강제하지 않는다. 공개 reservation 취소를 추가한다.
- Background progress thread는 필수로 만들지 않는다. EQ polling이 progress를
  수행하되 transport handshake나 ACK 대기로 polling을 장시간 막지 않는다.
- 공개 ABI 6으로 전환한다. ABI 5 구조체 layout을 유지하면서 내부 필드를 숨기려 하지 않는다.
- Native 예제, DMA benchmark, C++/Go gRPC 어댑터의 이전을 포함한다.

초기 범위에서 제외:

- POSIX preload의 기능 재설계와 ABI 6 이식. 아래 호환성 절차로 ABI 5를 분리한다.
- 명시적 accept/reject, 하나의 channel에서 복수 서비스 listener, EQ 간 연결 이동.
- 외부 application memory 등록, vectored zero-copy send, 매 send의 completion.
- 독립적인 half-close API. 첫 버전은 ordered full close를 제공한다.
- 32개 flow 제한 확대, block allocator 교체, gRPC 전체 경로의 zero-copy 전환.

이 항목들은 나중에 추가할 수 있도록 계약을 분리하되, 이번 완료 조건에는 넣지 않는다.

## 2. 구현 전에 고정할 계약

### 2.0 Channel당 단일 DOCA Comch session

현재 구현은 carrier flow/slot마다 Comch client와 PE를 생성하며, DPU는 Comch connection
포인터로 flow와 DPA thread를 찾는다. Backend spare도 public connection 생성 전에 Comch를
하나씩 소유한다. 이 일대일 관계를 다음과 같이 변경한다.

```text
Host channel ───── 하나의 Comch session ───── DPU channel session
  ├─ listener                                ├─ listener registry
  ├─ connection A / flow_id=A                 ├─ logical flow A
  └─ connection B / flow_id=B                 └─ logical flow B

Payload: flow별 DMA ring → flow별 DPA thread → flow별 destination window
Control: 공용 Comch의 LISTEN / OPEN / CLAIM / READY / CLOSE / ERROR
```

| API 객체/동작 | Comch 및 flow 처리 |
|---|---|
| channel_create | Device·공용 메모리와 Comch client/PE 하나를 생성. 서비스 등록은 하지 않음 |
| listen | 기존 channel session으로 서비스 등록. Comch 추가 생성 없음 |
| backend spare | 논리 flow와 기존 flow별 DMA/DPA 자원 준비. Comch 추가 생성 없음 |
| connect_async | flow ID를 할당하고 공용 Comch로 OPEN과 metadata 전송 |
| ACCEPTED | DPU의 CLAIM을 준비된 flow에 매칭하고 공개 connection으로 채택 |
| stop_listen_async | 등록 해제와 미수락 flow 정리. Channel Comch와 수락된 flow 유지 |
| close_async(conn) | 해당 논리 flow를 CLOSE하고 flow 자원만 정리 |
| channel 종료 | 모든 자식과 정리 작업이 끝난 뒤 Comch session을 종료 |

DPU Comch server는 기존처럼 proxy worker별로 공유한다. Comch 연결 하나가 host channel
session 하나를 나타내며, 이 session은 여러 logical flow를 갖는다. Session을 여는 것만으로
flow slot이나 DPA thread를 할당하지 않는다. OPEN이 flow별 DPA thread/ring을 준비한다.
기존 flow table 및 RX window 한도는 유지한다.

**Native payload와 Comch fast path의 분리:** 현재 `channel_conn_post()`는 DMA descriptor를
commit한다. Host Comch producer의 실제 send 시작 코드는 비활성화되어 있다. 새 native
경로에서는 이 원격 producer와 DPU consumer의 준비·대기 의존성을 함께 제거한다. Payload
TX block, flow별 DMA ring·destination/RX window와 공개 buffer API는 유지한다.
CPU↔DPA의 local Comch MsgQ도 기존처럼 flow별로 유지한다.

**식별과 메시지 분배:** protocol version, message type/length, session epoch,
listener/flow ID, generation 및 request ID를 포함한다. 수신한 Comch connection으로 session을
찾고 `(session, flow_id, generation)`으로 flow를 찾는다. Metadata/reverse export/READY/
CLOSE/ERROR 모두 같은 식별 체계를 사용한다. 하나의 `objs->flow`, `rev_msg` 등에 여러 flow의
정보를 덮어쓰지 않는다. DPA pool의 owner 키는 Comch 포인터 대신 logical flow로 둔다.
같은 Comch를 공유하는 flow들이 같은 DPA thread를 잘못 재사용하지 않게 하는 식별 변경이며,
DPA thread 수와 실행 모델을 변경하지 않는다.

**Progress 소유권:** channel에 control EQ 하나를 명시적으로 지정하고 그 EQ의 polling
스레드가 Comch PE와 send task를 단독으로 다룬다. 다른 EQ/스레드는 bounded control command
queue에 요청하고 owner를 깨운다. Callback은 결과를 대상 connection/listener EQ의 inbox에
전달하고 알린다. 여러 EQ가 동일 PE를 동시에 progress하지 않는다. Background thread는
추가하지 않으며, control EQ를 poll하지 않으면 lifecycle이 진행되지 않는 계약을 명시한다.
Control EQ 지정 API와 channel session 종료 절차를 L0에서 함께 고정하며, session의 정리가
끝나기 전에 control EQ를 파괴할 수 없도록 한다.

첫 transport 단계에서는 아직 이 공개 control EQ 계약을 도입하지 않는다. 기존 EQ 사용법을
유지하면서 channel mutex로 Comch PE/send/callback/flow table 접근을 직렬화하고, 공용
notification fd를 여러 EQ에 중복 등록하는 대신 기존 fallback tick으로 progress한다.
향후 async lifecycle 단계에서 위 단일 control EQ 모델로 옮긴다.

Comch client 연결 및 HELLO를 시작하는 단계와 완료를 기다리는 단계를 분리한다. Session이
준비되기 전에 수용한 listen/connect 요청은 bounded queue에 두고, 각 요청의 deadline에는
session 준비 시간도 포함한다. Session 준비 완료와 CONN_READY를 혼동하지 않는다.

**순서와 종료:** control message는 하나의 Comch를 공유하지만 payload DMA와 같은 완료
순서를 갖는 것은 아니다. CLAIM 처리와 ACCEPTED를 RECV 공개보다 앞세우며, 정상 CLOSE는
마지막 data sequence에 연결한다. 개별 flow의 오류/close에서 server_disconnect나 client stop을
호출하지 않는다. Comch session 장애는 channel 수준 오류로 처리하여 그 session의 모든
listener/flow에 실패를 전파하고 DMA 자원을 안전하게 정리한다. 초기에는 자동 재연결이나
기존 flow의 암묵적인 복구를 제공하지 않는다.

### 2.0.1 후속 검토 — Channel 공용 DPA thread/ring (현재 구현 제외)

아래 내용은 DPA 공유를 다시 검토할 때의 설계 참고이며 첫 Comch 단계의 요구사항이 아니다.

현재 kernel은 thread별 destination mmap/base, pos/rd_pos를 사용하며 completion callback도
하나의 conn에 묶여 있다. Descriptor에 flow ID만 추가해서는 여러 flow를 처리할 수 없다.
공유 범위는 Comch, submission ring, DPA thread, local MsgQ/completion 경로이며, 연결별
byte-stream 순서·destination·credit·오류/close 상태는 flow table에 둔다.

1. **다중 생산자 게시:** 여러 connection이 여러 스레드/EQ에서 제출하면 MPSC가 된다.
   첫 구현은 짧은 channel submit lock 안에서 credit 확인·차감, slot 예약, descriptor 작성과
   게시를 완료한다. 앱의 payload 작성이나 공간 대기에는 lock을 잡지 않는다. 만석이면
   EAGAIN을 반환한다. Atomic head 증가만으로는 예약 후 멈춘 producer의 미완성 slot을
   후속 producer가 공개하는 문제를 해결하지 못한다. Lock-free 전환은 후속 측정 과제다.

2. **Descriptor와 완료 식별:** flow ID/generation, flow 내 byte offset 또는 sequence,
   channel submission ticket, source offset/length, operation/flags를 구분한다. Session epoch는
   ring의 session binding으로 검증할 수 있다. Completion에도 flow/generation/ticket/status와
   destination segment 정보를 보존한다. Ring format은 version 및 alignment/크기를 검증한다.
   인접 source 주소라는 이유만으로 서로 다른 flow의 descriptor를 하나의 segment로 합치지 않는다.

3. **Flow context 설치:** OPEN은 DPA flow table의 destination, 범위, credit 및 generation이
   설치된 뒤 READY가 된다. Active descriptor가 참조하는 entry를 제자리에서 교체하지 않는다.
   Payload와 descriptor 게시, DPA invalidate/read, 완료·credit writeback의 가시성과 순서를
   SDK memory model에 맞게 정한다. CPU mutex/volatile만으로 PCIe 가시성을 보장하지 않는다.

4. **Head-of-line blocking과 credit:** flow A의 destination이 가득 찼다고 shared ring의
   consumer를 멈추면 B도 막힌다. 초기안은 실제 수신 공간을 보장하는 per-flow byte credit을
   publication 전에 예약한다. DMA 및 completion 자원 한도도 admission에 반영한다.
   Destination credit은 해당 수신자가 bytes를 반환한 후 돌려준다. Flow별 buffer만 유지하거나
   descriptor 개수만 제한하는 것으로는 FIFO 정체를 해결하지 못한다. 이 예약을 제공할 수 없다면
   bounded per-flow pending queue와 공정한 DPA scheduler가 필요하므로 별도 설계 없이 skip하지 않는다.

5. **공정성:** ring과 completion queue를 한 flow가 독점하지 않도록 per-flow outstanding
   byte/descriptor 한도와 제출 batch budget을 둔다. 서로 다른 flow 사이의 순서는 약속하지 않고
   같은 flow의 byte 순서를 유지한다. Admission 정책은 포화 상태의 tail latency로 검증한다.

6. **소비와 완료 분리:** descriptor slot은 필요한 metadata가 안전하게 복사·소비된 뒤 재사용한다.
   TX 원본은 실제 DMA source-safe 완료 뒤에만 재사용한다. Destination 공간은 수신자가 반환할
   때 재사용한다. 현재 consumer_head는 DMA 제출 뒤 전진하므로 세 조건을 하나의 head로 취급하지
   않는다. 순서가 다른 완료는 ticket별로 기록하고 안전한 완료 prefix 또는 flow별 completion으로
   회수한다. Data/완료 큐가 가득 찼을 때 조용히 버리는 경로는 허용하지 않는다.

7. **Flow close와 channel stop:** CLOSE는 마지막 flow sequence와 연결하고, DMA/늦은 완료가
   해당 flow를 더 이상 참조하지 않을 때 ID/generation과 destination을 재사용한다. Flow 하나의
   종료에 channel DPA thread의 stop flag를 사용하지 않는다. Thread/ring/MsgQ 정리는 channel
   종료에만 수행한다. Unknown/stale/out-of-range descriptor를 DMA에 제출하지 않는다.

8. **방향과 확장:** host→DPU ring과 DPU→host ring은 단방향 자원이다. `host-dpa` reverse는
   host 측에도 channel당 ring/DPA thread를 두는 별도 작업이다. `dpu-dma` reverse는 DPA thread가
   없는 경로이므로 완료 분배/flow control을 별도로 맞춘다. Shared ring의 producer cacheline과
   한 DPA thread의 descriptor 처리율은 병목 후보다. 여러 channel로 분산하는 방식을 유지하되,
   처음부터 flow마다 DPA thread를 다시 할당하지 않는다. 연결 한도 확대는 별도 작업이다.

### 2.1 서비스 연결과 readiness

`channel_create(opts)`는 device, 메모리와 공용 Comch session을 준비한다. `DPUMESH_SERVICE`를 읽고
자동으로 수신 서비스를 열지 않는다. 서비스 수신은 `listen()`의 명시적 인자로 지정한다.
Device/registry 등 배포 설정에는 환경변수 기본값을 계속 허용한다.

`connect_async(eq, service, opts, out_conn)`의 성공은 로컬 요청 수용을 뜻한다.
연결은 CONNECTING 상태이며, `CONN_READY` 이후에 TX를 허용한다. 요청 수용 전 실패는
handle이나 후속 event를 만들지 않는다. 수용된 요청은 READY 또는 ERROR로 귀결한다.

`CONN_READY`는 **DPU가 해당 서비스 stream을 수용하고 host가 TX를 시작할 수 있는 상태**다.
최종 backend application 연결 성립이나 remote receipt를 뜻하지 않는다. DPU의 L7
routing 및 HTTP/2 upstream pooling은 첫 요청에 의존할 수 있으므로, 최종 backend
연결을 기다리느라 첫 TX를 금지하는 순환 의존을 만들지 않는다.

Listener는 STARTING → LISTENING → STOPPING → STOPPED 상태를 갖는다.
`LISTEN_READY`는 DPU의 신규 stream 전달 준비 완료를 뜻한다. 비동기 실패는 ERROR로
보고하고 정리 후 LISTEN_STOPPED를 보낸다. 미리 연 backend flow와 실제 listener의
서비스 노출 상태를 분리한다.

`ACCEPTED`는 이미 생성·배정된 inbound connection이다. 별도 accept 호출은 없다.
그 connection의 첫 RECV/EOF/ERROR보다 ACCEPTED가 먼저 전달되어야 한다.
첫 payload가 없어도 DPU가 backend flow를 사용하기 시작한 사실을 control event로 알린다.
HTTP/2 stream 하나와 host transport connection 하나를 혼동하지 않는다.

`stop_listen_async()`의 수용 직후 로컬 admission을 닫고, DPU의 등록 해제와 미수락
flow 정리가 끝나면 LISTEN_STOPPED를 전달한다. 이미 수락된 연결은 유지한다.
반환된 batch에 포함된 ACCEPTED는 처리해야 하며, stop 이후 새 batch에는 해당 listener의
새 ACCEPTED를 추가하지 않는다. 경쟁해서 도착한 미수락 flow는 정리한다.
`listener_destroy()`는 LISTEN_STOPPED를 포함한 batch를 처리하고, 다른 EQ에서도 해당
listener를 참조하는 반환 batch 처리가 끝난 뒤 호출한다. Listener를 파괴해도 이미 수락된
connection은 유지한다. Listener event를 받을 EQ는 listen 시 지정한다. Comch progress를 담당하는 channel control EQ와는 구분한다.

### 2.2 TX reservation과 소유권

```text
reserve 성공 → 앱이 lease에 작성 → submit 성공 → library 소유 → source-safe 회수
                            └→ cancel → pool 반환
```

- `tx_reserve(conn, len, out_lease)`는 포인터, capacity, 검증 가능한 token을 반환한다.
- 초기에는 connection당 미제출 reservation 하나를 허용한다.
- `tx_submit(conn, lease, used, flags)` 성공은 전체 used bytes의 로컬 수용 및 소유권 이전이다.
- 수용 전 실패는 전송·소유권 이전·일부 수용이 없어야 한다. lease는 앱에 남는다.
- 수용 뒤 실제 제출 실패는 async ERROR로 보고한다. 같은 bytes를 다시 submit하지 않는다.
- `tx_cancel()`은 아직 수용되지 않은 reservation만 반납한다. 전송 취소/rollback이 아니다.
- submit/cancel 성공 시 전달한 lease를 무효화한다. reservation 하나는 한 번만 소비한다.
- zero-length submit을 cancel 대용으로 사용하지 않는다. 잘못된 인자는 lease를 보존한다.
- reserve 시 memory 및 bounded pending-work 용량을 확보하거나, submit에서 수용 전에
  용량을 검증한다. 성공 후 무제한 내부 큐에 쌓는 방식은 사용하지 않는다.
- `TX_READY`는 reserve가 EAGAIN으로 막힌 뒤 발생하는 one-shot 재시도 힌트다.
  공간 예약이나 송신 완료를 뜻하지 않는다.
- 기본 batching은 유지한다. `TX_FLUSH` flag 또는 `tx_flush()`는 partial tail의 보류를
  끝내고 제출을 촉진한다. DMA 완료까지 기다리는 fence가 아니다.

동일 connection의 TX 생산자는 앱이 직렬화한다. EQ 소비자와 다른 스레드일 수 있다.
내부 lock은 짧은 상태 전환만 보호하며, 앱이 serialize하는 동안 lock을 보유하지 않는다.
deadline progress가 활성 reservation을 건드리지 않도록 reservation 상태와 committed
prefix를 구분한다. close/destroy는 TX 생산자와 외부적으로 직렬화한다.

### 2.3 ACK와 memory 재사용

다음을 서로 다른 단계로 정의한다.

1. 요청/bytes가 라이브러리에 수용됨.
2. DMA와 transport가 TX 원본을 더 이상 참조하지 않음.
3. remote transport가 받음.
4. remote application이 소비함.

기본 API는 1을 반환값으로 알리고, 2를 내부 회수 기준으로 사용한다. 3/4를 주장하지 않는다.
`consumer_head`가 descriptor 소비인지 source-safe 완료인지 양방향 DMA 구현에서 검증한다.
제출 직후 head 전진만으로 안전하지 않다면 submission credit과 completion/reclaim cursor를
분리하고 실제 DMA completion으로 source-safe prefix를 전진시킨다.
Flush/메모리 writeback을 DMA completion의 대용으로 간주하지 않는다.

### 2.4 RX lease와 객체 수명

- RECV는 `(data, len, lease)`를 전달한다. 앱은 payload를 수정하지 않는다.
- RX lease는 event 배열 및 connection handle과 독립적으로 존재한다.
- `rx_release(channel, lease)`까지 bytes를 변경·재사용하지 않는다.
- release는 다른 스레드에서도 가능하게 한다. lease 자체의 동시 사용은 앱이 직렬화한다.
- lease는 단일 소유이며 복사해서 두 번 반납하지 않는다. 성공 후 원본 lease를 비운다.
  stale/복제 token은 검증하여 현재 세대의 buffer를 해제하지 않는다.
- 내부 token은 channel/slot generation과 batch identity를 검증한다. offset만으로 해제하지 않는다.
- connection이 닫혀도 outstanding RX lease가 있으면 window를 격리한다. 모든 lease가
  반환되고 DMA 접근도 끝나야 slot/window를 재사용한다.
- channel destroy는 EQ/listener/connection, TX reservation, RX lease, 비정리 DMA 자원이
  남아 있으면 EBUSY를 반환한다. 실패하면 handle과 소유권을 유지한다.
- release 순서는 자유롭지만 window credit은 연속 반납 prefix까지만 전진한다.

### 2.5 close와 오류

```text
CONNECTING → READY → CLOSING → CLOSED → conn_destroy
      └ ERROR ────────────────┘
```

`close_async(conn, opts)`는 신규 TX를 막고 수용된 bytes 뒤에 정상 종료를 배치한다.
미제출 reservation이 있으면 EBUSY를 반환하며 앱이 먼저 cancel하도록 한다.
close가 수용된 뒤에는 EQ progress로 종료를 진행한다. deadline 초과 시 ERROR를 알리고
강제 종료로 진행한다. 단, timeout만으로 DMA 안전 조건을 충족했다고 간주하지 않는다.

`abort_async()`는 미전송 bytes를 폐기하고 연결을 실패 종료한다. 이미 전달된 bytes를
되돌리지 않는다. outstanding reservation은 먼저 cancel해야 한다.
CONNECTING 상태에서는 abort로 연결 요청을 취소한다. READY 전환과 취소의 수용을
직렬화하고, 취소가 먼저 수용되면 이후 CONN_READY를 만들지 않는다. 이미 반환된 batch의
CONN_READY는 유효하며, 취소 뒤에는 취소 status의 ERROR와 정리 완료 시 CLOSED를 받는다.
Connect timeout도 같은 종료 경로를 사용한다.

ERROR는 원인과 대상 scope를 전달하며 terminal connection error는 CLOSING으로 전환한다.
최초 terminal error를 보존하고 동일 실패를 poll마다 반복해서 전달하지 않는다.
DMA 접근과 전송 작업의 정리가 안전하게 끝나면 CLOSED를 정확히 한 번 전달한다.
하드웨어 quiescence 실패 시 자원을 유지하며 CLOSED를 조기에 만들지 않는다.
연결 상태는 terminal이지만 자원이 아직 정리되지 않았다는 사실을 status로 구분한다.

CLOSED 뒤에는 새 connection event가 없다. 앱은 CLOSED를 포함한 poll batch를 모두 처리한
뒤 `conn_destroy()`한다. 미반환 RX lease는 connection destroy를 막지 않지만,
window 재사용과 channel destroy를 막는다.

정상 EOF와 reset/device/protocol 오류를 구분한다. EOF 앞의 수신 bytes는 먼저 전달한다.
연결 상실만 관찰한 경우 정상 종료라고 추측하지 않는다. 양쪽 방향을 독립적으로 닫는
half-close는 transport protocol이 지원되기 전에는 공개 계약에 넣지 않는다.

### 2.6 EQ와 이벤트

| 제안 이벤트 | 의미 |
|---|---|
| LISTEN_READY | listener의 DPU 등록 및 신규 stream 수신 준비 완료 |
| LISTEN_STOPPED | 신규 admission 중지와 listener 정리 완료 |
| CONN_READY | outbound host↔DPU 서비스 stream이 TX 가능한 상태 |
| ACCEPTED | inbound connection이 지정 EQ에 배정됨 |
| RECV | RX lease와 byte-stream 조각 |
| PEER_EOF | 정상적인 수신 종료 |
| TX_READY | EAGAIN으로 막혔던 TX 예약의 재시도 힌트 |
| ERROR | 대상·단계·오류 번호가 포함된 비동기 실패 |
| CLOSED | 해당 connection의 안전한 종료 처리 완료 |

Event에는 type, 대상 handle, 애플리케이션 context, status 및 타입별 payload를 둔다.
Status는 `errno`를 나중에 읽는 방식 대신 event 자체에 저장한다. Memory lease는
generation이 있는 별도 값으로 전달한다. 초기 API에서는 여러 동종 lifecycle 요청을
동시에 수용하지 않으므로 별도 per-send operation ID를 도입하지 않는다.

- EQ당 polling 소비자는 하나이며 서로 다른 EQ는 병렬 실행할 수 있다.
- EQ는 transport progress와 event dispatch를 담당한다. deadline은 progress가 실행될 때 처리된다.
- 한 poll의 work budget을 두고 무제한 RX 처리, handshake, backend 보충으로 독점하지 않는다.
- `eq_fd()`는 readiness용으로만 poll/epoll한다. 앱이 내부 fd에 raw read를 하지 않는다.
- Empty poll 시 library가 내부 drain/arm/recheck를 수행해 wakeup 유실을 방지한다.
- EQ destroy는 연결뿐 아니라 listener 참조와 미처리 lifecycle 작업도 검사한다.
- Listener의 EQ 집합은 처음에 고정하고, 그 EQ들만 ACCEPTED를 받을 수 있게 한다.
- 공용 Comch PE는 channel control EQ만 progress하며 결과를 각 대상 EQ로 전달한다.
- EQ/accept/inbox 포화 시 backpressure 또는 명시적 terminal error로 처리한다.
  데이터를 버리고 정상 reliable stream을 계속하는 경로를 없앤다.

## 3. 공개 API 변경 목록

이름은 ABI 6 header 작성 시 최종 고정한다. 기존 함수명의 기계적인 치환보다 위 계약을 우선한다.

| 현재 | ABI 6 계획 |
|---|---|
| 공개 channel/QP 구조체 | opaque channel/connection/listener, context accessor |
| create_channel() + 서비스 env | channel_create(opts) + 명시적 listen |
| 암묵적 backend 수신·정리 | listen → LISTEN_READY, stop_listen_async → LISTEN_STOPPED → listener_destroy |
| create_eq / poll_eq / eq_fd | 기본 역할 유지, readiness/progress 계약 통일 |
| create_qp(eq, service) | connect_async → CONN_READY / ERROR |
| CONN_REQ | ACCEPTED |
| alloc / post_send | tx_reserve / tx_submit / tx_cancel, 명시적 lease |
| flush | tx_flush 또는 submit flag, 수용과 완료 구분 |
| RECV + 내부 offset token | RECV + generation 검증 RX lease |
| release_rx_buffer(event) | event 수명과 독립적인 rx_release(channel, lease) |
| RECV_FIN / TX_ERROR | PEER_EOF / 범위와 오류 번호가 있는 ERROR |
| destroy_qp / abort_qp | close_async / abort_async → CLOSED → conn_destroy |
| pod/slot/block getter | caps/stats 조회 및 필요한 서비스 metadata accessor |

Options에는 struct size/version을 두고 기본값 생성 함수를 제공한다. 초기 public knobs는
TX pool budget, connection별 TX 한도, RX lease/연결 한도, listener backlog, connect/close
deadline으로 제한한다. 물리 block 크기, ring 수, DPA EU 수는 transport 내부 설정으로 둔다.
지원되지 않는 옵션은 명시적으로 거부하고 적용된 실제 한도는 caps query로 돌려준다.
`max_tx_reservation`과 `max_rx_fragment`는 조회값이며 고정 ABI 상수로 만들지 않는다.

## 4. 구현 단계와 완료 조건

### L0 — lifecycle 계약과 Comch 역할 고정

대상: 공개 header 초안, `design/API.md`, native transport interface, Comch protocol.

- Channel/listener/connection 소유권과 EQ 배정, 상태 전이 및 event 순서를 고정한다.
- Channel session HELLO/version, listener/flow ID와 generation, request ID 및 status를 정의한다.
- Control EQ 지정·polling 의무와 channel session 종료 및 control EQ 파괴 순서를 고정한다.
- 기존 공개 TX/RX 호출, buffer allocator 및 flow별 DPA/ring은 유지한다.
- 첫 transport 단계는 public ABI 5와 현행 lifecycle 호출을 유지하고 Comch session만 공유한다.
- Native/gRPC 호출자, ABI 5 baseline 및 기존 실패를 기록하고 새 ABI build를 분리한다.

완료 조건: Comch session과 logical flow의 생성/파괴 주체, 반환 batch의 handle 수명,
listen-stop/connect-cancel 경쟁 결과가 명시됨.

### L1 — channel 공용 Comch와 명시적 listener

대상: `src/core/dmesh_core.c`, `src/core/carrier.c`, host Comch, DPU server/registry.

- Channel 생성에서 service env 및 backend prewarm을 제거하고 명시적 listen으로 옮긴다.
- Channel 공용 Comch session을 추가하고 기존 per-flow client 생성·파괴를 제거한다.
- DPU의 session/flow table과 metadata 분배를 분리한다. DPA thread는 계속 flow별로
  배정하고, pool owner 키만 logical flow로 바꾼다.
- Native host의 원격 fast-path producer와 DPU consumer 초기화 의존성을 함께 제거한다.
  Local CPU↔DPA MsgQ와 DMA descriptor/completion format은 기존 flow별 구조를 유지한다.
- Listener 등록과 backend flow의 준비·공개를 분리한다.
- Backend spare 생성과 보충을 bounded incremental progress로 전환한다.
- LISTEN_READY, ACCEPTED, ERROR 및 stop/STOPPED를 구현한다.
- 첫 payload가 없어도 CLAIM을 통해 inbound connection을 통지한다.

완료 조건: channel 생성만으로 서비스가 노출되지 않음. listen 후 수신 가능하며,
stop 뒤 새 admission은 막히고 기존 연결은 유지됨. Spare 소진, control 단절,
CLAIM/STOP 경쟁, version mismatch, ACCEPTED 이전 data 도착을 검증.

### L2 — 비동기 outbound connect

대상: `src/transport/host/comch_client.c`, `channel.c`, carrier/core 및 DPU ready 응답.

- 공용 Comch session 준비와 flow별 OPEN/export/READY를 비동기 상태 머신으로 분리한다.
- Per-flow Comch RUNNING/remote-consumer 대기는 제거하고 실제 flow 준비 결과를 기다린다.
- connect_async의 요청 수용과 실제 연결 준비를 구분한다.
- CONN_READY는 flow의 host↔DPU 송신 준비를 뜻하며 최종 L7 backend 연결을 기다리지 않는다.
- Timeout/cancel/초기화 실패를 ERROR와 정리 경로로 연결한다.

완료 조건: 즉시 반환 후 EQ로 결과를 받음. 연결 하나의 지연이 같은 EQ의 다른 연결을
막지 않음. READY/cancel 경쟁과 실패 중 부분 초기화 자원 정리를 검증.

### L3 — 비동기 close/abort와 terminal event

대상: core close ACK 대기, carrier teardown, host/DPU transport 정리.

- Close 요청 수용, 정상 종료 교환, DOCA context 정리, handle destroy를 분리한다.
- 기존 data path의 수용된 bytes보다 정상 CLOSE가 앞서지 않도록 최종 data sequence와
  종료 제어를 연결한다. Comch 제어와 payload DMA의 상대적인 완료 순서에 의존하지 않는다.
- Flow close에서는 해당 flow의 DMA quiescence와 DPA/ring/MsgQ 정리를 확인한다.
  Channel 공용 Comch는 다른 flow를 위해 유지한다.
- Flow close/오류가 공용 Comch를 disconnect하지 않는지 확인한다. Session 종료는 별도 처리한다.
- ERROR 원인을 보존하고 정리 완료 시 CLOSED를 한 번 전달한다.
- Timeout만으로 자원을 free하지 않는다. 현재 TX/RX 계약을 적용하며 일반 lease API
  개편과 회수 정책 최적화는 후속으로 남긴다.

완료 조건: close/abort가 EQ를 장시간 막지 않음. 정상 EOF와 reset을 구분하고,
진행 중 DMA, 미반환 RX, peer crash, 초기화 도중 취소 시 premature free가 없음.
기존 buffer 수명 계약을 지키기 위한 최소 정리는 이 단계에 포함한다.

### L4 — EQ·공개 API·호출자 이전

대상: 공개 header/façade, EQ readiness, native 예제, benchmark, C++/Go gRPC.

- Lifecycle event의 typed payload/status, context 및 handle 접근을 정리한다.
- EQ당 단일 소비자, channel control PE의 단일 owner, bounded command queue와
  대상 EQ event 분배를 연결한다. Work budget 및 fd arm/recheck를 검증한다.
- Native 예제와 C++/Go adapter를 explicit listen 및 async connect/close로 이전한다.
- Go 등의 동기 wrapper는 호출자를 기다리게 할 수 있지만 EQ poller는 막지 않는다.
- ABI exports/SONAME 및 독립적인 legacy build를 검증한다.

완료 조건: max_events=1, 여러 EQ, 늦은 event, wakeup 경쟁, adapter deadline/cancel과
listener shutdown 테스트를 통과함. 기존 TX/RX 동작의 회귀가 없음.

### L5 — lifecycle 장비 검증

- 두 reverse mode에서 payload 없는 연결, server-first traffic, 연결 churn,
  listener restart, peer 종료, connect/close timeout 및 한도 도달을 검증한다.
- Channel당 Comch가 하나이며 flow 증감·listen restart가 Comch 수명을 바꾸지 않는지 확인한다.
- 같은 session의 flow A 종료 중 flow B 통신, 동시 OPEN의 metadata 분리, session 장애의
  전체 자식 실패 처리 및 여러 EQ 사이의 command/event 분배를 검증한다.
- 여러 flow의 OPEN/CLOSE 교차, 늦은 control reply와 generation 재사용 및 기존 flow별
  DMA 경로의 회귀를 검증한다. Shared ring 자체의 검증은 후속 DPA 공유 과제다.
- 연결 지연, EQ 응답성, idle 비용과 기존 통신 성능을 baseline과 비교한다.
- 장비 접근 불가 시 host-only 검증 결과만 기록하고 장비 검증을 완료로 표시하지 않는다.

### 후속 T/R — TX/RX 계약과 buffer 관리 개편

Lifecycle 변경 이후에 별도로 착수한다.

- TX reserve/submit/cancel, commit 전후 오류 경계, producer lock 범위 및 TX_READY.
- 일반 TX reclaim의 source-safe DMA completion/credit 기준 감사와 회수 정책 개선.
- Generation을 갖는 공개 RX lease, connection과 독립적인 release 및 window 격리.
- 위 변경의 fault test, production carrier 수명 test 및 성능 측정.

기존 문서 2.2~2.4의 제안은 이 후속 작업의 설계 입력이다. Lifecycle 작업 중 확인한
buffer 관련 문제는 별도로 기록하되, close의 안전한 자원 정리에 필수인 수정은 L3에 포함한다.

## 5. 호환성과 작업 분리

- ABI 6은 별도 `codex/native-api-v6` 작업 브랜치/worktree에서 구현한다.
- 과도기에는 ABI 6을 별도 header/build prefix/명시적 target으로 빌드하여 기존 ABI 5
  default build를 조기에 깨뜨리지 않는다. ABI fixture를 덮어써 양쪽을 혼동하지 않는다.
- Native·gRPC 이전 완료 후 `libdpumesh.so.6` 및 ABI 6 header를 새 native 기본값으로 전환한다.
- POSIX preload는 ABI 5 baseline checkout/header/library로 분리 빌드한다. ABI 6의 기본
  native build/test/examples에는 preload를 포함하지 않고 legacy target 사용을 명시한다.
  단순히 preload를 `libdpumesh.so.6`에 다시 링크하거나 기존 검증을 통과했다고 주장하지 않는다.
- Legacy build는 고정된 source revision, header, SONAME을 함께 선택하는 재현 가능한
  recipe를 저장소에 둔다. 개인 checkout에 의존하지 않으며 ABI 5 legacy와 ABI 6 native를
  별도의 CI 작업으로 검증한다. 변경된 DPU protocol과의 호환성도 별도로 명시한다.
- ABI 5와 ABI 6을 한 프로세스에 혼합 로드하는 방식은 지원하지 않는다. 자동 호환 shim은
  이번 개편의 완료 조건이 아니다. 필요하면 후속으로 의미 보존 가능한 shim을 설계한다.
- DPU control protocol이 바뀌는 단계는 host ABI 버전과 별개로 version/capability를 관리한다.
- `linkerd2-proxy`는 submodule이므로 DPU 변경 commit과 host-side pointer update를 함께 추적한다.

병렬 작업은 계약을 공유한 후 독립 worktree에서 수행한다. L0 이후 DPU 제어 처리와
host의 공용 Comch session 및 비동기 flow 준비를 나눌 수 있다. 공통 control record 및 `dmesh_core.c` 병합
담당은 하나로 정한다. Adapter 이전은 공개 lifecycle 계약과 fake transport가 고정된 뒤 진행한다.

순서는 `L0 → L1/L2 → L3 → L4 → L5 → 후속 T/R`이다. L1/L2는 공통 channel session과
control progress 구현을 공유하므로 해당 기반을 먼저 통합한다.

## 6. 첫 구현 작업

첫 구현은 L0/L1 중 **channel Comch와 logical flow 분리**까지만 수행한다. Public ABI 5와
flow별 DPA/ring을 유지한다. 그다음 명시적 listener를 연결하고 L2/L3에서 connect/close를
비동기 상태 머신으로 전환한다. TX/RX API 개편과 DPA/ring 공유는 후속 과제다.

현재 작업 브랜치는 `codex/channel-comch`다. Host의 channel session, DPU session/flow
분배와 HELLO/OPEN/READY/CLOSE/CLOSED 처리를 통합했다. Public ABI 5는 유지한다.
private Comch protocol과 DPA argument layout이 바뀌므로 host library, DPU transport/proxy,
DPA kernel을 함께 다시 빌드해야 한다.

종료는 kernel `stopped`만으로 성공 처리하지 않는다. 발행한 DMA 수와 실제 완료 메시지
수를 비교하고, CPU DMA task와 MsgQ/context의 정리 결과를 확인한다. 정리 실패 시 남은
자원·flow ownership을 보존해 재시도한다. Proxy의 CPU IO handle도 staging 접근을 중단한
뒤 transport에 확인한다. DPA thread/ring 공유는 도입하지 않았다.

Native DMA는 마지막 flush에 필요한 credit이 없어 종료가 막히지 않도록 각 제출에
FLUSH를 지정한다. 성능 영향은 장비 검증 항목이다. `DMESH_NO_TEARDOWN` 설정 시
resource-owning flow의 close는 오류를 반환하며 자원을 보존한다. 비정상 session 종료로
remote reverse reader의 종료를 확인할 수 없으면 해당 자원을 격리하며, 복구 전까지
flow/session 용량을 차지한다.

검증 완료: `make test`(host·ABI·session·종료 실패 주입),
`ninja -C src/transport/build`(transport·DPA kernel), proxy `cargo check`와 Rust 테스트
12개, 변경된 종료 경로의 ASan/UBSan 검사. Rust 테스트는 기존 SDK 링크 순서를 위한
추가 링크 인자가 필요했다. 명령과 검증 범위는 `tests/README.md`에 기록했다.
추가 장비 검증 완료(2026-09-25): Go wrapper 수정 후 x86 host, DPU, 전체 proxy를
빌드하고 `dpu-dma`/`host-dpa` 모두 실제 gRPC payload·sibling 격리·40회 flow 재사용·
channel 재생성을 검증했다. 총 1,586 RPC, logical flow 104개와 DPA thread 반환
104개를 확인했다. 첫 장비 실행에서 발견한 terminal disconnect 무한 재시도도 수정했다.
성능 비교는 수행하지 않았다. 명령·로그·검증 범위는
[장비 검증 보고서](2026-09-25_channel-comch-grpc-validation.md)에 기록했다.

관련 현행 문서: [API](../design/API.md), [host library](../design/HOST.md),
[gRPC](../design/GRPC.md), [tests](../tests/README.md).
