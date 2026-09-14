# HTTP/2 선택적 디코딩 릴레이 — 타당성 검토와 구현 플랜 (2026-09-09)

## 0. 한 줄 결론

- **타당하다. 단, "클라이언트 h2 연결 ↔ 백엔드 h2 연결 1:1 고정" 조건에서만.** 헤더 블록을
  재인코딩 없이 전달하려면 목적지 디코더의 동적 테이블이 출발지 인코더의 테이블과 항상 같아야
  하고, 그 조건은 한 연결의 모든 블록이 순서대로 하나의 백엔드 연결로 갈 때만 성립한다.
  동적 테이블을 활용하자는 방향(헤더가 거의 안 변하니 정상 상태 블록은 인덱스 참조 몇 바이트)이
  맞고, 그렇게 하면 **stream id 재작성조차 필요 없다**(1:1이면 클라이언트 id를 그대로 쓴다).
- **HPACK 우회 자체의 이득은 작다.** 실측(BF-3 Arm 코어, 실제 grpc-go DSB 헤더 블록):
  디코드+재인코드 1,315 ns → 선택적 워크 306 ns, 요청당 **약 1.0 µs 절감**. 현재 DMA
  linkerd2-proxy는 요청당 ~58 µs(124k cycles)이므로 **1.7%**. 이득은 HPACK이 아니라
  **종단 엔진 전체(hyper/tower 스트림별 태스크, HeaderMap, 라우트 스택)를 프레임 릴레이로
  대체**할 때 나온다. nghttp2 종단 왕복이 3.6 µs/req인데 릴레이는 1–1.5 µs/req가 목표.
- 따라서 플랜은 "linkerd 안에서 HPACK만 건너뛰기"가 아니라 **정책 lookup만 하는 h2 프레임
  릴레이 데이터플레인**을 만들고, 같은 하네스로 linkerd / dmesh-router-cpp / relay를 비교하는 것.

## 1. 실측 근거

| 항목 | 값 | 출처 |
|---|---|---|
| 선택적 워크 (grpc-go DSB 블록, 테이블 churn 포함) | **306 ns/블록** | `hpack-h2-bench/c/dsb_bench` (BF-3 Arm, 2026-09-09 재실행) |
| 전체 디코드 (모든 필드 materialize) | 392 ns | 〃 |
| 디코드 + 재인코드 (종단 프록시의 헤더 작업) | **1,315 ns** | 〃 |
| 트레이스 헤더가 non-indexed일 때 워크 / 종단 | 41 / 672 ns | 〃 (`NI` 변형) |
| nghttp2 종단 왕복 (server+client 세션 mem-to-mem, m=1) | **3.56 µs/req** (281k/s) | `hpack-h2-bench/c/nghttp2_bench` |
| DMA linkerd2-proxy, 1 Arm 코어 | 17.2k req/s ≈ 58 µs/req, 124k cycles, 118k instr | `linkerd2-proxy/docs/BENCH_SETUP_B.md` |
| dmesh-router (hyper, tower 없음) | ~31k req/s | CLAUDE.md |
| dmesh-router-cpp (nghttp2 종단) | ~100k req/s | CLAUDE.md |
| DPA EU에서의 워커 | **기록 없음** — `bench_mode 3`은 host 하네스가 붙어야 실행됨 | Phase 0 |

해석: 정상 상태 블록이 306 ns나 걸리는 이유는 grpc-go가 `uber-trace-id`를 매 요청
"literal with incremental indexing"으로 보내 테이블이 계속 churn하기 때문이다(x/net hpack
인코더는 sensitive가 아닌 모든 필드를 인덱싱). 테이블 유지에는 삽입되는 값의 **디코드 길이**가
필요하므로(RFC 7541 entry size = 디코드된 name+value+32) Huffman을 출력 없이 count-walk 한다.
이미 `DPUMesh/device/hpack_walk.h`가 정확히 이 방식이다.

## 2. 기술 검토

### 2.1 왜 1:1인가 — HPACK 상태 조건
헤더 블록을 그대로 전달하려면 다음이 모두 성립해야 한다.
1. 그 클라이언트 연결의 **모든** HEADERS/CONTINUATION 블록이 **순서대로 하나의** 백엔드 연결로 간다.
2. 프록시가 그 백엔드 연결에 **자체 헤더 블록을 끼워 넣지 않는다**(자체 스트림/health 금지).
3. 백엔드가 광고한 `SETTINGS_HEADER_TABLE_SIZE`가 클라이언트 인코더가 쓰는 크기 이상이다.
   프록시는 백엔드 SETTINGS를 클라이언트에 그대로 전달(또는 양쪽 min 광고)한다.
4. 응답 방향(백엔드 인코더 ↔ 클라이언트 디코더)도 동일 — 1:1이면 자동 충족.

**N:1 muxing(여러 클라이언트 연결 → 한 백엔드 연결)이나 per-request LB는 동적 테이블과
양립할 수 없다.** 그 경우의 유일한 방법은 프록시가 양쪽에 테이블 크기 0을 광고해 모든 블록을
self-contained(정적 인덱스 + 리터럴)로 만드는 것인데, 그러면 요청당 헤더 바이트가 수 배로
늘고 **호스트 앱 CPU**(인코더 Huffman, 디코더)가 늘어난다 — DPUMesh가 아끼려는 바로 그 자원.

| | 1:1 고정 + 동적 테이블 미러 (권장) | N:1 mux + 테이블 크기 0 |
|---|---|---|
| 블록 전달 | 그대로 | 그대로 |
| stream id | 재작성 불필요 | 재작성 필요 |
| 워크 비용 | 306 ns (churn) / 41 ns | ~40 ns (리터럴 스캔) |
| 와이어 헤더 크기 | 정상 상태 수십 B | 200–300 B/요청 |
| LB 단위 | 연결 | 요청 |
| 호스트 앱 부담 | 변화 없음 | 증가 |

### 2.2 릴레이가 해야 하는 일 (1:1이면 "L7 검사가 붙은 h2-aware 바이트 릴레이")
- 프레임 헤더(9B) 파싱, 타입별 분기. HEADERS/CONTINUATION은 블록 조각 순서만 보장하면 된다.
- HEADERS 블록에 `hw_walk` 실행: 미러 테이블 갱신 + 정책 헤더만 materialize
  (`:method`, `:path`, `:authority`, `content-type`, 설정된 임의 헤더 N개). 트레일러
  HEADERS(END_STREAM)에서는 `grpc-status`만 뽑아 메트릭.
- 정책: **lookup만**. allow/deny, 라우팅은 연결 수립 시(백엔드 선택), 스트림 단위는 판정만.
  mesh identity는 DMA `flow_id`의 workload identity에서 오므로 헤더 디코드가 필요 없다.
- SETTINGS/PING/GOAWAY/WINDOW_UPDATE: 1:1이므로 **그대로 전달 가능**(end-to-end 흐름제어).
  프록시 자체 버퍼의 backpressure는 이미 구현된 DMA 3방향 흐름제어가 담당.
  `MAX_FRAME_SIZE`만 양쪽 min으로 광고.
- 한쪽 연결 종료 → 다른 쪽 종료(1:1).

### 2.3 제약과 부작용 (설계에 반영)
- **deny된 요청의 HEADERS도 백엔드에 전달해야 한다.** 블록이 테이블 삽입을 포함할 수 있어서
  버리면 백엔드 디코더가 어긋난다. 처리: HEADERS 전달 → DATA 보류 → 양방향 `RST_STREAM`.
  gRPC 요청은 HEADERS만으로 완결되지 않으므로 핸들러가 돌지 않지만, END_STREAM이 붙은
  헤더-only 요청(GET)은 백엔드에 닿는다. 엄격히 막으려면 "미러 테이블로 같은 삽입만 수행하는
  블록을 합성"하는 재인코딩 폴백이 필요 — 초기에는 제한으로 명시.
- **프리페이스 경쟁**: 클라이언트는 프록시 SETTINGS를 받기 전에 첫 블록을 보낼 수 있다
  (기본 4096 테이블). 1:1이고 백엔드도 기본 4096이면 문제없다. 백엔드가 다른 크기를 광고하면
  그 SETTINGS가 클라이언트에 도달하기 전 블록은 폴백으로 처리.
- **폴백 경로(전체 디코드 + 재인코드)는 반드시 유지**: 알 수 없는 표현, 미러 불일치 위험,
  프리페이스 경쟁 시 그 연결을 종단 모드로 전환. 구현체: `hpack_term.h`(무의존, DPA 이식 가능)
  또는 nghttp2 `nghttp2_hd_inflate/deflate` API.
- linkerd가 헤더를 **추가**하는 기능(`l5d-dst-canonical`, `l5d-orig-proto`, `forwarded`,
  트레이싱 전파)은 범위 밖 → TODO. 다만 미러 테이블이 있으므로 "블록 끝에
  literal-without-indexing 필드 append"는 테이블에 영향이 없어 길이 갱신만으로 가능하다.
  헤더 추가 TODO의 출발점이 된다. 수정/삭제는 재인코딩 필요.
- per-request LB(EWMA), 재시도(body 버퍼 + 재인코딩), 타임아웃 예산은 재설계 필요 → TODO.
- HTTP/1.1 클라이언트, h1↔h2 변환, 압축은 범위 밖(기존 종단 경로 유지).
- 정상 상태 워크가 306 ns인 것은 트레이스 헤더 churn 때문이다. DSB 앱이 트레이싱을 끄거나
  non-indexed로 보내면 41 ns가 된다 — 평가 시 두 경우를 모두 잰다.

## 3. 구현 플랜

### Phase 0 — 결정 실험 (1–2일)
- (a) `dmesh-router-cpp`를 perf로 프로파일해 `nghttp2_hd_*`(HPACK) 비중과 프레임 처리·
  세션 비중을 분리. 릴레이 전환 시 이득 상한을 확정한다. 이 숫자가 없으면 결과를 해석할 수 없다.
- (b) DPA 워커 ns/블록: host 하네스로 `DMESH_DPA_BENCH_MODE=3` 실행. "DPA 인라인 정책"(Phase 4)
  판단 근거.
- (c) grpc-go 실제 헤더 흐름 캡처: 테이블 크기(서버 광고값), 트레이스 헤더 indexing 여부,
  트레일러 형태. `hpack-h2-bench/src/dsb_blocks.rs` 생성기와 대조.

### Phase 1 — 릴레이 엔진 코어 (하드웨어 독립, 유닛 테스트)
위치: **`dmesh-router-cpp`에 `relay` 모드** 추가(권장). 이유: DMA 데이터패스와 nghttp2가 이미
있고(폴백 재인코딩에 사용), 100k 기준선과 같은 바이너리 안에서 A/B가 되며, C 워커
`hpack_walk.h`를 그대로 include한다. 대안은 Rust `dmesh-relay` 크레이트(vendored `h2::hpack`을
폴백으로) — 프록시 통합엔 가깝지만 Phase 3 비교엔 C++가 유리.

모듈:
1. **frame**: 9B 헤더 파싱, 타입 분기, 스트림별 "블록 진행 중" 상태(CONTINUATION).
2. **pair**: 연결쌍 상태 — 요청 방향/응답 방향 미러 테이블 2개(`struct hw_state`), 양쪽 SETTINGS,
   스트림 테이블(정책 판정 결과, deny 시 DATA 드롭 플래그).
3. **walk 확장**: `hw_walk`에 정책 헤더 목록(정적 인덱스 + 리터럴 이름 매칭), 트레일러 모드
   (`grpc-status`), 응답 방향(`:status`).
4. **policy hook**: `on_connect(flow_id) → backend`, `on_headers(fields) → allow|deny`,
   `on_trailers(fields)` 메트릭. lookup만; 헤더 변경 없음.
5. **fallback**: 조건 감지 시 그 연결을 nghttp2 세션 종단으로 전환(기존 라우터 경로).
6. **metrics**: `request_total`, status/grpc-status 카운터 — L7 검증 게이트에 필요한 최소.

테스트: dsb 블록 + 프레임 재생, 스트림 인터리빙 fuzz, 양쪽 nghttp2 세션을 mem-to-mem으로
붙여 릴레이 통과 전후 디코드 결과 동일성 검증(round-trip equality는 `dsb_bench`에 이미 있음).
디버그 빌드에서는 전체 디코드를 병행해 미러 테이블 이탈을 즉시 검출한다.

### Phase 2 — DMA 통합
ingress 채널(CLIENT 모드 flow)의 rcv ring에서 프레임을 읽어 정책 후 backend push 채널
tx staging으로 복사(1 copy, 블록 내용 무변경). 순서: `flow_id` 도착 → 정책(identity는 flow_id)
→ 백엔드 채널 take → 프리페이스/SETTINGS 교환 → 릴레이. 한쪽 GOAWAY/close → 다른 쪽 전달.
CLIENT 모드 teardown 세그폴트(CLAUDE.md gotcha)는 dmesh-router-cpp가 생존하는 쪽이므로 그대로.

### Phase 3 — 평가 (같은 하네스, 3자 비교)
- `scripts/sb-l7.sh` h2load `-c1 -m300` (h2 종단 검증 게이트 포함), `dm-bench.sh` gRPC 에코,
  DSB hotelReservation(`dsb-dmesh-jet.sh`). 1코어 → 코어 스윕. host CPU도 기록.
- 비교 대상: DMA linkerd2-proxy(17.2k/코어), dmesh-router-cpp(~100k), relay.
- 트레이스 헤더 churn 유/무 두 조건.
- **성공 기준**: 1코어 req/s가 dmesh-router-cpp의 1.5× 이상이면 아이디어 채택. 그 이하이면
  HPACK 우회 이득이 프레임 처리·DMA 드라이버 비용에 묻힌 것이며, 그때는 Phase 4(DPA)만 남는다.
- 기록: `bench-results/`에 환경+결과(bench 에이전트 프로토콜).

### Phase 4 (선택) — DPA 인라인 정책
워커를 DPA 스레드에 넣어 DMA 복사 중 정책 lookup, Arm은 예외(거부/폴백/연결 수립)만 처리.
Phase 0(b)에서 DPA 워커가 Arm의 ~2× 이내이면 검토. `hpack_walk.h`는 이미 무의존/DPA 이식 상태.

### TODO (의도적으로 범위 밖)
헤더 추가/수정/삭제(append-without-indexing부터), deny 시 백엔드 미도달 보장, per-request LB,
재시도/타임아웃 예산, h1 및 프로토콜 변환, 압축.

## 4. 리스크
- 이득의 출처가 "HPACK"이 아니라 "엔진 교체"라는 점 — Phase 0(a) 없이 결과를 해석하면 잘못된
  결론이 나온다.
- 미러 테이블 이탈은 **조용히** 백엔드가 잘못된 헤더를 보는 결함이 된다 → 폴백 + 디버그 병행
  디코드 필수.
- 흐름제어/오류 처리 정확성. 1:1 전달로 대부분 회피하지만 DMA 버퍼 한계와의 상호작용은 실측 필요.

## 참고 위치
- `DPUMesh/device/hpack_walk.h`, `hpack_term.h`, `huff_fsm.h`, `dsb_blocks.h` — 워커/종단/블록
- `DPUMesh/device/dpa_kernel.c:485` — DPA 워커 벤치(bench_mode 3)
- `hpack-h2-bench/` — ARM 벤치(`c/dsb_bench`, `c/nghttp2_bench`), h2/hyper 벤치(`src/`)
- `linkerd2-proxy/linkerd/http/nghttp2/` — nghttp2 기반 h2 엔진 크레이트(`DMESH_NGHTTP2=1`)
- `linkerd2-proxy/docs/BENCH_SETUP_B.md` — 프록시 요청당 비용 프로파일
