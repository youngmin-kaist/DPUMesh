# DMA linkerd2-proxy 요청당 비용 절감 — 구현 플랜 (2026-09-09)

근거: `bench-results/2026-09-09_dma-proxy-1core-perf-profile.md` (1 Arm 코어, 19.8k req/s, **106k cycles/req**, 코어 100% 사용).
전송(DMA/DOCA)은 3.5%뿐이고 나머지는 L7 소프트웨어 스택이다. 목표는 같은 하네스에서 **cycles/req를 절반(≈50k)** 으로 줄여
1코어 ~40k req/s. 바닥 참조치는 dmesh-router-cpp(≈21k cycles/req, 100k req/s).

## 측정 프로토콜 (모든 단계 공통)
- 하네스: `DMESH_SHARDED=1 sb-l7-prof.sh 1 1 4 load` (W=1, 코어 15 핀, M=4, h2load `-c1 -m300 /ok`).
  창 8초에 `perf stat -p`(cycles·instr·IPC) + `request_total{outbound}` Δ → **cycles/req**가 유일한 채점표.
- L7 게이트: `request_total` 증가 = h2load 요청수 (opaque 폴백 방지).
- 각 단계 전후를 `bench-results/`에 기록(환경 블록 포함). 한 번에 한 변수만.
- 마지막에 DSB hotelReservation e2e(`dsb-dmesh-jet.sh`)로 회귀 확인.

## Phase 0 — 반나절: 공짜 이득 + 기존 게이트 A/B
| # | 작업 | 대상 | 기대 |
|---|---|---|---|
| 0a | 빌드 플래그: `-C target-cpu=native`(=`+lse`) 로 outline atomics 인라인, `codegen-units=1`, `panic=abort` | `Cargo.toml [profile.release]`, `scripts/dev-proxy-env.sh` RUSTFLAGS | atomics 17.7% 중 **호출 오버헤드** 부분 제거, 3–6% |
| 0b | `DMESH_NGHTTP2=1` A/B — 기록이 없다. 같은 프로파일로 서버측 스트림별 spawn 비용을 수치화 | `linkerd/proxy/http/src/server.rs:136` | hyper server dispatch 16.6% incl. 중 일부 |
| 0c | 프로파일 재수집(0a 적용 후)을 기준선으로 고정 | | |

## Phase 1 — 1주: 요청당 런타임 오버헤드 (spawn·oneshot·Arc·Buffer)
프로파일 근거: tokio 6.3% self / 72% incl., atomics 호출자 상위가 task complete·oneshot·BytesMut drop·tower Buffer·Concrete drop.
1. **nghttp2 클라이언트 배선** — 크레이트에 `client.rs`가 있으나 배선은 서버(`server.rs:233`)뿐. outbound → backend 쪽
   (h2 client incl. **20.8%**, `Http2ClientConnExec::execute` spawn + oneshot/req)을 `linkerd/proxy/http/src/client.rs`에
   같은 게이트로 연결. 검증: `tests/conformance.rs`, `benches/pair.rs`.
2. **서버측 스트림별 태스크 제거** — 0b 결과가 좋으면 nghttp2 서버를 기본으로; hyper를 유지한다면 벤더 hyper
   `proto/h2/server.rs:318 exec.execute_h2stream`을 spawn 대신 연결 태스크 내 `FuturesUnordered` 폴링으로 교체(벤더 패치).
3. **요청당 Arc/Box 클론 감소** — `drop_in_place<outbound::http::logical::Concrete>`·`Route` 드롭, `BoxCloneSyncService` clone+alloc.
   `MemoOneshotRoute`(policy/router.rs:125)가 라우트 스택 클론은 이미 제거했으니, 남은 backend/Concrete 서비스도 라우트별 memo로
   `&self` 호출이 되게. 목표: 요청 경로에서 `Arc::clone` 0.
4. **tower::buffer 우회** — sharded 런타임은 단일 스레드인데 Buffer(세마포어 atomics + 채널)를 거친다. DMA 경로에서 Buffer 레이어를
   직접 호출로 대체(`linkerd_stack::result::ResultService` 아래).
기대: 15–25k cycles/req.

## Phase 2 — 1–2주: 레이어 다이어트 (11× 분해 방식으로 하나씩 켜고 끄기)
프로파일 근거: linkerd_stack+futures_util+tower+router+timeouts+box ≈13% self; 포함 시간 tracing 17%, prom/metrics 29.5%,
TapHttp 6.2%(탭 없음에도), `linkerd_http_detect::Detect` 5%(DMA 플로우는 프로토콜을 이미 안다).
- `DMESH_LEAN=<bitmask>` 환경변수로 레이어별 바이패스를 두고 각각 cycles/req를 잰다: detect / tap / classify+prom record_response /
  stream_timeouts / normalize_uri / box_future·map_err·ErrInto 래퍼 / tracing span(요청당 `Instrumented` 생성·드롭).
- 살릴 것은 살리되 형태를 바꾼다: 메트릭은 워커 로컬 카운터(atomics 제거), 박싱된 future는 구체 타입으로.
- 파일: `linkerd/app/outbound/src/http/{server.rs(10), endpoint.rs(17), logical.rs(7), logical/policy/route.rs(10), concrete.rs(5)}`의 `.push` 체인.
기대: 15–20k cycles/req.

## Phase 3 — 2주+: 헤더 경로 (선택적 디코딩 릴레이 플랜과 합류)
HPACK 5.7% + HeaderMap 왕복 ~7k cycles = 상한 ~12%. Phase 1–2가 끝나면 비중이 커진다.
`docs/2026-09-09_h2-selective-relay-plan.md` Phase 1(릴레이 엔진)을 nghttp2 엔진 크레이트 안에서 진행: 연결 1:1 고정 + 미러 테이블,
정책 필드만 `hw_walk`. 헤더 추가는 TODO.
기대: ~10k cycles/req.

## Phase 4 — 복사 제거
libc memcpy 64B 루프 2.9% + bytes 3.6% incl.: DMA staging → `BytesMut` → h2 프레임 → 백엔드 staging의 복사 사슬.
`DmeshIo` read가 DMA 링을 참조하는 `Bytes`(refcount 해제 시 링 소비 확정)를 내주고, 쓰기는 프레임을 staging에 직접 조립.
기대: ~5k cycles/req.

## 순서 이유와 리스크
- 0 → 1 → 2 → 3 순서인 이유: 큰 항목(런타임·스택)이 줄어야 헤더 경로 비중이 의미 있어지고, 릴레이 엔진은 nghttp2 크레이트 위에서
  만드는 편이 hyper 위보다 쉽다(연결당 단일 태스크 구조가 이미 있다).
- 벤더 hyper/h2 패치는 업스트림 추종 비용이 있다 → nghttp2 경로가 확정되면 hyper 패치는 버린다.
- tap/classify/metrics는 정책·관측 기능이다. 바이패스는 측정용 플래그로 두고, 채택 시 "값을 지키되 싸게"로 재구현한다.
- 모든 단계는 sharded W=1에서 재고, 최종만 코어 스윕(`sb-l7.sh 16 8 16`)으로 스케일 확인.

## 완료 기준
같은 하네스에서 cycles/req ≤ 55k(1코어 ≥ 38k req/s), L7 게이트 통과, DSB e2e 처리량 회귀 없음, 기록 파일 5개(단계별) 이상.
