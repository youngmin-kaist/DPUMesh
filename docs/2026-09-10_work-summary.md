# 작업 정리 — 2026-09-09 ~ 09-10 (세션 jet-bf-dmesh)

한 줄 요약: **HTTP/2 선택적 디코딩 아이디어를 얇은 데이터플레인(C++ 라우터)에서 검증하고(종단 대비 1.55×, 무거운 헤더 3.3×, 요청 단위 LB 유지),
linkerd 포크에 통합해 순이득까지 도달(P1 +3.3%, JWT 헤더 +41%)했다. 그 과정에서 찾은 프록시 요청당 비용의 진짜 원인(hyper-util의 요청당 서비스 딥클론)을
없애 1코어 처리량 21.9k → 27.3k req/s(+25%)가 되었다.** 모든 변경은 미커밋.

## 1. 시간순 작업과 결과

| # | 작업 | 결과 | 기록 |
|---|---|---|---|
| 1 | DSB k8s 1k~4k 재측정(단일 프로세스 wrk2) | no-mesh 평평, Linkerd 저부하 tail은 커넥션 6,000개 효과로 동일 재현. `reconnect_socket`은 롤아웃 중 실행이 원인, 정상 상태 재현 불가 | `bench-results/2026-09-04_dsb-k8s-FINAL-3way-*` 참고 |
| 2 | pod CPU 실시간 도구 | `podtop.py`(cgroup 직접, 1 s), metrics-server v0.9.0 설치(10 s 해상도, `--kubelet-insecure-tls`) | jet1 `/home/youngmin/dpumesh/setup/final/` |
| 3 | replica 변경 | frontend 8, geo/profile/rate/reservation 3, recommendation 2; CPU limit 없음(BestEffort) 확인 | — |
| 4 | 선택적 디코딩 타당성 검토 + 플랜 | 1:1 고정 조건, 워커 306 ns vs 종단 1,315 ns, linkerd에서의 상한 ~12% | `docs/2026-09-09_h2-selective-relay-plan.md`, `…-experiment-plan.md` |
| 5 | DMA linkerd 1코어 프로파일 | 106k cycles/req: h2 20%, atomics 18%, alloc 8%, tokio 6%, 레이어 13%, HeaderMap 5%, 전송 3.5% | `2026-09-09_dma-proxy-1core-perf-profile.md` |
| 6 | Phase 0 빌드 플래그 / nghttp2 엔진 | `target-cpu=native`+`codegen-units=1` **+5%**(outline atomics 소멸); nghttp2 서버 엔진 +1.4% | `…buildflags-AB.md`, `…nghttp2-AB.md` |
| 7 | C++ relay v1/v2/v3 | v2(논리적 종단) 종단 99.9k → 154.7k = L4 상한, P2 3.6×; v3(트랜스코더, N:M) P2 89k = L4, dyn_miss 0 | `…relay-prototype.md`, `…relay-v2.md`, `…relay-v3-transcode.md` |
| 8 | h2 crate 포크 1~3단계 (서브에이전트) | 1: 미러/트랜스코더 모듈+테스트. 2: 배선, **−7~9%**. 3: 최적화, **P1 +3.3%, P2 +41%** | `…selective-h2-stage2-AB.md`, `2026-09-10_selective-h2-stage3-AB.md` |
| 9 | work-stealing × 플래그 | C1 +1.7%, C4 +6.9%, C8 판정 불가(런 간 40~85k) | `…work-stealing-buildflags-AB.md` |
| 10 | Envoy 루프백 비교 | Envoy 6.1k vs nghttpx 88k vs linkerd 21k (1코어) | `…dpu-loopback-engine-envoy.md` |
| 11 | 요청당 Arc/Box 클론 제거 (서브에이전트) | **+18%**, 95.7k → 81.0k cycles/req. 원인은 hyper-util `TowerToHyperService`의 요청당 딥클론(기존 memo 무효화). tokio 협력 예산 wedge 발견·수정 | `2026-09-09_arc-clone-removal-AB.md` |
| 12 | 요청당 태스크 spawn 제거 (서브에이전트) | 동률(+0.6%): spawn이 아니라 readiness 추적이 비용. 옵션 유지, 기본 off | `2026-09-10_inline-streams-AB.md` |

## 2. 핵심 숫자 (1 Arm 코어, sharded, h2load -c1 -m300 ×4)

| 단계 | P1 req/s | cycles/req |
|---|---|---|
| 기준 (09-03 빌드) | 20.3k | 103.6k |
| + native 플래그 | 21.3k | 98.6k |
| + Arc/Box 클론 제거 | 25.9k | 81.0k |
| + selective h2 (3단계, ON) | **27.3k** | 78.2k |
| 참고: C++ 종단 / relay v2 / L4 (같은 세션) | 99.9k / 154.7k / 154.6k | 21.1k / 11.5k / 13.7k |

무거운 헤더(JWT 1.5 KB + 12헤더): linkerd OFF 15.6k → ON 22.0k(+41%); C++ 종단 26.9k → transcode 89.1k(3.3×).

## 3. 알게 된 것 (설계에 남길 결론)

- **HPACK 상태 조건**: 원본 블록 전달은 클라이언트 연결 ↔ 백엔드 연결 1:1일 때만 유효. **재인덱싱 트랜스코딩**(연결별 미러 + 우리 소유 인코더 테이블)은 이 제약을 없애고 요청 단위 LB·헤더 변경·순서 변경을 허용한다. never-indexed 보존, 크기 업데이트는 미러 전용, 삽입 즉시 축출(RFC §4.4) 처리 필수.
- **linkerd의 헤더 비용은 작다**: HPACK+블록 8%, HeaderMap 4%. 선택적 경로가 이기려면 구현 비용 ≤1~2%, NeededSet 최소화가 결정적. h2 나머지(프레임·스트림 기계 16~19%)는 헤더 처리와 무관.
- **요청당 비용의 진짜 원인**은 hyper-util 어댑터의 서비스 딥클론이었고, `CallInPlace`로 제거. 이때 **tokio 협력 예산(태스크당 128)**이 in-place readiness에서 spurious Pending → LoadShed 503을 유발(`unconstrained`로 해결, 회귀 테스트 있음).
- work-stealing 런타임의 손실은 코히어런스·지역성이라 빌드 플래그로는 안 줄어든다. spawn 제거는 이득 없음.
- 데이터패스 드라이버 틱 비용(틱당 syscall·watermark memcpy·clock_gettime)이 얇은 엔진에서는 지배적: 처리가 빨라질수록 요청당 틱 비용이 늘어(11k → 24k cycles) 저부하 CPU 절감을 상쇄. 배칭이 다음 병목.
- 함정 기록: 이름을 바꾼 바이너리는 comm 15자 절단으로 `pkill -x`를 피해 DOCA 장치를 붙잡는다(세 번 발생). 같은 이름을 다른 디렉터리에 둘 것. C++ 라우터에 `dmesh_doca_conn_rx_watermark` 호출이 빠져 있던 버그(9/2 흐름제어 추가 이후) 수정.

## 4. 환경 변경 (되돌릴 때 참고)

- host jet1 nginx `/etc/nginx/sites-enabled/dmesh-bench`: `http2_max_concurrent_streams 1024`, `location = /big`(1 MiB, `/var/www/dmesh-bench/big`).
- k8s: metrics-server 설치(`setup/final/metrics-server.yaml`), replica 변경(§1-3). `verify.sh`/`restore-fixed-config.sh`는 아직 frontend 6 기준.
- DPU: apt `nginx nghttp2-client nghttp2-proxy` 설치, `nghttpx` 서비스 disable, nginx 서비스 disable. Envoy 바이너리·설정은 `dmesh-router-cpp/scripts/loopback/`.
- linkerd2-proxy: `Cargo.toml` `codegen-units=1`, `dev-proxy-env.sh` 기본 RUSTFLAGS에 `-C target-cpu=native`, 워크스페이스 members에 `vendor/h2`. 보존 바이너리: `target/release/{baseline,pre-arc,post-arc,pre-sel3}/linkerd2-proxy`.

## 5. 미커밋 변경 (커밋 단위 제안)

1. `DPUMesh/device/hpack_walk.h` 가드 + `dmesh-router-cpp` relay v1/v2/v3(`src/relay*.{hpp,cpp}`, `router.*`, `main.cpp`, `meson.build`, `test/`, `scripts/`).
2. linkerd2-proxy: 빌드 플래그(`Cargo.toml`, `scripts/dev-proxy-env.sh`).
3. linkerd2-proxy: Arc/Box 클론 제거(`proxy/http/server.rs` CallInPlace, `stack/map_err.rs`, `outbound policy route/router`, `proxy/tap`, `router` 테스트).
4. linkerd2-proxy: 인라인 스트림 옵션(vendored hyper 서버, 기본 off).
5. linkerd2-proxy: selective h2(vendored h2 `hpack/{mirror,transcode,selective}`, frame/codec/proto 배선, hyper 빌더, `proxy/http/selective_h2.rs`, 게이트).
6. 이전부터 미커밋: DPUMesh 재연결 수정, `dmeshgo`, DSB `registry.go` dedupe, 하네스 스크립트.

## 6. 다음 후보

- selective 4단계: `Extensions` 대신 hyper 사이드 채널(0.6%), retry 경로의 `RawHeaderReps` 보존, 통계 노출. P2 헤더 목록을 고정해 2·3단계 P2를 같은 조건으로 재측정.
- 스택 다이어트 계속: prometheus 라벨 조회(`end_stream`, SipHash), 바디/BoxFuture 박싱, Buffer/ConcurrencyLimit 원자 연산.
- 데이터패스 드라이버 틱 배칭(C++·Rust 공통) — 얇은 엔진의 남은 병목.
- 8코어 work-stealing 판정은 5×5 교차 반복으로만 가능; sharded가 기본이므로 우선순위 낮음.
