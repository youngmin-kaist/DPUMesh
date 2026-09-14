# HTTP/2 Selective Decoding — 미팅 정리 (2026-09-10)

## 1. 아이디어
서비스 메시 프록시는 h2를 종단하며 요청마다 헤더 블록을 **HPACK 디코드 → HeaderMap 구성 → 정책 → HPACK 재인코드** 한다.
정책이 헤더 lookup만 필요하다면, **정책에 필요한 필드만 선택적으로 디코딩하고 헤더 블록 바이트는 재인코딩 없이 목적지로 넘기자.**
논리적으로는 양쪽 h2 연결을 종단(자체 SETTINGS·흐름제어·스트림 상태)하되, 헤더 블록의 디코드→재인코드만 건너뛴다.

## 2. 핵심 기술 포인트
- **HPACK 상태 조건**: 블록을 그대로 전달하려면 목적지 디코더의 동적 테이블이 출발지 인코더의 테이블과 같아야 한다 →
  클라이언트 연결 ↔ 백엔드 연결 **1:1 고정**, 블록 순서 보존, 프록시가 그 leg에 자체 블록을 끼워 넣지 않음(v2 방식). 요청 단위 LB·헤더 변경 불가가 제약.
- **재인덱싱 트랜스코딩**(v3, 채택): 연결마다 두 테이블을 둔다. rx **미러**(상대 인코더 복제; 값은 인코딩된 바이트로 저장, 정책 필드만 디코드)와
  tx **인코더**(우리가 상대 디코더를 향해 소유). 블록을 표현(representation) 단위로 번역 — 정적 인덱스 통과, 동적 인덱스는 미러 엔트리를 대상
  인코더 테이블에서 재해석(hit → 새 인덱스, miss → 원본 값 바이트 리터럴 + 삽입), never-indexed 보존, 크기 업데이트는 미러 전용.
  미러(파싱 시점)와 인코더(송신 시점)가 분리되어 **N:M 멀티플렉싱·요청 단위 LB·헤더 추가/삭제·순서 변경·deny**가 모두 허용된다.
- **정책 필드 추출**: 연결 스코프 미러를 유지하는 워커(`hpack_walk.h`)가 `:method/:path/:authority`(+설정된 이름)만 materialize.
  블록당 41 ns(non-indexed) ~306 ns(trace-id churn) vs 전체 디코드+재인코드 1,315 ns(BF-3 Arm).
- tower 통합 형태: 서버 엔진이 **희소 HeaderMap + 원본 표현 리스트(extension)** 로 `Request`를 만들고, 스택은 그대로 동작, 클라이언트 엔진이 송신 시
  선택된 백엔드 연결의 인코더 테이블로 트랜스코딩. 스택이 추가한 헤더는 literal append.

## 3. 만든 것
| 단계 | 내용 | 검증 |
|---|---|---|
| C++ relay v1 | 바이트 릴레이 + 헤더 검사(1:1) | walk_fail 0 / 2.3M 블록 |
| C++ relay v2 | 논리적 종단(자체 SETTINGS·PING·GOAWAY·RST 번역, leg별 흐름제어, deny=RST+403) | curl allow/deny/1 MiB, h2load 3M 스트림 |
| C++ relay v3 | 재인덱싱 트랜스코더 + N:M mux, 요청마다 백엔드 선택 | nghttp2 왕복 300블록(churn·크기 0·never-indexed), dyn_miss 0 |
| h2 crate 1단계 | `hpack/{mirror,transcode}` 모듈 + 10 테스트 | round-trip, N:M, 2 백엔드, JWT, 크기 0, malformed |
| h2 crate 2단계 | frame/codec/proto 배선, hyper 빌더, linkerd 게이트 `DMESH_SELECTIVE_H2` | in-memory e2e(2 클라이언트→프록시→2 백엔드) |
| h2 crate 3단계 | 아레나 테이블·`Copy` 엔트리/Rep·슬롯 캐시·풀 버퍼(블록당 할당 0)·최소 NeededSet·단일 패스 load | 카운팅 할당기 테스트, ON/OFF 헤더 바이트 동일 |

## 4. 결과 (1 Arm 코어)
| 환경 | 종단 / OFF | 선택적 / ON | 이득 |
|---|---|---|---|
| C++ 라우터, h2load 최소 헤더 | 99.9k | 154.7k (= L4 상한 154.6k) | **+55%** |
| C++ 라우터, JWT 1.5 KB + 12헤더 (v3, 요청 단위 LB) | 26.9k | 89.1k (= L4 88.4k) | **3.3×** |
| linkerd 포크, h2load 최소 헤더 (P1) | 26,396 | 27,278 | **+3.3%** |
| linkerd 포크, JWT 프로파일 (P2) | 15,648 | 22,032 | **+41%** |
| linkerd, DSB hotelReservation (4코어, 같은 부하) | 2.89 / 3.36 코어 | 2.75 / 3.28 코어 | CPU −2~5%, p99 147→110 ms |
| linkerd, gRPC-go 64 B echo | 16.9k / 19.0k | 16.5k / 19.0k | −2% / ±0 |
- linkerd 안에서 이득이 작은 이유: 헤더 처리(HPACK 8% + HeaderMap 4%)가 요청당 비용의 12%뿐. 무거운 헤더에서 커지고(P2 +41%), 헤더가 최소인 gRPC 핑퐁이 하한.
- 2단계 구현은 −7~9%였다가 3단계 최적화(할당 0, 슬롯 캐시, NeededSet 최소화)로 순이득. 구현 비용이 절감분보다 작아야 한다는 교훈.
- 같은 기간 프로파일 기반으로 찾은 다른 병목(hyper-util의 요청당 서비스 딥클론)을 제거해 +18%; 누적 1코어 21.9k → 27.3k.

## 5. 한계와 미구현
- 트레일러는 전체 materialize + 리터럴 재인코딩(gRPC `grpc-status`마다) → gRPC 핑퐁에서 −2%. 다음 작업.
- retry 경로가 표현 리스트 extension을 떨어뜨림(재시도 시 needed 밖 헤더 유실). 헤더 삭제·수정은 materialized 이름에 한해 지원.
- 동적 *이름* 참조를 리터럴로 재방출(필드당 +10 B 정도, 압축률 손실). 다른 클라이언트가 넣은 같은 값 재사용 없음.
- deny 시 인코더 테이블 삽입을 되돌릴 수 없어 블록 전송 + RST(헤더-only 요청은 백엔드에 닿음).
- 얇은 데이터플레인 결과의 남은 병목은 DMA 드라이버 틱 비용(저부하에서 요청당 11k → 24k cycles) — 배칭 필요.

## 6. 다음
1. 트레일러 reps 경로(gRPC 필수) → gRPC 핑퐁 재측정.
2. 실제 메시 헤더 프로파일(트레이스 churn + 인증 토큰) 고정 파일로 재측정, 2·3단계 P2 통일.
3. hyper 사이드 채널로 extension 삽입 비용 제거(0.6%), retry 경로 보존, 통계 노출.
4. 스택 다이어트 병행(프레임당 h2 기계, 바디 fast path, memcpy, 메트릭 라벨).

기록: `bench-results/2026-09-09_h2-selective-relay-{prototype,v2,v3-transcode}.md`, `…linkerd-selective-h2-stage2-AB.md`, `2026-09-10_selective-h2-stage3-AB.md`,
`2026-09-10_dsb-dma-selective-h2-AB.md`, `2026-09-10_grpc-echo-1core-selective-h2-AB.md`. 플랜: `docs/2026-09-09_h2-selective-relay-*.md`.
