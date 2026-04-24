# gRPC Persistent Ring Path Sharding Design

## shard model
- persistent ring path는 `request_id`의 stable hash를 기준으로 shard를 선택한다.
- 현재 shard key는 `request_id`이다.
- 이유는 외부 API를 넓히지 않고도 deterministic routing을 만들 수 있고, non-contiguous request_id 환경에서도 그대로 동작하기 때문이다.

## worker lifecycle
- shard마다 독립적인 `GrpcDpaWorkerArg`를 하나씩 둔다.
- shard마다 독립적인 DPA thread를 생성하고 `grpc_dpa_worker_main()`을 실행한다.
- worker는 자기 shard의 request ring / completion ring / msg pool / out pool만 접근한다.
- shutdown 시 host가 shard별 ring control에 `shutdown=1`을 기록하고 thread를 stop/destroy 한다.

## queue / memory ownership
- ring control은 shard별 독립 소유다.
- request ring, completion ring도 shard별 독립 소유다.
- msg pool, out pool도 shard별 독립 소유다.
- 이 구조로 hot path에서 shard 간 lock/atomic 공유를 피한다.
- `max_batch`는 global API limit로 유지하고, 각 shard가 최악의 skewed routing도 처리할 수 있도록 shard별 queue/pool capacity를 `max_batch` 기준으로 할당한다.

## host routing
- host는 batch를 받으면 먼저 `request_id -> shard` dispatch plan을 만든다.
- shard별로 request를 enqueue 한다.
- completion은 shard별로 poll 하되, 최종 matching은 request_id hash-map으로 수행한다.
- 따라서 completion 순서가 submit 순서와 달라도 문제없다.

## error handling
- shard enqueue 중 ring full/H2D/ring sync 실패가 나면 해당 item만 error completion으로 처리한다.
- 다른 shard item은 그대로 진행한다.
- mixed-success batch에서도 성공 item completion은 유지된다.

## known limitations
- 현재 DPA thread의 실제 HW affinity/EU pinning은 명시적으로 보장하지 않는다.
- 즉, logical sharding은 구현했지만 hardware placement는 runtime이 결정한다.
- shard별 pool capacity를 `max_batch`로 잡아서 메모리 사용량은 증가한다.
- RPC batch path는 그대로 단일 path이며, 이번 변경은 persistent ring path sharding에만 집중한다.
