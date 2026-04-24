#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/build_grpc_serializer_tests"
RUN_AFTER_BUILD=1
RANDOM_ITERS="${GRPC_SERIALIZER_RANDOM_ITERS:-200}"
RANDOM_SEED="${GRPC_SERIALIZER_RANDOM_SEED:-12345}"

if [[ "${1:-}" == "--build-only" ]]; then
  RUN_AFTER_BUILD=0
fi

mkdir -p "${BUILD_DIR}"

CXXFLAGS=(
  -std=c++17
  -Wall
  -Wextra
  -I"${ROOT_DIR}"
)

SRCS=(
  "${ROOT_DIR}/grpc_wire_encode.c"
  "${ROOT_DIR}/tests/grpc_serializer_golden_test.cc"
)

BIN="${BUILD_DIR}/grpc_serializer_golden_test"
SHARD_SRCS=(
  "${ROOT_DIR}/grpc_shard_routing.cc"
  "${ROOT_DIR}/tests/grpc_sharded_worker_test.cc"
)
SHARD_BIN="${BUILD_DIR}/grpc_sharded_worker_test"

g++ "${CXXFLAGS[@]}" "${SRCS[@]}" -o "${BIN}"
g++ "${CXXFLAGS[@]}" "${SHARD_SRCS[@]}" -o "${SHARD_BIN}"

if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists protobuf; then
  echo "protobuf runtime detected, but protobuf-backed verifier is not wired yet"
else
  echo "protobuf runtime not detected; running golden/reference tests only"
fi

echo "Build done: ${BIN}"
echo "Build done: ${SHARD_BIN}"

if [[ "${RUN_AFTER_BUILD}" -eq 1 ]]; then
  echo "Run random serializer tests: iters=${RANDOM_ITERS} seed=${RANDOM_SEED}"
  GRPC_SERIALIZER_RANDOM_ITERS="${RANDOM_ITERS}" \
  GRPC_SERIALIZER_RANDOM_SEED="${RANDOM_SEED}" \
  "${BIN}"
  "${SHARD_BIN}"
fi
