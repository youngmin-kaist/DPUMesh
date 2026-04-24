#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/build_codegen_demo"
mkdir -p "${BUILD_DIR}"

CXXFLAGS=(
  -std=c++17
  -Wall
  -Wextra
  -Wno-unused-parameter
  -I"${ROOT_DIR}"
)

SRCS=(
  "${ROOT_DIR}/framework/schema/grpc_schema_registry.cc"
  "${ROOT_DIR}/framework/policy/grpc_selective_policy.cc"
  "${ROOT_DIR}/framework/dpa/grpc_dpa_emit_generic.c"
  "${ROOT_DIR}/framework/runtime/grpc_transport_stage.cc"
  "${ROOT_DIR}/codegen/generated/demo_request_schema.cc"
  "${ROOT_DIR}/codegen/generated/demo_request_lowering.cc"
  "${ROOT_DIR}/codegen/generated/demo_request_dpa_stub.c"
  "${ROOT_DIR}/tests/grpc_codegen_verify.cc"
  "${ROOT_DIR}/tests/grpc_codegen_smoke.cc"
)

BIN="${BUILD_DIR}/grpc_codegen_smoke"

g++ "${CXXFLAGS[@]}" "${SRCS[@]}" -o "${BIN}"

echo "Build done: ${BIN}"
