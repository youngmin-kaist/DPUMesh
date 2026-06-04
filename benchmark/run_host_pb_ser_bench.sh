#!/usr/bin/env bash
set -euo pipefail

SRC_DIR="$(pwd)"
BUILD_DIR="${SRC_DIR}/build"

CPU_CORE="${CPU_CORE:-35}"

mkdir -p "${BUILD_DIR}"

echo "[1/3] Generate protobuf C++ stub code"

protoc \
  -I="${SRC_DIR}" \
  --cpp_out="${BUILD_DIR}" \
  "${SRC_DIR}/hello.proto"

echo "[2/3] Build benchmark"

g++ -O3 -DNDEBUG -march=native -std=c++17 \
  -Wall -Wextra \
  -I"${BUILD_DIR}" \
  "${SRC_DIR}/host_pb_ser_bench.cc" \
  "${BUILD_DIR}/hello.pb.cc" \
  -lprotobuf \
  -pthread \
  -o "${BUILD_DIR}/host_pb_ser_bench"

echo "[3/3] Run benchmark with taskset"
echo "CPU_CORE=${CPU_CORE}"

taskset -c "${CPU_CORE}" \
  "${BUILD_DIR}/host_pb_ser_bench" \
  --duration_s=10 \
  --warmup_s=2 \
  --report_interval_s=1 \
  --name_len=1000 \
  --scores=4 \
  --pool=1 \
  --check_interval=1024