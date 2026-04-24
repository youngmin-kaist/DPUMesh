#!/usr/bin/env bash
set -euo pipefail

GRPC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${GRPC_DIR}/build_standalone"
DEVICE_BUILD_DIR="${BUILD_DIR}/device"

DOCA_DIR="${DOCA_DIR:-/opt/mellanox/doca}"
DOCA_INCLUDE="${DOCA_DIR}/include"
DOCA_TOOLS="${DOCA_DIR}/tools"
DOCA_DPACC="${DOCA_TOOLS}/dpacc"
DPACC_MCPU_FLAG="${DPACC_MCPU_FLAG:-nv-dpa-bf3}"
DPA_APP_NAME="${DPA_APP_NAME:-DPU_mesh_dpa_app}"

mkdir -p "${DEVICE_BUILD_DIR}"

DOCA_LIB_DIR="$(pkg-config --variable=libdir doca-dpa)"

HOST_CC_FLAGS="-Wno-deprecated-declarations -Wall -Wextra -DFLEXIO_ALLOW_EXPERIMENTAL_API"
DEVICE_CC_FLAGS="-Wno-deprecated-declarations -Wno-error -Wall -Wextra -DFLEXIO_DEV_ALLOW_EXPERIMENTAL_API -O2"
EXTRA_CPPFLAGS="${EXTRA_CPPFLAGS:-}"

DEVICE_LIB="${DEVICE_BUILD_DIR}/grpc_dpa_kernel.a"
DEVICE_SRCS=(
  "${GRPC_DIR}/grpc_dpa_kernel.c"
  "${GRPC_DIR}/grpc_wire_encode.c"
)

if [[ ! -x "${DOCA_DPACC}" ]]; then
  echo "ERROR: dpacc not found at ${DOCA_DPACC}" >&2
  exit 1
fi

echo "[1/4] Build DPA device program: ${DEVICE_SRCS[*]}"
"${DOCA_DPACC}" "${DEVICE_SRCS[@]}" \
  -o "${DEVICE_LIB}" \
  -mcpu="${DPACC_MCPU_FLAG}" \
  -hostcc=gcc \
  -hostcc-options="${HOST_CC_FLAGS}" \
  --devicecc-options="${DEVICE_CC_FLAGS}" \
  --app-name="${DPA_APP_NAME}" \
  -device-libs="-L${DOCA_LIB_DIR} -ldoca_dpa_dev -ldoca_dpa_dev_comm" \
  -flto \
  -I"${DOCA_INCLUDE}" \
  -I"${GRPC_DIR}"

echo "[2/4] Compile host C object"
gcc -std=gnu11 -c "${GRPC_DIR}/dpa_batch_worker.c" \
  -I"${DOCA_INCLUDE}" -I"${GRPC_DIR}" ${EXTRA_CPPFLAGS} \
  -o "${BUILD_DIR}/dpa_batch_worker.o"
gcc -std=gnu11 -c "${GRPC_DIR}/grpc_ring_loop.c" \
  -I"${DOCA_INCLUDE}" -I"${GRPC_DIR}" ${EXTRA_CPPFLAGS} \
  -o "${BUILD_DIR}/grpc_ring_loop.o"

echo "[3/4] Compile host C++ objects"
g++ -std=c++17 -c "${GRPC_DIR}/host_agent.cc" \
  -I"${DOCA_INCLUDE}" -I"${GRPC_DIR}" ${EXTRA_CPPFLAGS} \
  -o "${BUILD_DIR}/host_agent.o"
g++ -std=c++17 -c "${GRPC_DIR}/grpc_shard_routing.cc" \
  -I"${DOCA_INCLUDE}" -I"${GRPC_DIR}" ${EXTRA_CPPFLAGS} \
  -o "${BUILD_DIR}/grpc_shard_routing.o"
g++ -std=c++17 -c "${GRPC_DIR}/offload_benchmark.cc" \
  -I"${DOCA_INCLUDE}" -I"${GRPC_DIR}" ${EXTRA_CPPFLAGS} \
  -o "${BUILD_DIR}/offload_benchmark.o"
g++ -std=c++17 -c "${GRPC_DIR}/grpc_selective_policy_runtime.cc" \
  -I"${DOCA_INCLUDE}" -I"${GRPC_DIR}" ${EXTRA_CPPFLAGS} \
  -o "${BUILD_DIR}/grpc_selective_policy_runtime.o"
g++ -std=c++17 -c "${GRPC_DIR}/grpc_bench_main.cc" \
  -I"${DOCA_INCLUDE}" -I"${GRPC_DIR}" ${EXTRA_CPPFLAGS} \
  -o "${BUILD_DIR}/grpc_bench_main.o"

echo "[4/4] Link standalone benchmark"
g++ -std=c++17 \
  "${BUILD_DIR}/grpc_bench_main.o" \
  "${BUILD_DIR}/offload_benchmark.o" \
  "${BUILD_DIR}/host_agent.o" \
  "${BUILD_DIR}/grpc_shard_routing.o" \
  "${BUILD_DIR}/grpc_selective_policy_runtime.o" \
  "${BUILD_DIR}/dpa_batch_worker.o" \
  "${BUILD_DIR}/grpc_ring_loop.o" \
  "${DEVICE_LIB}" \
  -pthread \
  $(pkg-config --cflags --libs doca-common doca-dpa libflexio) \
  -o "${BUILD_DIR}/grpc_offload_bench"

echo "Build done: ${BUILD_DIR}/grpc_offload_bench"
