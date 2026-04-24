#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_SCRIPT="${SCRIPT_DIR}/build_standalone_bench.sh"
BIN="${SCRIPT_DIR}/build_standalone/grpc_offload_bench"

PCI_ADDR=""
BUILD_ONLY=0
RUN_ONLY=0

WARMUP=100
ITERS=1000
NAME_LEN=128
SCORES=512
SEED=1
BATCH=64
SUBMIT_BATCH=64
ENCODED_BUF=""
RING_LOOP=0

print_usage() {
  cat <<'EOF'
Usage: run_dpu_arm_bench.sh [options]

Options:
  --pci <BDF>       PCI BDF (example: 94:00.0). If omitted, auto-detect first DOCA device.
  --warmup <N>      Warmup iterations (default: 200)
  --iters <N>       Measure iterations (default: 20000)
  --name-len <N>    HelloRequest.name length (default: 128)
  --scores <N>      Packed score element count (default: 512)
  --seed <N>        Score seed (default: 1)
  --batch <N>       Max batch size (default: 64)
  --submit-batch <N> Number of requests per offload submit (default: 64)
  --encoded-buf <N>  Override GRPC_DPA_MAX_ENCODED_BUF_SIZE at build time
  --ring-loop        Build with GRPC_DPA_ENABLE_RING_LOOP
  --build-only      Build only, do not run
  --run-only        Run only, skip build
  -h, --help        Show this help
EOF
}

auto_detect_pci() {
  local detected
  detected="$(sudo /opt/mellanox/doca/tools/doca_caps --list-devs 2>/dev/null | awk '/^PCI:/{print $2; exit}')"
  if [[ -z "${detected}" ]]; then
    echo "ERROR: failed to auto-detect PCI. Use --pci <BDF>." >&2
    exit 1
  fi
  echo "${detected#0000:}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pci)
      PCI_ADDR="${2:-}"
      shift 2
      ;;
    --warmup)
      WARMUP="${2:-}"
      shift 2
      ;;
    --iters)
      ITERS="${2:-}"
      shift 2
      ;;
    --name-len)
      NAME_LEN="${2:-}"
      shift 2
      ;;
    --scores)
      SCORES="${2:-}"
      shift 2
      ;;
    --seed)
      SEED="${2:-}"
      shift 2
      ;;
    --batch)
      BATCH="${2:-}"
      shift 2
      ;;
    --submit-batch)
      SUBMIT_BATCH="${2:-}"
      shift 2
      ;;
    --encoded-buf)
      ENCODED_BUF="${2:-}"
      shift 2
      ;;
    --ring-loop)
      RING_LOOP=1
      shift
      ;;
    --build-only)
      BUILD_ONLY=1
      shift
      ;;
    --run-only)
      RUN_ONLY=1
      shift
      ;;
    -h|--help)
      print_usage
      exit 0
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      print_usage
      exit 2
      ;;
  esac
done

if [[ "${BUILD_ONLY}" -eq 1 && "${RUN_ONLY}" -eq 1 ]]; then
  echo "ERROR: --build-only and --run-only cannot be used together." >&2
  exit 2
fi

if [[ "${RUN_ONLY}" -ne 1 ]]; then
  EXTRA_FLAGS=""
  if [[ -n "${ENCODED_BUF}" ]]; then
    EXTRA_FLAGS="${EXTRA_FLAGS} -DGRPC_DPA_MAX_ENCODED_BUF_SIZE=${ENCODED_BUF}"
  fi
  if [[ "${RING_LOOP}" -eq 1 ]]; then
    EXTRA_FLAGS="${EXTRA_FLAGS} -DGRPC_DPA_ENABLE_RING_LOOP"
  fi
  if [[ -n "${EXTRA_FLAGS}" ]]; then
    export EXTRA_CPPFLAGS="${EXTRA_FLAGS}"
  fi
  "${BUILD_SCRIPT}"
fi

if [[ "${BUILD_ONLY}" -eq 1 ]]; then
  echo "Build completed."
  exit 0
fi

if [[ ! -x "${BIN}" ]]; then
  echo "ERROR: benchmark binary not found: ${BIN}" >&2
  echo "Run without --run-only or build first." >&2
  exit 1
fi

if [[ -z "${PCI_ADDR}" ]]; then
  PCI_ADDR="$(auto_detect_pci)"
fi

echo "Running on PCI ${PCI_ADDR}"
exec sudo -E bash -lc "ulimit -l unlimited; \"${BIN}\" --pci \"${PCI_ADDR}\" --warmup \"${WARMUP}\" --iters \"${ITERS}\" --submit-batch \"${SUBMIT_BATCH}\" --name-len \"${NAME_LEN}\" --scores \"${SCORES}\" --seed \"${SEED}\" --batch \"${BATCH}\""
