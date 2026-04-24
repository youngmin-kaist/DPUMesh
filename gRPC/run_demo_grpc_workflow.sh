#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
APP_DIR="${REPO_ROOT}/DPUMesh"
BUILD_DIR="${APP_DIR}/build-demo"
BIN="${BUILD_DIR}/dpumesh"

ROLE=""
PCI=""
REP_PCI=""
PROFILE="reference3"
BATCH=3
SCORES=4
SEED=10
GRPC_ID=1
NAME="demo"
REPEAT=1
TIMEOUT_SEC=30
NO_BUILD=0
LEGACY_BOUNCE=0
LOG_DIR="/tmp/dmesh-grpc-workflow-$(date +%Y%m%d-%H%M%S)"

usage() {
    cat <<USAGE
Usage:
  $(basename "$0") --role dpu    --pci <DPU_PF_PCI> --rep-pci <REP_PCI> [--no-build]
  $(basename "$0") --role host   --pci <HOST_PCI> [--profile <name>] [--batch N] [--scores N] [--seed N] [--grpc-id N] [--name STR] [--legacy-bounceback] [--repeat N] [--timeout SEC] [--no-build]
  $(basename "$0") --role matrix --pci <HOST_PCI> [--repeat N] [--timeout SEC] [--no-build]

Roles:
  dpu     Build and run the DPU-side demo server.
  host    Build and run one Host-side workload case.
  matrix  Build and run multiple Host-side workload cases sequentially. Assumes the DPU side is already running.

Supported workload profiles:
  generated
  reference3
  workflow_smoke
  workflow_batch8
  workflow_batch16

Examples:
  $(basename "$0") --role dpu --pci 03:00.0 --rep-pci 03:00.1
  $(basename "$0") --role host --pci 03:00.0 --profile reference3 --batch 3 --scores 4 --seed 10 --grpc-id 7 --name hello
  $(basename "$0") --role host --pci 03:00.0 --profile reference3 --batch 3 --legacy-bounceback
  $(basename "$0") --role matrix --pci 03:00.0 --repeat 3
USAGE
}

ensure_build() {
    if [[ "${NO_BUILD}" -eq 1 ]]; then
        return
    fi

    mkdir -p "${APP_DIR}"
    echo "[build] configuring ${BUILD_DIR}"
    if [[ -d "${BUILD_DIR}" ]]; then
        meson setup "${BUILD_DIR}" --reconfigure -Ddemo_grpc_offload=true >/dev/null
    else
        meson setup "${BUILD_DIR}" -Ddemo_grpc_offload=true >/dev/null
    fi
    echo "[build] compiling ${BUILD_DIR}"
    CCACHE_DISABLE=1 meson compile -C "${BUILD_DIR}"
}

ensure_binary() {
    if [[ ! -x "${BIN}" ]]; then
        echo "Binary not found: ${BIN}" >&2
        echo "Run without --no-build or build ${BUILD_DIR} first." >&2
        exit 1
    fi
}

run_host_case() {
    local case_name="$1"
    local profile="$2"
    local batch="$3"
    local scores="$4"
    local seed="$5"
    local grpc_id="$6"
    local name="$7"
    local iter
    local log_file
    local rc
    local cmd
    local run_cmd

    mkdir -p "${LOG_DIR}"

    for ((iter = 1; iter <= REPEAT; ++iter)); do
        log_file="${LOG_DIR}/${case_name}.iter${iter}.log"
        cmd=("${BIN}"
             -p "${PCI}"
             --grpc-workload-profile "${profile}"
             --grpc-batch "${batch}"
             --grpc-scores "${scores}"
             --grpc-score-seed "${seed}"
             --grpc-id "${grpc_id}"
             --grpc-name "${name}"
             --grpc-legacy-bounceback "${LEGACY_BOUNCE}")
        if command -v stdbuf >/dev/null 2>&1; then
            run_cmd=(stdbuf -oL -eL "${cmd[@]}")
        else
            run_cmd=("${cmd[@]}")
        fi

        echo "[host] case=${case_name} iter=${iter}/${REPEAT} profile=${profile} batch=${batch} scores=${scores} seed=${seed} grpc_id=${grpc_id} name=${name}"
        set +e
        if command -v timeout >/dev/null 2>&1 && [[ "${TIMEOUT_SEC}" -gt 0 ]]; then
            timeout --preserve-status "${TIMEOUT_SEC}s" "${run_cmd[@]}" 2>&1 | tee "${log_file}"
            rc=${PIPESTATUS[0]}
        else
            "${run_cmd[@]}" 2>&1 | tee "${log_file}"
            rc=${PIPESTATUS[0]}
        fi
        set -e

        if [[ "${rc}" -ne 0 ]]; then
            echo "[host] FAIL case=${case_name} iter=${iter} rc=${rc} log=${log_file}" >&2
            return 1
        fi

        if ! grep -q "DEMO_GRPC_OFFLOAD host completed" "${log_file}"; then
            echo "[host] FAIL case=${case_name} iter=${iter} missing completion log: ${log_file}" >&2
            return 1
        fi

        echo "[host] PASS case=${case_name} iter=${iter} log=${log_file}"
    done

    return 0
}

run_matrix() {
    local failures=0

    run_host_case "reference3" reference3 3 4 10 7 hello || failures=$((failures + 1))
    run_host_case "workflow_smoke" workflow_smoke 3 3 21 11 smoke || failures=$((failures + 1))
    run_host_case "workflow_batch8" workflow_batch8 8 2 100 100 batch8 || failures=$((failures + 1))
    run_host_case "workflow_batch16" workflow_batch16 16 1 1000 200 batch16 || failures=$((failures + 1))
    run_host_case "generated_batch6" generated 6 5 55 9 generated || failures=$((failures + 1))

    echo "[host] matrix summary: failures=${failures} logs=${LOG_DIR}"
    [[ "${failures}" -eq 0 ]]
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --role)
            ROLE="$2"
            shift 2
            ;;
        --pci)
            PCI="$2"
            shift 2
            ;;
        --rep-pci)
            REP_PCI="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --batch)
            BATCH="$2"
            shift 2
            ;;
        --scores)
            SCORES="$2"
            shift 2
            ;;
        --seed)
            SEED="$2"
            shift 2
            ;;
        --grpc-id)
            GRPC_ID="$2"
            shift 2
            ;;
        --name)
            NAME="$2"
            shift 2
            ;;
        --repeat)
            REPEAT="$2"
            shift 2
            ;;
        --timeout)
            TIMEOUT_SEC="$2"
            shift 2
            ;;
        --legacy-bounceback)
            LEGACY_BOUNCE=1
            shift
            ;;
        --build-dir)
            BUILD_DIR="$2"
            BIN="${BUILD_DIR}/dpumesh"
            shift 2
            ;;
        --log-dir)
            LOG_DIR="$2"
            shift 2
            ;;
        --no-build)
            NO_BUILD=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown argument: $1" >&2
            usage >&2
            exit 1
            ;;
    esac
done

if [[ -z "${ROLE}" ]]; then
    usage >&2
    exit 1
fi

if [[ -z "${PCI}" ]]; then
    echo "--pci is required" >&2
    usage >&2
    exit 1
fi

ensure_build
ensure_binary

case "${ROLE}" in
    dpu)
        if [[ -z "${REP_PCI}" ]]; then
            echo "--rep-pci is required for --role dpu" >&2
            exit 1
        fi
        echo "[dpu] starting server: pci=${PCI} rep_pci=${REP_PCI} bin=${BIN}"
        if command -v stdbuf >/dev/null 2>&1; then
            exec stdbuf -oL -eL "${BIN}" -p "${PCI}" -r "${REP_PCI}"
        fi
        exec "${BIN}" -p "${PCI}" -r "${REP_PCI}"
        ;;
    host)
        run_host_case "${PROFILE}" "${PROFILE}" "${BATCH}" "${SCORES}" "${SEED}" "${GRPC_ID}" "${NAME}"
        echo "[host] logs=${LOG_DIR}"
        ;;
    matrix)
        run_matrix
        ;;
    *)
        echo "Invalid --role: ${ROLE}" >&2
        usage >&2
        exit 1
        ;;
esac
