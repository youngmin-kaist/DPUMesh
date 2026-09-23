#!/bin/bash
# Create and activate host-facing SFs (controller 1) on a BlueField-3 DPU.
#
#   sudo scripts/create_sf.sh -p <pf_index> -s <num_sf> [-o <first_sfnum>] [-d] [-n]
#
#   -p  PF index (pfnum) the SFs hang off; the DPU PCI address is looked up
#       from `mlxdevm port show` (the pcipf port with controller 1 pfnum <p>)
#   -s  number of SFs wanted: sfnum o..o+s-1 (o = -o, default 0). An sfnum
#       that already exists is counted, not recreated (it is activated if it
#       is not active yet).
#   -o  first sfnum of the range (default 0)
#   -d  delete instead of create: deactivate and remove sfnum o..o+s-1 (an
#       sfnum that does not exist is skipped). Refused by the kernel while a
#       host process still holds the SF.
#   -n  dry run: print the commands instead of running them
#
# Each new SF gets a locally administered, deterministic MAC
# 02:d0:<pf>:<sfnum hi>:<sfnum lo>:<node byte> (node byte from
# SF_MAC_NODE, default 00), then
#   mlxdevm port function set pci/<addr>/<port> hw_addr <mac> trust off state active
# Runs on the DPU as root. Representors show up on the DPU as
# en3f<pf>c1pf<pf>sf<sfnum>; the SF appears on the host as an auxiliary
# mlx5_core.sf.N device.
set -euo pipefail

# Refuse to run anywhere but on the BlueField Arm side: the BF bundle release
# file exists only in DPU images, and the SoC shows up on the DPU's own PCI bus.
is_dpu() {
    grep -qs -i 'bf-bundle\|bluefield' /etc/mlnx-release && return 0
    [[ "$(uname -m)" == "aarch64" ]] && lspci 2>/dev/null | grep -qi 'BlueField.* SoC' && return 0
    return 1
}
if ! is_dpu; then
    echo "error: this script must run on the BlueField DPU (Arm side), not on the host: host SFs are created from the DPU with 'mlxdevm port add ... controller 1'" >&2
    exit 1
fi

MLXDEVM=${MLXDEVM:-/opt/mellanox/iproute2/sbin/mlxdevm}
CONTROLLER=1
PF_IDX=""
NUM_SF=""
FIRST=0
DELETE=0
DRY=0

usage() { sed -n '2,24p' "$0"; exit 2; }

while getopts "p:s:o:dnh" opt; do
    case $opt in
        p) PF_IDX=$OPTARG ;;
        s) NUM_SF=$OPTARG ;;
        o) FIRST=$OPTARG ;;
        d) DELETE=1 ;;
        n) DRY=1 ;;
        *) usage ;;
    esac
done
[[ -n "$PF_IDX" && -n "$NUM_SF" ]] || usage
[[ "$PF_IDX" =~ ^[0-9]+$ && "$NUM_SF" =~ ^[0-9]+$ && "$FIRST" =~ ^[0-9]+$ ]] || { echo "error: -p, -s and -o take integers" >&2; exit 2; }
LAST=$(( FIRST + NUM_SF ))
[[ -x "$MLXDEVM" ]] || { echo "error: $MLXDEVM not found (set MLXDEVM=)" >&2; exit 1; }
if [[ $DRY -eq 0 && $EUID -ne 0 ]]; then echo "error: run as root" >&2; exit 1; fi

run() { if [[ $DRY -eq 1 ]]; then echo "+ $*"; else "$@"; fi; }

# One snapshot of the port table; every lookup below parses it.
port_table() { "$MLXDEVM" port show 2>/dev/null; }

# PCI address of the host-facing PF <pfnum> on this controller.
PCI_ADDR=$(port_table | awk -v c="$CONTROLLER" -v p="$PF_IDX" '
    /flavour pcipf/ && $0 ~ ("controller " c " pfnum " p " ") { split($1, a, "/"); print a[2]; exit }')
[[ -n "$PCI_ADDR" ]] || { echo "error: no pcipf port with controller $CONTROLLER pfnum $PF_IDX in '$MLXDEVM port show'" >&2; exit 1; }
echo "PF $PF_IDX -> pci/$PCI_ADDR (controller $CONTROLLER)"

# Port index of SF <sfnum> under this PF, empty if it does not exist.
sf_port_index() {
    port_table | awk -v c="$CONTROLLER" -v p="$PF_IDX" -v s="$1" '
        /flavour pcisf/ && $0 ~ ("controller " c " pfnum " p " sfnum " s " ") { split($1, a, "/"); sub(":", "", a[3]); print a[3]; exit }'
}

# state of an SF port ("active", "inactive", ...) from the "function:" line
# that follows its port line in `port show` (there is no `function show`).
sf_state() {
    port_table | awk -v port="pci/$PCI_ADDR/$1:" '
        $1 == port { hit = 1; next }
        hit && /^pci\// { exit }
        hit { for (i = 1; i < NF; i++) if ($i == "state") { print $(i+1); exit } }'
}

sf_mac() {
    local node=${SF_MAC_NODE:-00}
    printf '02:d0:%02x:%02x:%02x:%s' "$PF_IDX" $(( $1 >> 8 & 0xff )) $(( $1 & 0xff )) "$node"
}

if [[ $DELETE -eq 1 ]]; then
    deleted=0; missing=0
    for (( sf = FIRST; sf < LAST; sf++ )); do
        idx=$(sf_port_index "$sf")
        if [[ -z "$idx" ]]; then echo "sfnum $sf: not present"; missing=$((missing + 1)); continue; fi
        echo "sfnum $sf: deleting (port $idx)"
        if [[ "$(sf_state "$idx")" == "active" ]]; then
            run "$MLXDEVM" port function set "pci/$PCI_ADDR/$idx" state inactive
        fi
        run "$MLXDEVM" port del "pci/$PCI_ADDR/$idx"
        deleted=$((deleted + 1))
    done
    echo "done: sfnum $FIRST..$((LAST - 1)) on PF $PF_IDX ($deleted deleted, $missing not present)"
    [[ $DRY -eq 1 ]] || "$MLXDEVM" port show | grep -E "flavour pcisf controller $CONTROLLER pfnum $PF_IDX " || echo "(no SFs left on PF $PF_IDX)"
    exit 0
fi

created=0; existing=0
for (( sf = FIRST; sf < LAST; sf++ )); do
    idx=$(sf_port_index "$sf")
    if [[ -n "$idx" ]]; then
        existing=$((existing + 1))
        st=$(sf_state "$idx")
        if [[ "$st" == "active" ]]; then
            echo "sfnum $sf: exists (port $idx, active)"
            continue
        fi
        echo "sfnum $sf: exists (port $idx, state '${st:-?}'), activating"
        # an existing SF keeps its hw_addr (changing it is refused once set)
        run "$MLXDEVM" port function set "pci/$PCI_ADDR/$idx" state active
        continue
    else
        echo "sfnum $sf: creating"
        run "$MLXDEVM" port add "pci/$PCI_ADDR" flavour pcisf pfnum "$PF_IDX" sfnum "$sf" controller "$CONTROLLER"
        created=$((created + 1))
        if [[ $DRY -eq 1 ]]; then idx="<port>"; else
            idx=$(sf_port_index "$sf")
            [[ -n "$idx" ]] || { echo "error: sfnum $sf not visible after port add" >&2; exit 1; }
        fi
    fi
    run "$MLXDEVM" port function set "pci/$PCI_ADDR/$idx" hw_addr "$(sf_mac "$sf")" trust off state active
done

echo "done: sfnum $FIRST..$((LAST - 1)) on PF $PF_IDX ($created created, $existing already existed)"
[[ $DRY -eq 1 ]] || "$MLXDEVM" port show | grep -E "flavour pcisf controller $CONTROLLER pfnum $PF_IDX " || true
