#!/bin/bash
# Give the host-facing SFs of one PF their DPA EU partitions, in groups.
#
#   sudo scripts/assign_dpa_eu.sh -p <pf_index> -g <sfs_per_partition> -e <eus_per_partition> -b <eu_base> [-n] [-f]
#   sudo scripts/assign_dpa_eu.sh -p <pf_index> -d [-n]
#
#   -p  PF index (pfnum) whose SFs are grouped (see scripts/create_sf.sh)
#   -g  G: SFs per partition; SF sfnum s belongs to group k = s / G
#   -e  E: EUs per partition; group k gets EUs [base + k*E, base + (k+1)*E)
#   -b  first EU of the host range (keep it above what the DPU's own workers use)
#   -n  dry run: print the plan and the commands, change nothing
#   -f  replace an existing partition that overlaps a planned one (any shared
#       vhca or EU) but is not identical; without -f such a conflict is reported
#       and the script exits 1 after handling the other groups
#   -d  delete: destroy every partition that contains a vhca of this PF's SFs
#       (-g/-e/-b not needed). Refused by firmware while a DPA process runs
#       on one of the partition's vhcas; `dpaeumgmt info status` shows them.
#
# The sfnum -> vhca_id mapping comes from the SF representors of the DPU PF
# (doca_caps --list-rep-devs); the DPU PCI address of the PF is looked up from
# `mlxdevm port show`. Idempotent: a partition that already has exactly the
# planned vhca set and EU set is left alone. Partitions do not survive a
# reboot and cannot be edited, so re-run this after create_sf.sh on boot.
#
# Limits on BlueField-3 (dpaeumgmt partition info): 15 partitions, 32 vhcas
# per partition, 190 EUs in total; a destroy is refused while a DPA process
# runs on one of the partition's vhcas.
set -euo pipefail

is_dpu() {
    grep -qs -i 'bf-bundle\|bluefield' /etc/mlnx-release && return 0
    [[ "$(uname -m)" == "aarch64" ]] && lspci 2>/dev/null | grep -qi 'BlueField.* SoC' && return 0
    return 1
}
if ! is_dpu; then
    echo "error: this script must run on the BlueField DPU (Arm side): EU partitions are managed with dpaeumgmt on the DPU" >&2
    exit 1
fi

DPAEUMGMT=${DPAEUMGMT:-/opt/mellanox/doca/tools/dpaeumgmt}
DOCA_CAPS=${DOCA_CAPS:-/opt/mellanox/doca/tools/doca_caps}
MLXDEVM=${MLXDEVM:-/opt/mellanox/iproute2/sbin/mlxdevm}
DPA_DEV=${DPA_DEV:-mlx5_0}          # dpaeumgmt device (the DPU's root DPA device)
MAX_GROUPS=${MAX_EU_GROUPS:-1}      # --max_num_eu_group per partition
CONTROLLER=1
PF_IDX=""; G=""; E=""; BASE=""; DRY=0; FORCE=0; DELETE=0

usage() { sed -n '2,29p' "$0"; exit 2; }
while getopts "p:g:e:b:nfdh" opt; do
    case $opt in
        p) PF_IDX=$OPTARG ;; g) G=$OPTARG ;; e) E=$OPTARG ;; b) BASE=$OPTARG ;;
        n) DRY=1 ;; f) FORCE=1 ;; d) DELETE=1 ;; *) usage ;;
    esac
done
if [[ $DELETE -eq 1 ]]; then
    [[ -n "$PF_IDX" && "$PF_IDX" =~ ^[0-9]+$ ]] || usage
    G=1; E=1; BASE=0   # unused in delete mode
else
    [[ -n "$PF_IDX" && -n "$G" && -n "$E" && -n "$BASE" ]] || usage
fi
for v in "$PF_IDX" "$G" "$E" "$BASE"; do [[ "$v" =~ ^[0-9]+$ ]] || { echo "error: -p -g -e -b take integers" >&2; exit 2; }; done
(( G >= 1 && G <= 32 )) || { echo "error: -g must be 1..32 (vhcas per partition)" >&2; exit 2; }
(( E >= 1 )) || { echo "error: -e must be >= 1" >&2; exit 2; }
for t in "$DPAEUMGMT" "$DOCA_CAPS" "$MLXDEVM"; do [[ -x "$t" ]] || { echo "error: $t not found" >&2; exit 1; }; done
if [[ $DRY -eq 0 && $EUID -ne 0 ]]; then echo "error: run as root" >&2; exit 1; fi

# real runs keep dpaeumgmt's stdout blurb quiet; its errors still reach stderr
run() { if [[ $DRY -eq 1 ]]; then echo "+ $*"; else "$@" >/dev/null; fi; }

# --- DPU PCI address of the host PF -----------------------------------------
PCI_ADDR=$("$MLXDEVM" port show 2>/dev/null | awk -v c="$CONTROLLER" -v p="$PF_IDX" '
    /flavour pcipf/ && $0 ~ ("controller " c " pfnum " p " ") { split($1, a, "/"); print a[2]; exit }')
[[ -n "$PCI_ADDR" ]] || { echo "error: no pcipf port with controller $CONTROLLER pfnum $PF_IDX" >&2; exit 1; }
SHORT_PCI=${PCI_ADDR#0000:}

# --- sfnum -> vhca from the SF representors ----------------------------------
# One "representor-PCI:" block per rep; SF blocks carry vhca_id and sf_index.
mapfile -t SF_ROWS < <("$DOCA_CAPS" --list-rep-devs -p "$SHORT_PCI" 2>/dev/null | awk '
    function flush() { if (t == "SF" && sf != "" && v != "") print sf, v; t = ""; sf = ""; v = "" }
    /representor-PCI:/ { flush(); next }
    $1 == "pci_func_type" { t = $2 }
    $1 == "vhca_id" { v = $2 }
    $1 == "sf_index" { sf = $2 }
    END { flush() }' | sort -n)
(( ${#SF_ROWS[@]} > 0 )) || { echo "error: no SF representors under $PCI_ADDR (run scripts/create_sf.sh first)" >&2; exit 1; }
if [[ $DELETE -eq 1 ]]; then
    echo "PF $PF_IDX -> $PCI_ADDR: ${#SF_ROWS[@]} SF(s); deleting their partitions"
else
    echo "PF $PF_IDX -> $PCI_ADDR: ${#SF_ROWS[@]} SF(s); groups of $G, $E EUs each from EU $BASE"
fi

declare -A GROUP_VHCAS GROUP_SFS
for row in "${SF_ROWS[@]}"; do
    sf=${row% *}; vhca=${row#* }
    k=$(( sf / G ))
    GROUP_VHCAS[$k]="${GROUP_VHCAS[$k]:+${GROUP_VHCAS[$k]} }$vhca"
    GROUP_SFS[$k]="${GROUP_SFS[$k]:+${GROUP_SFS[$k]},}$sf"
done

# --- existing partitions ------------------------------------------------------
# expand "38-43" / "80,81" / "64-71,90" into a sorted space-separated set
expand() { tr ',' '\n' <<<"$1" | while IFS= read -r x; do
        if [[ $x == *-* ]]; then seq "${x%-*}" "${x#*-}"; else echo "$x"; fi; done | sort -n | tr '\n' ' ' | sed 's/ $//'; }

declare -A EX_VHCAS EX_EUS
while IFS='|' read -r id vhcas eus; do
    [[ -n "$id" ]] || continue
    EX_VHCAS[$id]=$(expand "$vhcas"); EX_EUS[$id]=$(expand "$eus")
done < <("$DPAEUMGMT" partition query -d "$DPA_DEV" 2>/dev/null | awk '
    /EU Partition ID:/ { id = $NF }
    /VHCA IDs, namely:/ { v = $NF }
    /member EUs are:/ { print id "|" v "|" $NF }')

intersects() { # two space-separated sets
    local a=" $1 " x
    for x in $2; do [[ $a == *" $x "* ]] && return 0; done
    return 1
}

# --- delete mode: every partition holding one of this PF's SF vhcas -------------
if [[ $DELETE -eq 1 ]]; then
    all_vhcas=$(for row in "${SF_ROWS[@]}"; do echo "${row#* }"; done | sort -n | tr '\n' ' ' | sed 's/ $//')
    procs=$("$DPAEUMGMT" info status -d "$DPA_DEV" 2>/dev/null | awk '/processes:/ { print $2 }')
    [[ "${procs:-0}" == "0" ]] || echo "warning: $procs DPA process(es) running on $DPA_DEV; a destroy of a partition in use will be refused" >&2
    hits=0
    for id in $(printf '%s\n' "${!EX_VHCAS[@]}" | sort -n); do
        if intersects "${EX_VHCAS[$id]}" "$all_vhcas"; then
            echo "partition $id: vhca ${EX_VHCAS[$id]// /,}, EUs ${EX_EUS[$id]// /,} -> destroy"
            run "$DPAEUMGMT" partition destroy -d "$DPA_DEV" --id_partition "$id"
            hits=$((hits + 1))
        fi
    done
    (( hits > 0 )) || echo "no partition holds a vhca of PF $PF_IDX's SFs"
    if [[ $DRY -eq 0 ]]; then
        left=$("$DPAEUMGMT" partition query -d "$DPA_DEV" 2>/dev/null | awk '/In total there are/ { print $5 }')
        echo "done: $hits partition(s) destroyed, ${left:-0} left on $DPA_DEV"
    fi
    exit 0
fi

# --- plan and apply -------------------------------------------------------------
printf '%-6s %-10s %-22s %-14s %s\n' group sfnums vhcas EUs action
conflicts=0
for k in $(printf '%s\n' "${!GROUP_VHCAS[@]}" | sort -n); do
    vhcas=$(tr ' ' '\n' <<<"${GROUP_VHCAS[$k]}" | sort -n | tr '\n' ' ' | sed 's/ $//')
    eu_lo=$(( BASE + k * E )); eu_hi=$(( eu_lo + E - 1 ))
    eus=$(seq "$eu_lo" "$eu_hi" | tr '\n' ' ' | sed 's/ $//')
    vlist=${vhcas// /,}
    action=""; victims=""
    for id in "${!EX_VHCAS[@]}"; do
        if [[ "${EX_VHCAS[$id]}" == "$vhcas" && "${EX_EUS[$id]}" == "$eus" ]]; then action="ok (partition $id)"; break; fi
        if intersects "${EX_VHCAS[$id]}" "$vhcas" || intersects "${EX_EUS[$id]}" "$eus"; then victims="$victims $id"; fi
    done
    if [[ -z "$action" && -n "$victims" && $FORCE -eq 0 ]]; then
        action="CONFLICT with partition(s)$victims (use -f to replace)"; conflicts=$((conflicts + 1))
    fi
    printf '%-6s %-10s %-22s %-14s %s\n' "$k" "${GROUP_SFS[$k]}" "$vlist" "$eu_lo-$eu_hi" "${action:-create}"
    [[ -z "$action" || "$action" == create ]] || continue
    for id in $victims; do
        run "$DPAEUMGMT" partition destroy -d "$DPA_DEV" --id_partition "$id"
        unset "EX_VHCAS[$id]" "EX_EUS[$id]"
    done
    run "$DPAEUMGMT" partition create -d "$DPA_DEV" --vhca_list "$vlist" --range_eus "$eu_lo-$eu_hi" --max_num_eu_group "$MAX_GROUPS"
done

if [[ $DRY -eq 0 ]]; then
    echo "== partitions now on $DPA_DEV"
    "$DPAEUMGMT" partition query -d "$DPA_DEV" 2>/dev/null | awk '
        /EU Partition ID:/ { id = $NF } /VHCA IDs, namely:/ { v = $NF } /member EUs are:/ { print "  partition " id ": vhca " v ", EUs " $NF }'
fi
(( conflicts == 0 )) || { echo "error: $conflicts group(s) conflict with existing partitions" >&2; exit 1; }
