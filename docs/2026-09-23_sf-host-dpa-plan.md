# Per-pod SF + host-owned DPA for the DPU→host path (blocking host), plan 2026-09-23

Goal: every pod gets its own SF; the host library opens that SF, runs **one
DPA thread per pod** that pulls DPU→host bytes for all of the pod's
connections (the mirror of the forward path), and the host **blocks** on a
progress-engine fd instead of polling. The forward path (host ring → DPU DPA
pool → DPU staging) and the DPU-side per-connection rcv_ring / tx_staging
export stay as they are.

## What is already verified on the testbed (jet1 / BF-3, DOCA 3.5)

| Item | Result |
|---|---|
| Host SF `enpbs0f0s0` (`mlx5_2`, aux `mlx5_core.sf.2`, sfnum 0, vhca 37) | exists; DPU representor `en3f0c1pf0sf0` (vuid `MT260960158CMLNXS0D0F0SF32768`) |
| Which DPU PF owns the SF representor | the SF was made on host PF `0b:00.0`, so its rep is on DPU **`03:00.0`** (opening it from `03:00.1` returns NOT_FOUND) |
| Comch server name scope | two servers named `DPUMesh0` on one device, one on the PF0 rep and one on the SF rep, create/start/progress together: the name only needs to be unique per (device, representor) |
| SF identity from inside a pod | `/sys/class/infiniband_verbs/uverbsN/ibdev` → `mlx5_2` → `…/device` → `mlx5_core.sf.2` → `sfnum`; `doca_devinfo_get_vhca_id` gives 37 on the host and `doca_devinfo_rep_get_vhca_id` gives 37 on the DPU |
| DPA capability on the SF | `doca_dpa_cap_is_supported` = SUCCESS, `doca_dpa_create` + `set_app` succeed |
| **DPA process start on the SF** | **fails**: `doca_dpa_start` → `DOCA_ERROR_DRIVER`, flexio `Failed to create PRM process. Status 0x3, syndrome 0x775dd0`, with an 8-EU partition for vhca 37 (EUs 64-71) and also as root |
| Host PF control | `doca_dpa_start` on `mlx5_0` also fails without a partition; creating a partition for vhca 0 (EUs 72-79) failed, so the PF control run is inconclusive |
| Push-wire DMA rates (today's API port) | 1 flow 371k DMA/s = 12.2 Gbps (= legacy), 16 flows 134 Gbps; see `bench-results/2026-09-22_dma-bench-api-port.md` |

Partition state left on the DPU: the 1-EU partition for vhca 37 was replaced by
EUs 64-71; vhca 38-43 keep their 1-EU partitions.

## Phase 0 — resolved 2026-09-24 (see design/HOST.md, pull wire)

- SF as DPA device: the process must be created on the PF, then
  `doca_dpa_device_extend` to the SF works end to end once the kernel and the
  init RPC call `doca_dpa_dev_device_set` (the official flow). Limit found:
  only one distinct SF can be extended at a time across processes (second
  SF's consumer-completion CQ fails, syndrome 0x5ecb3); many processes may
  share one SF or use the PF directly.
- PF as DPA device: works, and **many host processes can each hold a DPA
  process on the same PF** (two verified side by side), so the per-pod model
  is SF for Comch + PF (or one shared SF) for the DPA, one EU partition for
  the PF vhca.
- The pull wire (host-owned DPA thread per connection, `DPUMESH_WIRE=pull`)
  is implemented and measured; Phases 2-4 below shrink to: per-pod thread
  with a slot table (optional, EU saving), blocking on the PE fd, and the
  forward-ACK notification.

## Phase 0 (original) — unblock DPA-on-SF

The whole design rests on a host process creating a flexio process on its SF.
That is exactly what fails today. Order of work:

1. Decode syndrome `0x775dd0` (firmware `CREATE_PROCESS` PRM status 0x3 =
   bad parameter / not permitted) against the DOCA/flexio release notes for
   "DPA over SF"; check whether SF DPA needs a firmware or SF-creation flag
   (`mlxconfig` on the host shows `DPA_AUTHENTICATION=0`,
   `PER_PF_NUM_SF=1`, `PF_TOTAL_SF=236`; nothing SF-DPA specific is set).
2. Re-establish the host-PF baseline that worked earlier (memory note: reverse
   DPA on the host PF ran once a partition for the host vhca existed). The
   partition create for vhca 0 failed today; find out why (overlapping EU
   range, vhca 0 being the root partition, or the 15-partition/EU-group
   budget), then confirm `doca_dpa_start` on `mlx5_0`. If the PF also fails
   with the same syndrome the problem is the host-built `dpa_kernel.a` or the
   probe, not the SF.
3. Only then: the probe's remaining steps on the SF (thread start,
   `get_external_ptr` window over an imported DPU mmap, one
   `producer_dma_copy`). The window API requires a base (non-extended) DPA
   context; an SF context should be one, but this is the second unknown.
4. Decide the fallback if the SF cannot host a DPA process: DPU-owned DPA
   doing the reverse copy with `doca_dpa_dev_post_memcpy` (no host DPA, host
   stays on the push wire, no host doorbell), as written up on 2026-09-22.

Exit criterion: a host process on the SF runs a DPA thread that copies a
buffer from a DPU export into host memory and delivers the msgq completion
to the host consumer.

## Phase 1 — DPU side: per-PF workers and SF representors

Files: `src/transport/dpu/comch_server.c`, `dpu_worker.c`,
`src/transport/common/comch_common.h`, `linkerd2-proxy/linkerd/doca/src/shim.c`.

- `dmesh_doca_init(dev, rep, name)` takes a PCI rep only. Add a rep spec that
  is one of `pci:<bdf>`, `vuid:<id>`, `sf:<sfnum>` (resolve through
  `doca_devinfo_rep_create_list` + `rep_get_pci_func_type/sf_index/vuid`).
- A worker per **DPU PF** (`03:00.0` for host PF0 SFs, `03:00.1` for PF1),
  each with one comch server per representor, all named `DPUMesh0`. Today one
  worker owns one server; make the server list dynamic and rescan the rep
  list on a slow tick so an SF created after the worker started gets a server
  without a restart (the DPU-side "agent" from the pod-SF discussion).
- Flow modes: add `INGRESS_PULL` and `BACKEND_PULL` (`DMESH_FLOW_USES_PUSH`
  false) so the existing export branch in `dmesh_doca_ctrl_advance`
  (rcv_ring + tx_staging → `EXPORT_RCV_RING`) serves them; `send_staged`
  already writes descriptors for non-push modes.
- Staging reuse safety: the kernel publishes `consumer_head` right after
  submitting `producer_dma_copy`; on the reverse path that lets the DPU
  writer reuse tx_staging bytes a copy may still be reading. Publish after
  the producer completion instead (poll `dpa_producer_comp`). The forward
  path has the same latent race.
- Proxy: `LINKERD2_PROXY_DOCA_REP_PCI_ADDR` becomes a list (or "all reps of
  this device"), one shim `objects` per rep.

## Phase 2 — host DPA kernel (per-pod thread)

Files: `src/transport/device/rev_kernel.c` (new), `common/dpa_common.h`,
`build_dpacc.sh`/`meson.build` (second dpacc app for the host side).

- `struct rev_thread_arg { stop, stopped; uint32_t nslots; struct rev_slot
  slots[32]; }`, `rev_slot { active, quiesced, buf_arr (rcv_ring), ring_size,
  src_mmap (tx_staging), dst_mmap, dst_base, dst_size, pos, rd_pos, fwd_ctrl
  }`. The host writes slots with `doca_dpa_h2d_memcpy` and sets `active` last;
  the kernel reads with `__dpa_thread_window_read_inv`.
- Loop: round-robin active slots; per slot pop descriptors, 128 B rule
  (multiple of 128 or one ≤128 B block), `producer_dma_copy` tx_staging →
  host window with imm `{slot, pos, len, count}`; `is_consumer_empty` is a
  non-blocking check (skip the slot, no head-of-line stall); publish
  `consumer_head` after completion; honor `rd_pos` (host release watermark).
- Forward custody ACKs have no completion on this wire: every K spins the
  thread also reads each active slot's forward ring ctrl (host memory) and,
  when `consumer_head` moved, sends an imm-only ACK message. This is what
  lets the host block for ACKs too.
- `active=0` → the thread drains the slot's in-flight copy, sets
  `quiesced=1`; the host waits for it before unmapping.
- One thread = one EU per pod; the partition per SF vhca needs ≥1 EU (plus
  what flexio needs to create the process, to be learned in Phase 0).

## Phase 3 — host wire layer

Files: `src/transport/host/wire_pull.c` (new, same `wire_push.h` contract
plus `wire_dev_fd()`), `wire_push.c` kept behind `DPUMESH_WIRE=push`.

- `wire_dev_open`: device by `DPUMESH_DEV` (ibdev name) or auto-discovery
  (the single SF-type device whose uverbs node the pod owns), falling back
  to `DPUMESH_PCI_ADDR`. Then DPA ctx + app, one thread, its msgq
  (consumer/producer completions bound to the thread), a pod PE, the thread
  arg with all slots inactive, `thread_run`.
- `wire_conn_open`: comch client (`init_comch_ctrl_path_client`), forward
  export as today, then wait for `EXPORT_RCV_RING`, import the two mmaps on
  the SF device, `setup_dpa_buf_array` over the ring, fill the slot, activate.
- RX: the msgq recv callback turns each imm into a per-slot `{pos,len}` entry
  that `wire_conn_rx_next` returns; `wire_conn_rx_consumed` batches
  `rd_pos` writes (one `h2d_memcpy` per tick, like `rx_watermark` in shim.c).
- Close: deactivate → wait `quiesced` → drain the slot's completions → destroy
  buf_arr and mmaps → comch disconnect. The DPU frees tx_staging only after
  the disconnect, so this order keeps DMA off freed memory in both directions.
- Process exit: stop/stopped handshake with the thread, then completions,
  then the thread (never `doca_dpa_thread_stop` on a comch-attached thread).

## Phase 4 — carrier and core: blocking

Files: `src/core/carrier_push.c` (becomes wire-agnostic), `src/core/dmesh_core.c`.

- `dmesh_native_wait` = `epoll_wait` on {msgq PE fd, comch client PE fd}
  after `doca_pe_request_notification`; on wake clear + progress until empty,
  re-arm before the final progress (the lost-wakeup rule).
- ACK imm → `DMESH_NATIVE_ACK` events; RX imm → `DMESH_NATIVE_RX`.
- Pull wire forces one drain shard (the PE is single-owner) and drops the
  adaptive nap; with one EQ, `dmesh_eq_fd` can hand out the PE fd directly
  (the "drain 0" option), which is the configuration Go runtimes want.
- `dmesh_eq_next_deadline_ns` and the tail timer stay as they are.

## Phase 5 — packaging

- Device plugin (sriov-network-device-plugin auxiliary/SF selector →
  `nvidia.com/sf`), pod spec `limits: nvidia.com/sf: 1` + `IPC_LOCK`,
  `DPUMESH_POD_IP` from the downward API; the library discovers the SF and
  uses the fixed server name. Node prep: SF creation on the host PF, the
  DPU worker for that PF, EU partition per SF vhca (both scriptable in the
  DPU agent; partitions do not survive reboot).
- `design/HOST.md`, `.env.example`, `tests/` (slot table and quiesce logic
  host-only), `dpumesh_host` gains `--wire` so the same benchmark runs on both
  wires.

## Phase 6 — evaluation (same harness as 2026-09-22)

- `dpumesh_host` sink/echo on the pull wire vs the push wire: DMA/s, Gbps, RTT,
  and above all **host CPU at idle and under load** (the push wire costs up to
  ~9 cores of polling at 16 echo threads today).
- Blocking wake latency: 64 B echo RTT with the host asleep between messages.
- Lifecycle: 1000× QP open/close per pod, pod restart, EU release, fd count.
- Scale: pods × connections against the 190-EU budget (one EU per pod here).

## Risks, in order

1. Phase 0 (DPA process on an SF) is unresolved; the fallback changes
   Phases 2-4 into the DPU-owned `post_memcpy` design.
2. `get_external_ptr` on an SF DPA context (base vs extended).
3. flexio thread teardown wedges (`0xe5300`) now on the host side; mitigated
   by one long-lived thread per pod.
4. Per-PF workers double the DPU-side objects the proxy manages.
