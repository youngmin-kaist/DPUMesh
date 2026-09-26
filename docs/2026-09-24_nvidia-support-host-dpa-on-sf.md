# Support request: host DPA processes on BlueField-3 SFs (per-tenant DPA offload)

## Summary

We are building a host-side DPA offload where each tenant (Kubernetes pod) on
the x86 host owns one scalable function (SF) of a BlueField-3 and runs its own
DPA thread on that SF. On the current firmware / DOCA 3.5 host stack we hit
three limits that make a per-SF DPA thread impossible. We would like to know
whether they are intended restrictions, firmware bugs, or configuration
mistakes on our side, and whether a firmware or DOCA version lifts them.

1. A DPA process cannot be created with a host SF as its device: `doca_dpa_start`
   fails in the flexio `CREATE_PROCESS` PRM command with status 0x3,
   syndrome 0x775dd0.
2. With the process created on the host PF and extended to an SF
   (`doca_dpa_device_extend`), a thread pinned to an EU that belongs to the SF
   vhca's own EU partition fails `doca_dpa_thread_start` (DOCA_ERROR_DRIVER);
   only EUs of the PF vhca's partition work. So an EU partition assigned to an
   SF vhca has no effect at all.
3. Once a DPA thread is running on one extended SF, a DOCA Comch msgq consumer
   completion cannot be started on any other SF: its completion queue
   (`Failed to create consumer completion WOD CQ`) is refused by the firmware
   with devx syndrome 0x5ecb3. This happens within one process or across
   processes, with any EU partition layout, and regardless of whether the
   first SF itself has a consumer completion. Everything else on the second SF
   (extension, thread, `doca_dpa_completion`, msgqs) succeeds. A thread that is
   created but not started on the first SF does not block.

Item 3 is the blocking one for us: it limits the whole host to a single SF
that can run DPA threads with msgq completions.

## Environment

| | Host (x86) | BlueField-3 (Arm) |
|---|---|---|
| Device | MT43244 BlueField-3, part 900-9D3B6-00CV-A_Ax, PSID MT_0000000884 | same card |
| Firmware | 32.50.1002 | 32.50.1002 |
| OS / kernel | Ubuntu, 6.8.0-139-generic | bf-bundle-3.5.0-89_26.07_ubuntu-24.04_64k_prod, 6.8.0-1030-bluefield-64k |
| DOCA | doca-sdk-dpa 3.5.0098-1, doca-runtime 3.5.0-082000 | doca-runtime 3.5.0098-1.26.07.0.7.7 |
| flexio | flexio-sdk 26.07.3297-1 | (bundle) |
| OFED | 26.07-0.7.7 | (bundle) |
| Host PFs | 0b:00.0 (mlx5_0, vhca 0), 0b:00.1 (mlx5_1) | 03:00.0 / 03:00.1 |
| Host SFs | created on PF 0b:00.0: mlx5_2..mlx5_5 = sfnum 0..3, vhca 37..40 | representors en3f0c1pf0sf0..3 on 03:00.0 |

SF creation (on the DPU):

```
/opt/mellanox/iproute2/sbin/mlxdevm port add pci/0000:03:00.0 flavour pcisf pfnum 0 sfnum <n> controller 1
/opt/mellanox/iproute2/sbin/mlxdevm port function set pci/0000:03:00.0/<port> hw_addr <mac> trust off state active
```

EU partitions (on the DPU, `dpaeumgmt partition query -d mlx5_0`):

```
1) EU Partition ID: 1   VHCA IDs: 0       EUs: 0-63
2) EU Partition ID: 2   VHCA IDs: 37-40   EUs: 64-71
```

Item 3 was also reproduced with one partition per SF vhca (37: EUs 64-67,
38: EUs 68-71) and with no partition for the SF vhcas at all (only the PF
partition). Item 1 was also reproduced with an 8-EU partition for the SF vhca
and running as root.

## What works

- Any number of host processes can each create their own DPA process on the
  same host PF (0b:00.0) and run threads concurrently.
- A DPA process created on the PF and extended to one SF runs threads,
  `doca_dpa_completion`, Comch msgqs, consumer completions, mmaps and buf
  arrays on the SF at the same throughput as on the PF (our data path: 1 flow
  10.7 Gbit/s, 4 flows 32 Gbit/s, 64 B round trip 25 us). The kernel switches
  device with `doca_dpa_dev_device_set`, as in the DOCA extended-context
  sample.
- Several processes can extend to the *same* SF concurrently.

## Reproduction

Two small programs (sources attached as `sf_dpa_probe.c` and
`sf_ext_probe.c`; they link a DPA application built with dpacc whose thread
function is a plain polling loop). Each prints every DOCA call with its result
and ends with `RESULT: <n> failing step(s)`. `PROBE_SDK_LOG=debug` enables the
DOCA SDK debug log, which shows the devx syndromes quoted below.

### 1. DPA process directly on an SF

```
$ ./sf_dpa_probe mlx5_2
open_doca_device_with_ibdev_name(...)      DOCA_SUCCESS
device mlx5_2 func_type=2 vhca_id=37 dpa_supported=DOCA_SUCCESS
doca_dpa_create(dev, &dpa)                 DOCA_SUCCESS
doca_dpa_set_app(dpa, app)                 DOCA_SUCCESS
doca_dpa_start(dpa)                        DOCA_ERROR_DRIVER
flexio_prm_create_process 316 - Failed to create PRM process. Status is 0x3, syndrome 0x775dd0.
```

`./sf_dpa_probe mlx5_0` (the PF) succeeds.

### 2. Extended context: thread affinity to the SF partition's EUs

```
$ PROBE_EU1=0  ./sf_ext_probe mlx5_0 mlx5_2     # EU of the PF partition
RESULT: 0 failing step(s)

$ PROBE_EU1=64 ./sf_ext_probe mlx5_0 mlx5_2     # EU of the SF vhca partition
doca_dpa_eu_affinity_create / _set / doca_dpa_thread_set_affinity   DOCA_SUCCESS
doca_dpa_thread_start(th)                  DOCA_ERROR_DRIVER
```

An unassigned EU (e.g. 100) behaves exactly like 64.
`doca_dpa_get_total_num_eus_available` returns 254 on both the base and the
extended context.

### 3. Consumer completion on a second SF

```
$ ./sf_ext_probe mlx5_0 mlx5_2 mlx5_3
== DPA process on mlx5_0, extended to mlx5_2 (vhca 37)
   thread, doca_dpa_completion, mmap, buf array, msgqs, consumer completion:  all DOCA_SUCCESS
== same process: extend to a second SF mlx5_3 (vhca 38)
   doca_dpa_device_extend, thread, doca_dpa_completion, msgq:                 all DOCA_SUCCESS
doca_comch_consumer_completion_start(cc2)  DOCA_ERROR_DRIVER
RESULT: 1 failing step(s)
```

With `PROBE_SDK_LOG=debug`:

```
[DOCA][ERR][CORE][linux_devx_obj.cpp:133] Failed to create devx object with syndrome=0x5ecb3
[DOCA][ERR][CORE][doca_cq.cpp:837] CQ ...: Failed to create CQ DevX object. Fail syndrome=388275
[DOCA][ERR][DPA][dpa_core_utils.cpp:158] Failed to create CQ
[DOCA][ERR][DPA][dpa_comch_msgq.cpp:137] Failed to create consumer completion WOD CQ
[DOCA][ERR][DPA][dpa_comch_msgq.cpp:998] Failed to start DPA CommChannel consumer completion context
[DOCA][ERR][COMCH][doca_comch_msgq.cpp:941] Failed to start consumer completion context: failed to create completion context on DPA: error=DOCA_ERROR_DRIVER
```

Variations, all reproduced on this system:

| First SF (mlx5_2) state while the second SF (mlx5_3) is set up | Second SF consumer completion |
|---|---|
| full setup (started thread + completions + msgqs) | fails, 0x5ecb3 |
| no consumer completion on the first SF | fails |
| no msgqs and no consumer completion on the first SF | fails |
| started thread only (no completion, no msgq) | fails |
| thread created but **not started** | succeeds |
| extension only (no thread) | succeeds |
| first SF in another process, started thread | fails |
| first SF in another process, thread not started | succeeds |
| order reversed (mlx5_3 first, then mlx5_2) | fails the same way |
| threads pinned to different PF EUs (1 and 2) | fails |
| per-SF EU partitions, or no SF partition | fails |

Command lines for the variations (`PROBE_NOSTART_THREAD1=1`,
`PROBE_SKIP_THREAD1=1`, `PROBE_SKIP_MSGQ1=1`, `PROBE_SKIP_CC1=1`,
`PROBE_EU1=<eu> PROBE_EU2=<eu>`, and `HOLD=<seconds>` to keep the first
process alive while a second one runs) are documented at the top of
`sf_ext_probe.c`.

## Questions

1. Is a DPA process owned by an SF vhca unsupported by design on BlueField-3,
   or is there a firmware / mlxconfig setting (or a newer firmware) that
   enables it? What does `CREATE_PROCESS` status 0x3 / syndrome 0x775dd0 mean?
2. For an extended context, is it expected that only the base PF's EU
   partition is usable, so an EU partition assigned to the SF vhca is never
   consulted? If so, what is the intended use of assigning SF vhcas to EU
   partitions with `dpaeumgmt`?
3. What resource does a running DPA thread on an extended SF hold that
   prevents a Comch msgq consumer completion CQ on another SF (syndrome
   0x5ecb3)? Is it a per-process, per-PF, or per-device limit, and is there a
   way to lift it (firmware version, mlxconfig, DOCA API)? Our target is one
   SF per tenant, each with its own DPA thread and msgq completions, all
   extended from one PF-owned DPA process (or from one process per tenant).
4. If the per-SF model is not achievable, is "one DPA process per tenant on
   the PF, Comch on the tenant's SF" the recommended deployment, and are there
   documented limits on the number of DPA processes per PF?

## Attachments

- `sf_dpa_probe.c`, `sf_ext_probe.c` (host programs; build line in the
  accompanying Makefile: gcc against libdoca-sdk-dpa, libdoca-sdk-comch and
  libflexio, plus the dpacc-built DPA application archive)
- Full outputs with `PROBE_SDK_LOG=debug` for cases 1 and 3
- `dpaeumgmt partition query -d mlx5_0` output, `mlxdevm port show` output
