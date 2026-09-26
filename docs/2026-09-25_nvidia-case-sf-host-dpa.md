# NVIDIA Enterprise Support case: host-side DPA on BlueField-3 scalable functions

## Title

BlueField-3 / DOCA 3.5: host DPA cannot be owned by an SF, and a running DPA thread on one extended SF blocks Comch consumer-completion CQs on every other SF (syndromes 0x775dd0, 0x5ecb3)

## Description

### Goal

We run a multi-tenant host: each Kubernetes pod owns one scalable function (SF)
of a BlueField-3 and must run its own DPA thread with DOCA Comch msgq
completions on that SF (a host-side DPA offload of a DMA data path). On the
current firmware this is not achievable; we would like to know whether the
three behaviours below are by design, firmware defects, or a configuration
error, and which firmware or DOCA version changes them.

### Environment

- Card: BlueField-3, MT43244, part 900-9D3B6-00CV-A_Ax, PSID MT_0000000884, firmware 32.50.1002 (both host and DPU report it)
- Host: x86, Ubuntu, kernel 6.8.0-139-generic, OFED 26.07-0.7.7, doca-sdk-dpa 3.5.0098-1, doca-runtime 3.5.0-082000, flexio-sdk 26.07.3297-1
- DPU: bf-bundle-3.5.0-89_26.07_ubuntu-24.04_64k_prod, doca-runtime 3.5.0098, kernel 6.8.0-1030-bluefield-64k
- Host PFs: 0b:00.0 (mlx5_0, vhca 0) and 0b:00.1. SFs created from the DPU on PF 0 with
  `mlxdevm port add pci/0000:03:00.0 flavour pcisf pfnum 0 sfnum N controller 1`, activated with
  `hw_addr <mac> trust on state active`; on the host they enumerate as mlx5_2..mlx5_5 (vhca 37..40)
- DPA EU partitions (`dpaeumgmt partition query -d mlx5_0`): partition 1 = vhca 0, EUs 0-63; partition 2 = vhca 37-40, EUs 64-71

### Behaviour 1: a DPA process cannot be created with an SF as its device

`doca_dpa_create(sf_dev)` and `doca_dpa_set_app()` succeed, `doca_dpa_cap_is_supported()` returns
DOCA_SUCCESS for the SF, but `doca_dpa_start()` fails with DOCA_ERROR_DRIVER. flexio prints:

```
flexio_prm_create_process 316 - Failed to create PRM process. Status is 0x3, syndrome 0x775dd0.
```

Reproduced with the SF vhca in its own 8-EU partition, in a shared partition, in no partition,
with `trust on` and `trust off`, and as root. The same program on the PF (mlx5_0) starts fine.

### Behaviour 2: an extended context cannot run threads on the SF vhca's partition EUs

Creating the process on the PF and extending it to the SF (`doca_dpa_device_extend`) works, and
threads, completions, msgqs, mmaps and buf arrays created on the extended context run at full
speed. But a thread pinned with `doca_dpa_thread_set_affinity()` to an EU of the SF vhca's own
partition (EU 64) fails `doca_dpa_thread_start()` with DOCA_ERROR_DRIVER, exactly like an
unassigned EU (EU 100); pinned to a PF-partition EU (EU 0) it runs. So a partition assigned to
an SF vhca is never usable by that SF.

### Behaviour 3 (blocking for us): one running SF thread blocks consumer-completion CQs on every other SF

With one DPA process on the PF extended to two SFs (or two processes each extended to one SF),
once a DPA thread is *running* on the first SF, starting a
`doca_comch_consumer_completion` bound to a thread on the second SF fails:

```
[DOCA][ERR][CORE][linux_devx_obj.cpp:133] Failed to create devx object with syndrome=0x5ecb3
[DOCA][ERR][CORE][doca_cq.cpp:837] CQ ...: Failed to create CQ DevX object. Fail syndrome=388275
[DOCA][ERR][DPA][dpa_comch_msgq.cpp:137] Failed to create consumer completion WOD CQ
[DOCA][ERR][COMCH][doca_comch_msgq.cpp:941] Failed to start consumer completion context: failed to create completion context on DPA: error=DOCA_ERROR_DRIVER
```

Everything else on the second SF succeeds (extension, thread, `doca_dpa_completion`, msgqs).
Isolation results, all on this system:

| First SF state while the second SF is set up | Second SF consumer completion |
|---|---|
| full setup (started thread, completions, msgqs) | fails, 0x5ecb3 |
| no consumer completion on the first SF | fails |
| no msgqs and no consumer completion | fails |
| started thread only | fails |
| thread created but not started | succeeds |
| extension only | succeeds |
| first SF in another process, thread started / not started | fails / succeeds |
| order reversed (mlx5_3 first) | fails the same way |
| threads pinned to distinct PF EUs | fails |
| per-SF EU partitions, one shared partition, or no SF partition | fails |
| SFs re-created with trust on | fails |

Several processes can each create a DPA process on the same PF, and several processes can
extend to the *same* SF concurrently; only a second *distinct* SF is refused.

### Reproduction

Two small host programs are attached (`sf_dpa_probe.c`, `sf_ext_probe.c`, with a Makefile;
they link libdoca-sdk-dpa, libdoca-sdk-comch, libflexio and a dpacc-built DPA app whose thread
function is a plain polling loop). Each prints every DOCA call with its result:

```
./sf_dpa_probe mlx5_2                       # behaviour 1
PROBE_EU1=64 ./sf_ext_probe mlx5_0 mlx5_2   # behaviour 2 (EU 0 succeeds)
./sf_ext_probe mlx5_0 mlx5_2 mlx5_3         # behaviour 3
PROBE_SDK_LOG=debug ...                     # DOCA SDK debug log with the devx syndromes
```

Full debug logs of the three cases, `dpaeumgmt partition query` and `mlxdevm port show` output
are attached.

### Questions

1. Is a DPA process owned by an SF vhca unsupported by design on BlueField-3? What do
   `CREATE_PROCESS` status 0x3 / syndrome 0x775dd0 mean, and is there a firmware, mlxconfig or
   DOCA setting that enables it?
2. Is it expected that an extended context can only use the base PF's EU partition? If so,
   what is the intended use of assigning SF vhcas to EU partitions with `dpaeumgmt`?
3. Which resource does a running DPA thread on an extended SF hold that prevents a Comch msgq
   consumer-completion CQ on another SF (syndrome 0x5ecb3)? Is the limit per process, per PF or
   per device, and is there a way to lift it? Our target is one SF per tenant, each with its
   own DPA thread and msgq completions.
4. If per-SF DPA threads are not achievable, is "one DPA process per tenant on the PF, Comch
   on the tenant's SF" the recommended deployment, and is there a documented limit on the
   number of DPA processes per PF?
