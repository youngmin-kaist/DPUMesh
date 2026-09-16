# Verification

`make test` runs the host-only checks and the ABI contract; no device is opened.

| Test | Covers |
|---|---|
| `native_header_contract_test.py` | Public headers compile as C and C++ |
| `abi_contract_test.sh` | Exported symbols of `libdpumesh.so.5` and the preload shim match `fixtures/native_abi_lp64.txt` |
| `native_api_contract_test`, `preload_api_contract_test` | Façade argument validation without a transport |
| `native_core_transport_test` | Channel, EQ, QP, reservation, custody ACK, held RX buffers, FIN and teardown over the memory carrier in `support/` |
| `native_writable_test` | Writable-buffer accounting of the core |
| `carrier_push_logic_test` | Forward chunking to the DPUMesh copy rule and the in-order release window of the push carrier |
| `service_registry_test` | Registry validation and transactional reload |
| `topology_test`, `benchmark_result_contract_test`, `generator_selftest_test.sh` | Topology header, benchmark result format, generator self-test |

`support/native_memory_transport.c` is a deterministic loopback carrier for the
core: it implements the private carrier contract in memory, can hold custody
ACKs and is never linked into the library. The device-backed carrier
(`src/core/carrier_push.c`) is validated on hardware.
