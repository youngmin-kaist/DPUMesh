# Host library, façades, tests and benchmarks. The DPU side is Youngmin's
# DPUMesh/ tree, built with its own Meson project (see `make dpu-help`).
CC ?= cc
CXX ?= c++
PYTHON ?= python3
BUILD := build
LIBDIR := $(BUILD)/lib
TESTDIR := $(BUILD)/test
BINDIR := $(BUILD)/bin
ABI_MAJOR := 5
DOCA_INC := /opt/mellanox/doca/include
DOCA_LIBS := $(shell pkg-config --libs doca-common doca-comch doca-dma doca-dpa)
HOST_CFLAGS := -std=gnu11 -O2 -g -Wall -Wextra -D_GNU_SOURCE -DDOCA_ALLOW_EXPERIMENTAL_API \
    -Iinclude -I. -IDPUMesh -I$(DOCA_INC)
# Host-side DPUMesh sources, used unmodified.
WIRE_SRCS := $(addprefix DPUMesh/,object.c buffer.c common.c comch_common.c comch_client.c \
    comch_consumer.c comch_producer.c ring.c)
LIB_SRCS := src/core/dmesh_core.c src/core/carrier_push.c src/core/wire_push.c \
    src/core/wire_host_stubs.c src/core/service_registry.c src/facade/dmesh_api.c $(WIRE_SRCS)
HOST_TESTS := carrier_push_logic_test service_registry_test native_writable_test native_core_transport_test topology_test \
    native_api_contract_test preload_api_contract_test benchmark_result_contract_test
POSIX_BINS := bench_sock echo_sock http1_bench http1_echo tcp_client tcp_echo preload_runner
NATIVE_BINS := bench_dpumesh echo_dpumesh

.PHONY: all lib test test-hostfree test-native-headers test-generator test-abi bench examples dpu-help clean
all: lib bench

lib: $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so

$(LIBDIR) $(TESTDIR) $(BINDIR):
	mkdir -p $@

$(LIBDIR)/libdpumesh.so.$(ABI_MAJOR): $(LIB_SRCS) include/dpumesh/*.h src/core/*.h | $(LIBDIR)
	$(CC) $(HOST_CFLAGS) -fPIC -shared -Wl,-soname,libdpumesh.so.$(ABI_MAJOR) -Wl,--no-undefined \
	    $(LIB_SRCS) -pthread $(DOCA_LIBS) -o $@
	ln -sfn libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh.so

$(LIBDIR)/libdpumesh_preload.so: src/facade/dmesh_preload.c $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR)
	$(CC) $(HOST_CFLAGS) -U_FILE_OFFSET_BITS -fPIC -shared $< -L$(LIBDIR) -ldpumesh -ldl -pthread -o $@

test: test-hostfree test-abi

test-hostfree: test-native-headers $(addprefix $(TESTDIR)/,$(HOST_TESTS)) test-generator
	@set -e; for test in $(HOST_TESTS); do $(TESTDIR)/$$test; done

test-native-headers:
	CC="$(CC)" CXX="$(CXX)" $(PYTHON) tests/native_header_contract_test.py

test-abi: lib
	sh tests/abi_contract_test.sh $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so $(ABI_MAJOR)

$(TESTDIR)/carrier_push_logic_test: tests/carrier_push_logic_test.c src/core/carrier_push_logic.h src/core/wire_push.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/topology_test: tests/topology_test.c include/dpumesh/dmesh_topology.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/native_api_contract_test: tests/native_api_contract_test.c src/facade/dmesh_api.c src/core/dmesh_core.h include/dpumesh/dmesh.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections -Wl,--gc-sections tests/native_api_contract_test.c src/facade/dmesh_api.c -o $@

$(TESTDIR)/preload_api_contract_test: tests/preload_api_contract_test.c src/facade/dmesh_preload.c src/core/dmesh_core.h include/dpumesh/dmesh.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections -Wl,--gc-sections $< -ldl -lpthread -o $@

$(TESTDIR)/benchmark_result_contract_test: tests/benchmark_result_contract_test.c bench/apps/bench_result.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/native_writable_test: tests/native_writable_test.c src/core/dmesh_core.c src/core/native_transport.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/native_core_transport_test: tests/native_core_transport_test.c tests/support/native_memory_transport.c src/core/dmesh_core.c src/facade/dmesh_api.c src/core/native_transport.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $(filter %.c,$^) -pthread -o $@

$(TESTDIR)/service_registry_test: tests/service_registry_test.c src/core/service_registry.c src/core/service_registry.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $(filter %.c,$^) -o $@

test-generator: $(BINDIR)/bench_sock
	sh tests/generator_selftest_test.sh $(BINDIR)/bench_sock

bench: $(addprefix $(BINDIR)/,$(POSIX_BINS))

$(BINDIR)/%: bench/apps/%.c bench/apps/bench.h bench/apps/bench_selftest.h bench/apps/bench_result.h | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -lm -lpthread -o $@

$(BINDIR)/%: bench/validators/%.c | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -ldl -lpthread -o $@

bench-native: lib $(addprefix $(BINDIR)/,$(NATIVE_BINS))

$(BINDIR)/bench_dpumesh $(BINDIR)/echo_dpumesh: $(BINDIR)/%: bench/apps/%.c bench/apps/bench.h | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -L$(LIBDIR) -ldpumesh -Wl,-rpath,$(abspath $(LIBDIR)) -lm -lpthread -o $@

examples: lib $(BINDIR)/hello_dpumesh $(BINDIR)/hello_dpumesh_server

$(BINDIR)/hello_dpumesh $(BINDIR)/hello_dpumesh_server: $(BINDIR)/%: bench/examples/%.c | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -L$(LIBDIR) -ldpumesh -Wl,-rpath,$(abspath $(LIBDIR)) -lpthread -o $@

dpu-help:
	@echo "DPU side (Youngmin's tree): cd DPUMesh && meson setup build && meson compile -C build"
	@echo "Proxy: see linkerd2-proxy (pinned submodule)."

clean:
	rm -rf $(BUILD)
