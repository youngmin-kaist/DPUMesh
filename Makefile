# Host library, façades, tests and examples. The DOCA transport is
# `src/transport/` (its own Meson project, the authority on transport sources);
# the host library compiles the host-side subset listed in TRANSPORT_SRCS so it
# builds with plain make on any host that has the DOCA headers.
CC ?= cc
CXX ?= c++
PYTHON ?= python3
BUILD := build
LIBDIR := $(BUILD)/lib
TESTDIR := $(BUILD)/test
BINDIR := $(BUILD)/bin
ABI_MAJOR := 5
TRANSPORT := src/transport
DOCA_INC := /opt/mellanox/doca/include
DOCA_LIBS := $(shell pkg-config --libs doca-common doca-comch doca-dma doca-dpa)
HOST_CFLAGS := -std=gnu11 -O2 -g -Wall -Wextra -D_GNU_SOURCE -DDOCA_ALLOW_EXPERIMENTAL_API \
    -Iinclude -I. -I$(TRANSPORT)/common -I$(TRANSPORT)/dpu -I$(TRANSPORT)/host -I$(DOCA_INC)
# Transport sources the host library needs: the common set (no DPA) plus the
# Comch client and the wire layer. Keep in step with src/transport/meson.build.
TRANSPORT_SRCS := $(addprefix $(TRANSPORT)/common/,object.c buffer.c common.c comch_common.c \
    comch_consumer.c comch_producer.c ring.c) \
    $(addprefix $(TRANSPORT)/host/,comch_client.c wire_push.c wire_host_stubs.c)
LIB_SRCS := src/core/dmesh_core.c src/core/carrier_push.c src/core/service_registry.c \
    src/facade/dmesh_api.c $(TRANSPORT_SRCS)
HOST_TESTS := carrier_push_logic_test service_registry_test native_writable_test native_core_transport_test \
    topology_test native_api_contract_test preload_api_contract_test
EXAMPLES := hello_dpumesh hello_dpumesh_server tcp_echo tcp_client
BENCH_APPS := dma_bench

.PHONY: all lib test test-native-headers test-abi examples bench clean
all: lib

lib: $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so

$(LIBDIR) $(TESTDIR) $(BINDIR):
	mkdir -p $@

$(LIBDIR)/libdpumesh.so.$(ABI_MAJOR): $(LIB_SRCS) include/dpumesh/*.h src/core/*.h $(TRANSPORT)/host/wire_push.h | $(LIBDIR)
	$(CC) $(HOST_CFLAGS) -fPIC -shared -Wl,-soname,libdpumesh.so.$(ABI_MAJOR) -Wl,--no-undefined \
	    $(LIB_SRCS) -pthread $(DOCA_LIBS) -o $@
	ln -sfn libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh.so

$(LIBDIR)/libdpumesh_preload.so: src/facade/dmesh_preload.c $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR)
	$(CC) $(HOST_CFLAGS) -U_FILE_OFFSET_BITS -fPIC -shared $< -L$(LIBDIR) -ldpumesh -ldl -pthread -o $@

test: test-native-headers $(addprefix $(TESTDIR)/,$(HOST_TESTS)) test-abi
	@set -e; for test in $(HOST_TESTS); do $(TESTDIR)/$$test; done

test-native-headers:
	CC="$(CC)" CXX="$(CXX)" $(PYTHON) tests/native_header_contract_test.py

test-abi: lib
	sh tests/abi_contract_test.sh $(LIBDIR)/libdpumesh.so.$(ABI_MAJOR) $(LIBDIR)/libdpumesh_preload.so $(ABI_MAJOR)

$(TESTDIR)/carrier_push_logic_test: tests/carrier_push_logic_test.c src/core/carrier_push_logic.h $(TRANSPORT)/host/wire_push.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/topology_test: tests/topology_test.c include/dpumesh/dmesh_topology.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $< -o $@

$(TESTDIR)/native_api_contract_test: tests/native_api_contract_test.c src/facade/dmesh_api.c src/core/dmesh_core.h include/dpumesh/dmesh.h include/dpumesh/dmesh_common.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections -Wl,--gc-sections tests/native_api_contract_test.c src/facade/dmesh_api.c -o $@

$(TESTDIR)/preload_api_contract_test: tests/preload_api_contract_test.c src/facade/dmesh_preload.c src/core/dmesh_core.h include/dpumesh/dmesh.h include/dpumesh/dmesh_common.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections -Wl,--gc-sections $< -ldl -lpthread -o $@

$(TESTDIR)/native_writable_test: tests/native_writable_test.c src/core/dmesh_core.c src/core/native_transport.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) -ffunction-sections -fdata-sections $< -Wl,--gc-sections -pthread -o $@

$(TESTDIR)/native_core_transport_test: tests/native_core_transport_test.c tests/support/native_memory_transport.c src/core/dmesh_core.c src/facade/dmesh_api.c src/core/native_transport.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $(filter %.c,$^) -pthread -o $@

$(TESTDIR)/service_registry_test: tests/service_registry_test.c src/core/service_registry.c src/core/service_registry.h | $(TESTDIR)
	$(CC) $(HOST_CFLAGS) $(filter %.c,$^) -o $@

examples: lib $(addprefix $(BINDIR)/,$(EXAMPLES))

$(BINDIR)/hello_dpumesh $(BINDIR)/hello_dpumesh_server: $(BINDIR)/%: examples/native/%.c | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -L$(LIBDIR) -ldpumesh -Wl,-rpath,$(abspath $(LIBDIR)) -lpthread -o $@

$(BINDIR)/tcp_echo $(BINDIR)/tcp_client: $(BINDIR)/%: examples/preload/%.c | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -lpthread -o $@

# Benchmarks over the host library (bench/apps); the DPU peer is dpumesh-echo.
bench: lib $(addprefix $(BINDIR)/,$(BENCH_APPS))

$(BINDIR)/dma_bench: bench/apps/dma_bench.c include/dpumesh/*.h | $(BINDIR)
	$(CC) $(HOST_CFLAGS) $< -L$(LIBDIR) -ldpumesh -Wl,-rpath,$(abspath $(LIBDIR)) -lpthread -o $@

clean:
	rm -rf $(BUILD)
