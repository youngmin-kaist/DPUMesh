# Host-side DPA probes (run on the host, e.g. jet1):
#   sf_dpa_probe <ibdev>        create + start a DPA process directly on that device
#   sf_ext_probe <pf> <sf>      create on the PF, doca_dpa_device_extend to the SF, run a thread
#
# Needs the transport archives (cd src/transport && meson setup build && ninja -C build)
# and the host-compiled DPA kernel archive (make lib at the repo root builds it).
#   make            build both probes into ./bin
#   make TRANSPORT_BUILD=... DPA_KERNEL=...   override the archive locations

ROOT       := ../..
TRANSPORT  := $(ROOT)/src/transport
TRANSPORT_BUILD := $(TRANSPORT)/build
DPA_KERNEL := $(ROOT)/build/dpa/device/dpa_kernel.a
DOCA_INC   := /opt/mellanox/doca/include
DOCA_LIBS  := $(shell pkg-config --libs doca-common doca-comch doca-dma doca-dpa libflexio)

CC      ?= gcc
CFLAGS  := -std=gnu11 -O1 -g -Wall -D_GNU_SOURCE -DDOCA_ALLOW_EXPERIMENTAL_API \
           -I$(TRANSPORT)/common -I$(TRANSPORT)/dpu -I$(TRANSPORT)/host -I$(DOCA_INC)
ARCHIVES := -Wl,--whole-archive $(TRANSPORT_BUILD)/libdmesh_common.a -Wl,--no-whole-archive \
            $(TRANSPORT_BUILD)/libdmesh_host.a $(TRANSPORT_BUILD)/libdmesh_dpu.a $(DPA_KERNEL)
LDLIBS  := $(DOCA_LIBS) -lpthread

BIN    := bin
PROBES := $(BIN)/sf_dpa_probe $(BIN)/sf_ext_probe

all: $(PROBES)

$(BIN)/%: %.c | $(BIN) archives
	$(CC) $(CFLAGS) $< -o $@ $(ARCHIVES) $(LDLIBS)

$(BIN):
	mkdir -p $@

archives:
	@test -f $(TRANSPORT_BUILD)/libdmesh_common.a || { echo "$(TRANSPORT_BUILD)/libdmesh_common.a missing: build src/transport first"; exit 1; }
	@test -f $(DPA_KERNEL) || { echo "$(DPA_KERNEL) missing: run 'make lib' at the repo root"; exit 1; }

clean:
	rm -rf $(BIN)

.PHONY: all clean archives
