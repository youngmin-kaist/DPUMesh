project('dpumesh', 'c', 'cpp',
  version : '1.0.0',
  license : 'BSD-3',
  default_options : ['buildtype=debugoptimized'],
  meson_version : '>=0.61.2'
)

# The DOCA transport: the only tree that includes DOCA headers. It is split by
# the side that runs it and built into static archives that every consumer
# links (the `dpumesh` binary below, the Rust proxy's `linkerd/doca/build.rs`,
# `bench/dmesh-router-cpp`, the host library's root Makefile), so this file is
# the single list of transport sources.
#
#   common/  buffers, rings, per-worker objects, Comch control-path framing,
#            DPA thread/msgq management (both sides)
#   dpu/     Comch server, forward DPA pool + DMA push engine, DPU worker
#   host/    Comch client, and the wire layer the host library builds on
#   device/  the DPA kernel (compiled by dpacc, see build_dpacc.sh)
#   legacy/  the host worker's TCP bridges and the old Go host library
#   apps/    the `dpumesh` CLI (HOST_MODE / DPU_MODE picked by DOCA_ARCH_DPU)
#            and `dpumesh-echo`, the DMA benchmark peer over the shim API

# Comment this line to restore warnings of experimental DOCA features
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language: ['c', 'cpp'])

# Every subdirectory is on the include path so sources keep their bare
# `#include "x.h"` lines; a header lives next to its .c.
inc_dirs = include_directories('/opt/mellanox/doca/include/',
                               'common', 'dpu', 'host', 'legacy', 'apps')

deps = []
deps += dependency('threads')
deps += dependency('doca-common')
deps += dependency('doca-comch')
deps += dependency('doca-dma')
deps += dependency('doca-dpa')
deps += dependency('libflexio')

# DPA management (common/dpa.c, and comch_msgq.c on top of it) references the
# dpacc host stub, so it is kept apart from the DPA-free sources for the
# shared library below and for the host library's Makefile.
common_nodpa_srcs = files(
  'common/buffer.c',
  'common/ring.c',
  'common/object.c',
  'common/common.c',
  'common/comch_common.c',
  'common/comch_consumer.c',
  'common/comch_producer.c',
)
common_srcs = common_nodpa_srcs + files('common/dpa.c', 'common/comch_msgq.c')
dpu_srcs = files(
  'dpu/comch_server.c',
  'dpu/dma.c',
  'dpu/dpu_worker.c',
)
host_srcs = files(
  'host/comch_client.c',
)
# The host library's wire layer. wire_host_stubs.c stubs the two server-side
# symbols the shared sources reference, so this archive must never be linked
# together with dmesh_dpu.
wire_srcs = files(
  'host/wire_push.c',
  'host/wire_host_stubs.c',
)

# DPA device code: dpacc produces device/dpa_kernel.a (plus the host stub that
# registers the DPA app), linked into every DPU-side binary.
program_name = 'dpa_kernel'
dpacc_build_script = meson.project_source_root() / 'build_dpacc.sh'
device_src = meson.project_source_root() / 'device' / 'dpa_kernel.c'
dpacc_mcpu_flag = 'nv-dpa-bf3'
doca_lib_dir = dependency('doca-dpa').get_variable(pkgconfig : 'libdir')

run_command(dpacc_build_script, meson.project_build_root(), meson.project_source_root(), device_src,
            program_name, dpacc_mcpu_flag, doca_lib_dir, check: true)

device_build_dir = meson.project_build_root() / 'device'
dpa_program_lib = meson.get_compiler('c').find_library(program_name, dirs: device_build_dir,
                                                       static: true)

dmesh_common = static_library('dmesh_common', common_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)
dmesh_dpu = static_library('dmesh_dpu', dpu_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)
dmesh_host = static_library('dmesh_host', host_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)
dmesh_wire = static_library('dmesh_wire', wire_srcs,
  dependencies : deps,
  include_directories : inc_dirs,
)

# The standalone data plane (DPU side) / bridge host (host side).
executable('dpumesh',
  files('apps/main.c', 'apps/config.c', 'legacy/host_worker.c'),
  link_whole : [dmesh_dpu, dmesh_common, dmesh_host],
  dependencies : deps + [dependency('doca-argp'), dpa_program_lib],
  include_directories : inc_dirs,
  install : false,
)

# The old Go host library (bench/dmeshgo): the push channel as a byte stream.
# No host DPA, so the DPA archive (non-PIC) and dpa.c stay out; the DPU-side
# symbols the shared sources reference are stubbed in host_lib.c, which is why
# this links the common sources directly rather than dmesh_dpu.
shared_library('dmesh_hostlib',
  common_nodpa_srcs + host_srcs + files('legacy/host_lib.c'),
  dependencies : deps,
  include_directories : inc_dirs,
  install : false,
)

# dpumesh-echo: the DPU-side benchmark peer (sink / echo) over the same shim the
# proxy and the C++ router drive. shim.c lives in the linkerd2-proxy submodule;
# the target is skipped when the submodule is not checked out.
fs = import('fs')
shim_c = meson.project_source_root() / '..' / '..' / 'linkerd2-proxy' / 'linkerd' / 'doca' / 'src' / 'shim.c'
if fs.exists(shim_c)
  executable('dpumesh-echo',
    files('apps/dpu_echo.c') + files(shim_c),
    link_whole : [dmesh_dpu, dmesh_common, dmesh_host],
    dependencies : deps + [dependency('doca-aes-gcm'), dependency('doca-sha'), dpa_program_lib],
    include_directories : inc_dirs,
    c_args : ['-DALLOW_EXPERIMENTAL_API', '-DFLEXIO_ALLOW_EXPERIMENTAL_API', '-Wno-unused-parameter'],
    install : false,
  )
else
  message('linkerd2-proxy submodule not checked out: skipping dpumesh-echo')
endif
