
project('dpumesh', 'c', 'CPP',
  version : '1.0.0',
  license : 'BSD-3',
  default_options : ['buildtype=debug'],
  meson_version : '>=0.61.2'
)

program_name = 'dpa_kernel'

# Comment this line to restore warnings of experimental DOCA features
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language: ['c', 'cpp'])

inc_dirs = []
inc_dirs += include_directories('/opt/mellanox/doca/include/')
inc_dirs += include_directories('../')

sample_dependencies = []
# Required for all DOCA programs
sample_dependencies += dependency('doca-common')
# The DOCA library of the sample itself
sample_dependencies += dependency('doca-comch')
# Utility DOCA library for executables
sample_dependencies += dependency('doca-argp')
# DOCA DPA library
sample_dependencies += dependency('doca-dpa')
# FlexIO definitions - Required by DPACC
sample_dependencies += dependency('libflexio')

srcs = [
    'main.c',
    'object.c',
    'config.c',
    'buffer.c',
    'common.c',
    'comch_common.c',
    'comch_server.c',
    'comch_client.c',
    'comch_consumer.c',
    'comch_producer.c',
    'comch_msgq.c',
    'dpa.c',
    'dma.c',
    'dpu_worker.c',
    'host_worker.c',
    'ring.c',
    'grpc/grpc_offload.c',
]

# DPA compiler build script
dpacc_build_script_name = 'build_dpacc.sh'
dpacc_build_script = meson.project_source_root() + '/' + dpacc_build_script_name
device_src = meson.project_source_root() + '/device/' + 'dpa_kernel.c'
dpacc_mcpu_flag = 'nv-dpa-bf3'
doca_dep = dependency('doca-dpa')
doca_lib_dir = doca_dep.get_variable(pkgconfig : 'libdir')

dpa_program_lib = custom_target('dpa_kernel_archive',
  input : device_src,
  output : program_name + '.a',
  command : [dpacc_build_script, meson.project_build_root(), meson.project_source_root(),
             '@INPUT@', program_name, dpacc_mcpu_flag, doca_lib_dir, '@OUTPUT@'],
  depend_files : [
    dpacc_build_script,
    meson.project_source_root() + '/dpa_common.h',
    meson.project_source_root() + '/grpc/grpc_wire_encode.c',
    meson.project_source_root() + '/grpc/grpc_wire_encode.h',
    meson.project_source_root() + '/grpc/proto_meta.h',
  ],
  build_by_default : true,
)

executable('dpumesh', srcs, dpa_program_lib,
  dependencies : sample_dependencies,
  include_directories : inc_dirs,
  install : false,
)
