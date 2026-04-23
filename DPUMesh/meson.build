
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
]

# DPA compiler build script
dpacc_build_script_name = 'build_dpacc.sh'
dpacc_build_script = meson.project_source_root() + '/' + dpacc_build_script_name
device_src = meson.project_source_root() + '/device/' + 'dpa_kernel.c'
dpacc_mcpu_flag = 'nv-dpa-bf3'
doca_dep = dependency('doca-dpa')
doca_lib_dir = doca_dep.get_variable(pkgconfig : 'libdir')

result = run_command(dpacc_build_script, meson.project_build_root(), meson.project_source_root(), device_src,
        program_name, dpacc_mcpu_flag, doca_lib_dir, check: true)

device_build_dir = meson.project_build_root() + '/device'
dpa_program_lib = meson.get_compiler('c').find_library(program_name, dirs: device_build_dir,
                  static: true)

sample_dependencies += dpa_program_lib

executable('dpumesh', srcs,
  dependencies : sample_dependencies,
  include_directories : inc_dirs,
  install : false,
)
