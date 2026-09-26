dpa_program_lib = custom_target('dpa_kernel',
  input : 'dpa_kernel.c',
  output : 'dpa_kernel.a',
  depend_files : files('../common/dpa_common.h', 'hpack_walk.h', 'hpack_term.h', 'dsb_blocks.h'),
  command : [dpacc_build_script, meson.project_build_root(), meson.project_source_root(),
             '@INPUT@', program_name, dpacc_mcpu_flag, doca_lib_dir],
  build_by_default : true,
)
