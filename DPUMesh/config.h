#ifndef CONFIG_H_
#define CONFIG_H_

#include <doca_dev.h>

enum program_mode {
    HOST_MODE = 1,
    DPU_MODE = 2,
};

struct global_config {
    enum program_mode mode;
    char dev_pci_addr[DOCA_DEVINFO_PCI_ADDR_SIZE];  /* DOCA device PCI address */
    char dev_rep_pci_addr[DOCA_DEVINFO_REP_PCI_ADDR_SIZE];  /* DOCA device representor PCI address */

};

/*
 * Register the command line parameters
 *
 * @return: DOCA_SUCCESS on success and DOCA_ERROR otherwise
 */
doca_error_t 
init_argp(const char *program_name, void *config, int argc, char **argv);

void
clean_argp(void);
#endif // CONFIG_H_