#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include <doca_comch.h>
#include <doca_dev.h>
#include <doca_log.h>
#include <doca_argp.h>
#include <doca_build_config.h>

#include "config.h"
#include "common.h"
#include "object.h"
#include "comch_consumer.h"
#include "dpa_common.h"
#include "dpa.h"
#include "dma.h"
#include "dpu_worker.h"
#include "host_worker.h"

DOCA_LOG_REGISTER(MAIN);

#define SLEEP_IN_NANOS (10 * 1000)	       /* Sample tasks every 10 microseconds */

double diff_sec(const struct timespec *start, const struct timespec *end)
{
    time_t sec  = end->tv_sec  - start->tv_sec;
    long   nsec = end->tv_nsec - start->tv_nsec;

    if (nsec < 0) {
        sec -= 1;
        nsec += 1000000000L;
    }

    return (double)sec + (double)nsec * 1e-9;
}

int main(int argc, char **argv)
{
    struct objects objs = {0};
    struct global_config gcfg = {0};

    doca_error_t result;
    struct doca_log_backend *sdk_log;

    /* register a logger backend */
    result = doca_log_backend_create_standard();
    if (result != DOCA_SUCCESS) {
        goto exit;
    }

    /* register a logger backend for internal SDK errors and warnings */
    result = doca_log_backend_create_with_file_sdk(stderr, &sdk_log);
    if (result != DOCA_SUCCESS) {
        goto exit;
    }
    result = doca_log_backend_set_sdk_level(sdk_log, DOCA_LOG_LEVEL_WARNING);
    if (result != DOCA_SUCCESS) {
        goto exit;
    }
    
#ifdef DOCA_ARCH_DPU
    gcfg.mode = DPU_MODE;
#else
    gcfg.mode = HOST_MODE;
#endif

    /* parse cmdline arguments */
    result = init_argp(NULL, &gcfg, argc, argv);
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to parse arguments: %s", doca_error_get_descr(result));
        goto exit;
    }

    /* Open DOCA device according to the given PCI address */
    result = open_doca_device_with_pci(gcfg.dev_pci_addr, NULL, &(objs.dev));
    if (result != DOCA_SUCCESS) {
        DOCA_LOG_ERR("Failed to open DOCA device based on PCI address");
        goto argp_cleanup;
    }

    /* open DOCA representor device */
    if (gcfg.mode == DPU_MODE) {
        result = open_doca_device_rep_with_pci(objs.dev,
                                            DOCA_DEVINFO_REP_FILTER_NET,
                                            gcfg.dev_rep_pci_addr,
                                            &(objs.rep_dev));
        if (result != DOCA_SUCCESS) {
            DOCA_LOG_ERR("Failed to open DOCA device representor based on PCI address");
            cleanup_objects(&objs);
            goto argp_cleanup;
        }
    }
    DOCA_LOG_INFO("Start %s application.", gcfg.mode == DPU_MODE ? "DPU" : "Host");

    if (gcfg.mode == HOST_MODE) {
        run_host_worker(&objs);
    } else {
        run_dpu_worker(&objs);
    }

argp_cleanup:
    clean_argp();
exit:
    return 0;
}