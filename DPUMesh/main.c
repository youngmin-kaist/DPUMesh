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
        
//     result = init_comch_ctrl_path_server("DPUMesh", &objs, true);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to init comch control path server: %s", doca_error_get_descr(result));
//         cleanup_objects(&objs);
//         goto argp_cleanup;
//     }

//     result = init_comch_datapath_consumer(&objs);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed in comch datapath recv msg: %s", doca_error_get_descr(result));
//         cleanup_objects(&objs);
//         goto argp_cleanup;
//     }
    
//     result = init_comch_dpa_datapath(&objs);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to init comch DPA datapath: %s", doca_error_get_descr(result));
//         cleanup_objects(&objs);
//         goto argp_cleanup;
//     }

//     result = init_dma_resources(&objs);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to init DMA resources: %s", doca_error_get_descr(result));
//         cleanup_objects(&objs);
//         goto argp_cleanup;
//     }

//     result = send_dma_request_to_dpa(&objs);
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to send DMA request to DPA: %s", doca_error_get_descr(result));
//         cleanup_objects(&objs);
//         goto argp_cleanup;
//     }

// #if MEASURE_DPA_THROUGHPUT
//     struct comch_msg msg = {0};
//     struct dmesh_doca_dpa_msgq *msgq = &objs.dpa_comch->send;
//     result = dmesh_doca_dpa_msgq_send_bulk(msgq, CC_DPA_MAX_MSG_NUM,
//                                             &msg, sizeof(struct comch_msg));
//     if (result != DOCA_SUCCESS) {
//         DOCA_LOG_ERR("Failed to send bulk message to DPA: %s",
//                      doca_error_get_descr(result));
//         cleanup_objects(&objs);
//         goto argp_cleanup;
//     }
// #endif  /* MEASURE_DPA_THROUGHPUT */

//     /* poll PE */
//     clock_gettime(CLOCK_MONOTONIC, &last);
//     int cnt = 0;
//     while (true) {
//         if (doca_pe_progress(objs.consumer_pe) == 0)
//             nanosleep(&ts, &ts);
        
//         if (cnt++ % 10 == 0)
//             doca_pe_progress(objs.pe);  

//         clock_gettime(CLOCK_MONOTONIC, &now);
// 		elapsed = diff_sec(&last, &now);
// 		if (elapsed >= 1.0) {
//             if (objs.sent_msg_cnt > 0 || objs.recv_msg_cnt > 0)
// 			    DOCA_LOG_INFO("elapsed: %.2f, sent: %d/s, recv: %d/s", elapsed, objs.sent_msg_cnt, objs.recv_msg_cnt);
// 			objs.sent_msg_cnt = 0;
// 			objs.recv_msg_cnt = 0;
// 			last = now;
// 		}
//     }

argp_cleanup:
    clean_argp();
exit:
    return 0;
}