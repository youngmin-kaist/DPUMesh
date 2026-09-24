/* Can a host PF's DPA process be extended to a host SF, and what can the
 * extended context create on the SF? (doca_dpa_device_extend) */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <doca_dev.h>
#include <doca_dpa.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_buf_array.h>
#include <doca_comch_msgq.h>
#include <doca_comch_consumer.h>
#include <doca_error.h>
#include "common.h"
#include "dpa_common.h"
extern struct doca_dpa_app *DPU_mesh_dpa_app;
extern doca_dpa_func_t run_dma_manager;
#define STEP(x) do { doca_error_t _r = (x); printf("%-62s %s\n", #x, doca_error_get_name(_r)); if (_r != DOCA_SUCCESS) { fails++; } } while (0)
int main(int argc, char **argv)
{
    const char *pf = argc > 1 ? argv[1] : "mlx5_0", *sf = argc > 2 ? argv[2] : "mlx5_2";
    struct doca_dev *pfdev = NULL, *sfdev = NULL;
    struct doca_dpa *base = NULL, *ext = NULL;
    struct doca_dpa_thread *th = NULL;
    struct doca_dpa_completion *comp = NULL;
    struct doca_mmap *mm = NULL; struct doca_buf_arr *ba = NULL;
    struct doca_comch_msgq *q = NULL;
    doca_dpa_dev_uintptr_t mem = 0; doca_dpa_dev_mmap_t mh = 0; doca_dpa_dev_buf_arr_t bh = 0;
    void *buf = NULL; uint16_t vhca = 0; int fails = 0;
    doca_log_backend_create_standard();
    struct doca_log_backend *sdk = NULL;
    if (doca_log_backend_create_with_file_sdk(stderr, &sdk) == DOCA_SUCCESS) doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_WARNING);

    STEP(open_doca_device_with_ibdev_name((const uint8_t *)pf, strlen(pf), NULL, &pfdev));
    STEP(open_doca_device_with_ibdev_name((const uint8_t *)sf, strlen(sf), NULL, &sfdev));
    doca_devinfo_get_vhca_id(doca_dev_as_devinfo(sfdev), &vhca);
    printf("base on %s, extending to %s (vhca %u)\n", pf, sf, vhca);
    STEP(doca_dpa_create(pfdev, &base));
    STEP(doca_dpa_set_app(base, DPU_mesh_dpa_app));
    STEP(doca_dpa_start(base));
    STEP(doca_dpa_device_extend(base, sfdev, &ext));
    if (!ext) { printf("no extended context, stopping\n"); return 1; }
    printf("== objects on the extended (SF) context\n");
    STEP(doca_dpa_mem_alloc(ext, 4096, &mem));
    STEP(doca_dpa_thread_create(ext, &th));
    if (th) STEP(doca_dpa_thread_set_func_arg(th, run_dma_manager, mem));
    if (th) STEP(doca_dpa_thread_start(th));
    STEP(doca_dpa_completion_create(ext, 64, &comp));
    if (comp && th) STEP(doca_dpa_completion_set_thread(comp, th));
    if (comp) STEP(doca_dpa_completion_start(comp));
    /* SF-registered host memory reachable from the DPA */
    buf = aligned_alloc(4096, 1 << 20); memset(buf, 0, 1 << 20);
    STEP(doca_mmap_create(&mm));
    STEP(doca_mmap_add_dev(mm, sfdev));
    STEP(doca_mmap_set_permissions(mm, DOCA_ACCESS_FLAG_LOCAL_READ_WRITE | DOCA_ACCESS_FLAG_PCI_READ_WRITE));
    STEP(doca_mmap_set_memrange(mm, buf, 1 << 20));
    STEP(doca_mmap_start(mm));
    STEP(doca_mmap_dev_get_dpa_handle(mm, sfdev, &mh));
    printf("  dpa mmap handle on SF: %lx\n", (unsigned long)mh);
    STEP(doca_buf_arr_create(16, &ba));
    if (ba) STEP(doca_buf_arr_set_target_dpa(ba, ext));
    if (ba) STEP(doca_buf_arr_set_params(ba, mm, 64, 0));
    if (ba) STEP(doca_buf_arr_start(ba));
    if (ba) STEP(doca_buf_arr_get_dpa_handle(ba, &bh));
    /* the DPA <-> CPU msgq on the SF device (what completions ride on) */
    /* one msgq per direction, as dpa.c does (a msgq takes one DPA side) */
    struct doca_comch_msgq *q2 = NULL;
    STEP(doca_comch_msgq_create(sfdev, &q));
    if (q) STEP(doca_comch_msgq_set_max_num_consumers(q, 1));
    if (q) STEP(doca_comch_msgq_set_max_num_producers(q, 1));
    if (q) STEP(doca_comch_msgq_set_dpa_consumer(q, ext));
    if (q) STEP(doca_comch_msgq_start(q));
    STEP(doca_comch_msgq_create(sfdev, &q2));
    if (q2) STEP(doca_comch_msgq_set_max_num_consumers(q2, 1));
    if (q2) STEP(doca_comch_msgq_set_max_num_producers(q2, 1));
    if (q2) STEP(doca_comch_msgq_set_dpa_producer(q2, ext));
    if (q2) STEP(doca_comch_msgq_start(q2));
    /* the comch consumer completion (what a msgq consumer on the DPA needs):
     * bound to the thread on the extended context, then to a base-context thread */
    {
        struct doca_comch_consumer_completion *cc = NULL;
        struct doca_dpa_thread *bth = NULL; doca_dpa_dev_uintptr_t bmem = 0;
        doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_DEBUG);
        STEP(doca_comch_consumer_completion_create(&cc));
        if (cc) STEP(doca_comch_consumer_completion_set_max_num_recv(cc, 64));
        if (cc) STEP(doca_comch_consumer_completion_set_imm_data_len(cc, 16));
        if (cc && th) STEP(doca_comch_consumer_completion_set_dpa_thread(cc, th));
        if (cc) STEP(doca_comch_consumer_completion_start(cc));          /* ext thread */
        if (cc) { doca_comch_consumer_completion_stop(cc); doca_comch_consumer_completion_destroy(cc); cc = NULL; }
        /* the wire's exact parameters: 512 entries, sizeof(struct comch_msg) imm */
        {
            uint32_t cap_pf = 0, cap_sf = 0;
            doca_comch_consumer_cap_get_max_imm_data_len(doca_dev_as_devinfo(pfdev), &cap_pf);
            doca_comch_consumer_cap_get_max_imm_data_len(doca_dev_as_devinfo(sfdev), &cap_sf);
            printf("  sizeof(struct comch_msg)=%zu, max imm: PF %u, SF %u\n", sizeof(struct comch_msg), cap_pf, cap_sf);
        }
        for (int n = 64; n <= 512; n *= 2) {
            STEP(doca_comch_consumer_completion_create(&cc));
            if (cc) STEP(doca_comch_consumer_completion_set_max_num_recv(cc, n));
            if (cc) STEP(doca_comch_consumer_completion_set_imm_data_len(cc, sizeof(struct comch_msg)));
            if (cc && th) STEP(doca_comch_consumer_completion_set_dpa_thread(cc, th));
            printf("  max_num_recv=%d:\n", n);
            if (cc) STEP(doca_comch_consumer_completion_start(cc));
            if (cc) { doca_comch_consumer_completion_stop(cc); doca_comch_consumer_completion_destroy(cc); cc = NULL; }
        }
        STEP(doca_dpa_mem_alloc(base, 4096, &bmem));
        STEP(doca_dpa_thread_create(base, &bth));
        if (bth) STEP(doca_dpa_thread_set_func_arg(bth, run_dma_manager, bmem));
        if (bth) STEP(doca_dpa_thread_start(bth));
        STEP(doca_comch_consumer_completion_create(&cc));
        if (cc) STEP(doca_comch_consumer_completion_set_max_num_recv(cc, 64));
        if (cc) STEP(doca_comch_consumer_completion_set_imm_data_len(cc, 16));
        if (cc && bth) STEP(doca_comch_consumer_completion_set_dpa_thread(cc, bth));
        if (cc) STEP(doca_comch_consumer_completion_start(cc));          /* base thread */
        if (cc) { doca_comch_consumer_completion_stop(cc); doca_comch_consumer_completion_destroy(cc); }
        if (bth) doca_dpa_thread_destroy(bth);
        doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_WARNING);
    }
    /* ordering: a fresh ext thread with the consumer completion FIRST (no
     * doca_dpa_completion attached yet), as the wire does it */
    {
        struct doca_dpa_thread *t2 = NULL; doca_dpa_dev_uintptr_t m2 = 0;
        struct doca_comch_consumer_completion *cc2 = NULL; struct doca_dpa_completion *pc2 = NULL;
        doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_DEBUG);
        printf("== fresh ext thread, consumer completion first\n");
        STEP(doca_dpa_mem_alloc(ext, 4096, &m2));
        STEP(doca_dpa_thread_create(ext, &t2));
        if (t2) STEP(doca_dpa_thread_set_func_arg(t2, run_dma_manager, m2));
        if (t2) STEP(doca_dpa_thread_start(t2));
        STEP(doca_comch_consumer_completion_create(&cc2));
        if (cc2) STEP(doca_comch_consumer_completion_set_max_num_recv(cc2, 512));
        if (cc2) STEP(doca_comch_consumer_completion_set_imm_data_len(cc2, sizeof(struct comch_msg)));
        if (cc2 && t2) STEP(doca_comch_consumer_completion_set_dpa_thread(cc2, t2));
        if (cc2) STEP(doca_comch_consumer_completion_start(cc2));
        STEP(doca_dpa_completion_create(ext, 512, &pc2));
        if (pc2 && t2) STEP(doca_dpa_completion_set_thread(pc2, t2));
        if (pc2) STEP(doca_dpa_completion_start(pc2));
        doca_log_backend_set_sdk_level(sdk, DOCA_LOG_LEVEL_WARNING);
        if (pc2) { doca_dpa_completion_stop(pc2); doca_dpa_completion_destroy(pc2); }
        if (cc2) { doca_comch_consumer_completion_stop(cc2); doca_comch_consumer_completion_destroy(cc2); }
        if (t2) doca_dpa_thread_destroy(t2);
    }
    if (getenv("HOLD")) { printf("holding %s s\n", getenv("HOLD")); fflush(stdout); sleep(atoi(getenv("HOLD"))); }
    printf("== teardown\n");
    if (q2) { doca_comch_msgq_stop(q2); doca_comch_msgq_destroy(q2); }
    if (q) { doca_comch_msgq_stop(q); doca_comch_msgq_destroy(q); }
    if (ba) doca_buf_arr_destroy(ba);
    if (mm) { doca_mmap_stop(mm); doca_mmap_destroy(mm); }
    if (comp) { doca_dpa_completion_stop(comp); doca_dpa_completion_destroy(comp); }
    if (th) doca_dpa_thread_destroy(th);
    if (mem) doca_dpa_mem_free(ext, mem);
    STEP(doca_dpa_destroy(ext));
    STEP(doca_dpa_destroy(base));
    printf("RESULT: %d failing step(s)\n", fails);
    return fails != 0;
}
