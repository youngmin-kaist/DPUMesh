/* dpumesh-echo: the DPU-side DMA benchmark peer over the shim API.
 *
 * The port of dpu_worker.c's benchmark role onto the API the proxy and the C++
 * router drive (linkerd/doca/src/shim.c + the comch_server.c state machine):
 * one comch server, the shared DPA pool / consumer PE / DMA engine, then per
 * connection the zero-copy staging reader, the rx watermark and the push
 * sender. Two modes:
 *
 *   DMESH_ECHO_MODE=sink   count every forward DMA completion (host -> DPU)
 *   DMESH_ECHO_MODE=echo   ...and push the same bytes back (DPU -> host)
 *
 * Once a second it reports the same aggregate the legacy worker printed:
 * forward DMA completions per second (recv), their Gbps, reverse batches per
 * second (sent), dma_pending and dma_dropped. DMESH_BENCH_EXIT_OPS ends the
 * process after that many completions, like the legacy worker.
 *
 * Environment: DMESH_DEV_PCI (03:00.1), DMESH_REP_PCI (0b:00.1),
 * DMESH_SERVER_NAME (DPUMesh0), DMESH_BUSY_POLL (1 = poll the progress engines
 * instead of sleeping in epoll). */
#define _GNU_SOURCE
#include <errno.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

#include <doca_error.h>

#include "comch_server.h" /* dmesh_doca_ctrl_*, enum dmesh_doca_init_state */

/* shim.c ships without a header; these match its exported entry points. */
struct objects;
int32_t dmesh_doca_init(const char *dev_pci_addr, const char *rep_pci_addr, const char *server_name,
                        struct objects **handle);
int32_t dmesh_doca_data_get_fd(struct objects *objs, int *out_fd);
int32_t dmesh_doca_data_arm(struct objects *objs);
int32_t dmesh_doca_data_clear_and_drain(struct objects *objs, int fd, int budget, int *out_drained);
int32_t dmesh_doca_max_conns(void);
int32_t dmesh_doca_conn_state_get(struct objects *objs, int32_t slot);
int32_t dmesh_doca_conn_mode_get(struct objects *objs, int32_t slot);
int32_t dmesh_doca_conn_staging_base(struct objects *objs, int32_t slot, const uint8_t **out_base,
                                     size_t *out_len);
int32_t dmesh_doca_conn_recv_pop(struct objects *objs, int32_t slot, uint32_t *out_pos,
                                 uint32_t *out_len);
int32_t dmesh_doca_conn_tx_staging(struct objects *objs, int32_t slot, uintptr_t *out_base,
                                   size_t *out_len);
int32_t dmesh_doca_conn_rx_watermark(struct objects *objs, int32_t slot, uint32_t pos);
int32_t dmesh_doca_conn_send_staged(struct objects *objs, int32_t slot, uint32_t pos, uint32_t len);
void dmesh_doca_stats_get(struct objects *objs, int64_t *sent, int64_t *recv, int64_t *recv_bytes,
                          int64_t *dma_pending, int64_t *dma_dropped);

/* Mirrors enum dmesh_conn_state (object.h). */
enum { CONN_FREE = 0, CONN_RUNNING = 3, CONN_ERROR = 4, CONN_CLOSING = 6 };

#define DATA_DRAIN_BUDGET 8192
#define MAX_SLOTS 64
#define SEG_MAX 8192u /* one forward DMA completion covers at most this */

/* Write cursor over a connection's mapped tx_staging: bytes are copied once and
 * published by run; cumulative cursors, physical offset = cursor % len. */
struct tx_ring {
    uint8_t *base;
    size_t len;
    uint64_t write, publish;
};

static size_t tx_room(const struct tx_ring *t) { return t->len ? t->len - (size_t)(t->write - t->publish) : 0; }

static size_t tx_push(struct tx_ring *t, const uint8_t *data, size_t n)
{
    size_t room = tx_room(t), off, first;
    if (n > room) n = room;
    if (n == 0) return 0;
    off = (size_t)(t->write % t->len);
    first = n < t->len - off ? n : t->len - off;
    memcpy(t->base + off, data, first);
    if (first < n) memcpy(t->base, data + first, n - first);
    t->write += n;
    return n;
}

static int tx_next_run(const struct tx_ring *t, uint32_t *pos, uint32_t *len)
{
    size_t off, pending, run;
    if (!t->base || t->write == t->publish) return 0;
    off = (size_t)(t->publish % t->len);
    pending = (size_t)(t->write - t->publish);
    run = pending < t->len - off ? pending : t->len - off;
    *pos = (uint32_t)off;
    *len = (uint32_t)run;
    return 1;
}

struct slot {
    int32_t state;
    const uint8_t *rx_base;
    size_t rx_len;
    uint32_t rx_wm;
    int rx_wm_dirty;
    struct tx_ring tx;
    uint64_t segs, bytes;             /* forward completions consumed by this slot */
    uint64_t pushes, push_bytes;      /* reverse batches published (echo) */
};

static volatile sig_atomic_t g_stop;
static void on_signal(int s) { (void)s; g_stop = 1; }

static double now_sec(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + (double)ts.tv_nsec / 1e9;
}

static const char *env_or(const char *name, const char *fallback)
{
    const char *v = getenv(name);
    return v && *v ? v : fallback;
}

/* Publish everything staged on the slot; the push engine accepts at most one
 * <= 8 KiB batch at a time, so a short return means "retry next tick". */
static void slot_publish(struct objects *objs, int idx, struct slot *s)
{
    uint32_t pos, len;
    while (tx_next_run(&s->tx, &pos, &len)) {
        int32_t rv = dmesh_doca_conn_send_staged(objs, idx, pos, len);
        if (rv <= 0) break;
        s->tx.publish += (uint32_t)rv;
        s->pushes++;
        s->push_bytes += (uint32_t)rv;
        if ((uint32_t)rv < len) break;
    }
}

static void slot_wire(struct objects *objs, int idx, struct slot *s)
{
    if (!s->rx_base) {
        const uint8_t *base = NULL;
        size_t len = 0;
        if (dmesh_doca_conn_staging_base(objs, idx, &base, &len) == DOCA_SUCCESS && base) {
            s->rx_base = base;
            s->rx_len = len;
        }
    }
    if (!s->tx.base) {
        uintptr_t base = 0;
        size_t len = 0;
        if (dmesh_doca_conn_tx_staging(objs, idx, &base, &len) == 0 && base && len) {
            s->tx.base = (uint8_t *)base;
            s->tx.len = len;
        }
    }
}

/* Consume completed forward segments. In echo mode a segment is popped only
 * when tx_staging can take it whole, so nothing is ever dropped: the DPA gate
 * (rx watermark) then holds the host's forward ring, which is the
 * backpressure the proxy path exercises too. */
static void slot_pump(struct objects *objs, int idx, struct slot *s, int echo)
{
    uint32_t pos, len;
    if (!s->rx_base) return;
    for (;;) {
        if (echo) {
            slot_publish(objs, idx, s);
            if (!s->tx.base || tx_room(&s->tx) < SEG_MAX) break;
        }
        if (dmesh_doca_conn_recv_pop(objs, idx, &pos, &len) != DOCA_SUCCESS) break;
        if ((size_t)pos + len > s->rx_len) {
            fprintf(stderr, "slot %d: recv segment out of range pos=%u len=%u\n", idx, pos, len);
            continue;
        }
        s->segs++;
        s->bytes += len;
        if (echo) tx_push(&s->tx, s->rx_base + pos, len);
        s->rx_wm = pos + len;
        s->rx_wm_dirty = 1;
    }
    if (s->rx_wm_dirty) {
        s->rx_wm_dirty = 0;
        dmesh_doca_conn_rx_watermark(objs, idx, s->rx_wm);
    }
    if (echo) slot_publish(objs, idx, s);
}

int main(void)
{
    const char *dev_pci = env_or("DMESH_DEV_PCI", "03:00.1");
    const char *rep_pci = env_or("DMESH_REP_PCI", "0b:00.1");
    const char *server = env_or("DMESH_SERVER_NAME", "DPUMesh0");
    const int echo = strcmp(env_or("DMESH_ECHO_MODE", "echo"), "sink") != 0;
    const int busy = atoi(env_or("DMESH_BUSY_POLL", "0")) != 0;
    const long exit_ops = atol(env_or("DMESH_BENCH_EXIT_OPS", "0"));
    struct objects *objs = NULL;
    enum dmesh_doca_init_state state = DMESH_DOCA_STATE_SERVER_STARTED;
    struct slot slots[MAX_SLOTS];
    int max_conns, ctrl_fd = -1, data_fd = -1, epfd = -1;
    int64_t prev[5] = {0}, cur[5];
    double last = now_sec(), t0 = 0;
    int64_t recv0 = 0, bytes0 = 0;
    uint64_t pushes = 0, push_bytes = 0, prev_pushes = 0, prev_push_bytes = 0, pushes0 = 0;

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    memset(slots, 0, sizeof(slots));

    if (dmesh_doca_init(dev_pci, rep_pci, server, &objs) != DOCA_SUCCESS || !objs) {
        fprintf(stderr, "dmesh_doca_init failed (dev=%s rep=%s server=%s)\n", dev_pci, rep_pci, server);
        return 1;
    }
    if (dmesh_doca_ctrl_advance(objs, &state) != DOCA_SUCCESS || state != DMESH_DOCA_STATE_RUNNING) {
        fprintf(stderr, "infrastructure did not reach RUNNING\n");
        return 1;
    }
    if (dmesh_doca_ctrl_get_fd(objs, &ctrl_fd) != DOCA_SUCCESS ||
        dmesh_doca_data_get_fd(objs, &data_fd) != DOCA_SUCCESS) {
        fprintf(stderr, "failed to get progress-engine fds\n");
        return 1;
    }
    if (!busy) {
        struct epoll_event ev = { .events = EPOLLIN };
        epfd = epoll_create1(0);
        ev.data.fd = ctrl_fd;
        if (epfd < 0 || epoll_ctl(epfd, EPOLL_CTL_ADD, ctrl_fd, &ev) != 0) { perror("epoll ctrl"); return 1; }
        ev.data.fd = data_fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, data_fd, &ev) != 0) { perror("epoll data"); return 1; }
    }
    max_conns = dmesh_doca_max_conns();
    if (max_conns > MAX_SLOTS) max_conns = MAX_SLOTS;
    fprintf(stderr, "dpumesh-echo: server=%s dev=%s rep=%s mode=%s poll=%s\n", server, dev_pci, rep_pci,
            echo ? "echo" : "sink", busy ? "busy" : "epoll");
    dmesh_doca_stats_get(objs, &prev[0], &prev[1], &prev[2], &prev[3], &prev[4]);

    while (!g_stop) {
        int drained = 0, active = 0, s;
        double now;

        if (!busy && (dmesh_doca_ctrl_arm(objs) != DOCA_SUCCESS || dmesh_doca_data_arm(objs) != DOCA_SUCCESS)) {
            fprintf(stderr, "failed to arm progress engines\n");
            break;
        }
        if (dmesh_doca_ctrl_drain(objs) != DOCA_SUCCESS) { fprintf(stderr, "control drain failed\n"); break; }
        dmesh_doca_data_clear_and_drain(objs, data_fd, DATA_DRAIN_BUDGET, &drained);
        if (dmesh_doca_ctrl_advance(objs, &state) != DOCA_SUCCESS || state == DMESH_DOCA_STATE_ERROR) {
            fprintf(stderr, "control advance failed\n");
            break;
        }

        for (s = 0; s < max_conns; s++) {
            struct slot *sl = &slots[s];
            int32_t st = dmesh_doca_conn_state_get(objs, s);
            if (st != sl->state) {
                if (st == CONN_RUNNING) {
                    memset(sl, 0, sizeof(*sl));
                    fprintf(stderr, "slot %d running (mode %d)\n", s, dmesh_doca_conn_mode_get(objs, s));
                } else if (st == CONN_FREE || st == CONN_ERROR || st == CONN_CLOSING) {
                    if (sl->state == CONN_RUNNING)
                        fprintf(stderr, "slot %d closed: %llu segments, %llu bytes in, %llu pushes out\n", s,
                                (unsigned long long)sl->segs, (unsigned long long)sl->bytes,
                                (unsigned long long)sl->pushes);
                    /* keep the closed slot's push totals in the aggregate */
                    pushes += sl->pushes;
                    push_bytes += sl->push_bytes;
                    memset(sl, 0, sizeof(*sl));
                }
                sl->state = st;
            }
            if (st != CONN_RUNNING) continue;
            slot_wire(objs, s, sl);
            slot_pump(objs, s, sl, echo);
            active++;
        }

        now = now_sec();
        if (now - last >= 1.0) {
            double el = now - last;
            uint64_t p = pushes, pb = push_bytes;
            for (s = 0; s < max_conns; s++) { p += slots[s].pushes; pb += slots[s].push_bytes; }
            dmesh_doca_stats_get(objs, &cur[0], &cur[1], &cur[2], &cur[3], &cur[4]);
            if (cur[1] != prev[1] || p != prev_pushes) {
                if (t0 == 0) { t0 = last; recv0 = prev[1]; bytes0 = prev[2]; pushes0 = prev_pushes; }
                fprintf(stderr,
                        "TOTAL(%d conns): elapsed: %.2f, recv: %.0f DMA/s (%.2f Gbps), sent: %.0f push/s "
                        "(%.2f Gbps), dma_pending: %lld, dma_dropped: %lld\n",
                        active, el, (double)(cur[1] - prev[1]) / el,
                        (double)(cur[2] - prev[2]) * 8.0 / el / 1e9, (double)(p - prev_pushes) / el,
                        (double)(pb - prev_push_bytes) * 8.0 / el / 1e9,
                        (long long)cur[3], (long long)cur[4]);
            }
            prev_pushes = p; prev_push_bytes = pb;
            memcpy(prev, cur, sizeof(cur));
            last = now;
            if (exit_ops > 0 && cur[1] >= exit_ops) {
                fprintf(stderr, "bench: recv target %ld reached (recv=%lld), exiting\n", exit_ops, (long long)cur[1]);
                break;
            }
        }

        if (!busy && drained < DATA_DRAIN_BUDGET) {
            struct epoll_event evs[2];
            /* Short timeout: the control state machine and the staged sends
             * have no fd of their own, like the router's 1 ms safety tick. */
            epoll_wait(epfd, evs, 2, 1);
        }
    }

    if (t0 > 0) {
        double wall = now_sec() - t0;
        uint64_t p = pushes;
        int s2;
        for (s2 = 0; s2 < max_conns; s2++) p += slots[s2].pushes;
        dmesh_doca_stats_get(objs, &cur[0], &cur[1], &cur[2], &cur[3], &cur[4]);
        fprintf(stderr, "DPU_BENCH_DONE mode=%s wall_sec=%.2f recv_dma=%lld recv_dma_per_sec=%.0f "
                "recv_gbps=%.2f sent_push=%llu dma_dropped=%lld\n",
                echo ? "echo" : "sink", wall, (long long)(cur[1] - recv0), (double)(cur[1] - recv0) / wall,
                (double)(cur[2] - bytes0) * 8.0 / wall / 1e9, (unsigned long long)(p - pushes0), (long long)cur[4]);
    }
    fflush(NULL);
    _exit(0); /* DOCA teardown of live slots is not clean in any DPU-side process; exit like the legacy worker */
}
