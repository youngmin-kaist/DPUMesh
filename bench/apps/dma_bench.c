/* dma_bench: the host-side DMA throughput benchmark over the DPUmesh native API.
 *
 * The port of host_worker.c's benchmark role (one connection per thread,
 * fixed-size messages streamed as fast as the transport takes them) onto
 * libdpumesh: channel -> one EQ + one QP per thread -> dmesh_alloc/post_send
 * under the library's credit and tail policy, replies (echo mode) read
 * zero-copy from the RX region. The DPU peer is dpumesh-echo (src/transport/
 * apps/dpu_echo.c) in the matching mode.
 *
 *   BENCH_MODE=sink   one-way host -> DPU stream; the DPU counts completions
 *   BENCH_MODE=echo   the DPU pushes every byte back; RTT is measured per
 *                     message from a timestamp in its first 8 bytes
 *
 * Once a second the main thread prints the aggregate over all threads and at
 * the end a HOST_BENCH_DONE line. "DMA" here means wire descriptors: a post
 * of S bytes becomes ceil(S/8064) forward descriptors, each one DPA copy.
 *
 * Environment (besides the library's DPUMESH_*): BENCH_THREADS (1),
 * BENCH_SIZE (8192), BENCH_DURATION seconds (10), BENCH_MODE (sink),
 * BENCH_WINDOW bytes in flight per thread in echo mode (262144),
 * BENCH_SERVICE (dma-echo). */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <dpumesh/dmesh.h>

#define MAX_THREADS 32
#define EVENT_BATCH 64
#define WIRE_DESC_MAX 8064u
#define HIST_BUCKETS (1u << 20) /* RTT histogram, 1 us per bucket */

enum { MODE_SINK = 0, MODE_ECHO = 1 };

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

struct worker {
    dmesh_channel_t *ch; /* the process's one channel, shared by every thread */
    int idx, mode;
    uint32_t size, post_max, window;
    double duration, start_at;
    const char *service;
    atomic_int *stop;
    /* live counters, read by the reporter (monotonic, torn reads tolerated) */
    _Atomic uint64_t posts, tx_bytes, rx_bytes, rx_msgs, tx_ready, rtt_sum_ns, rtt_cnt;
    _Atomic uint32_t rtt_max_us;
    uint32_t *hist;
    int failed;
    double wall;
};

/* Fixed-size message stream: the first 8 bytes carry the send timestamp so the
 * receiver can time each message even when a RECV fragment splits it. */
struct reframer {
    uint32_t off;      /* bytes of the current message already seen */
    uint8_t hdr[8];
    uint64_t stamp;
};

static void rx_feed(struct worker *w, struct reframer *rf, const uint8_t *buf, uint32_t len)
{
    while (len > 0) {
        uint32_t take = w->size - rf->off;
        if (take > len) take = len;
        if (rf->off < 8) {
            uint32_t h = 8 - rf->off < take ? 8 - rf->off : take;
            memcpy(rf->hdr + rf->off, buf, h);
        }
        rf->off += take;
        buf += take;
        len -= take;
        if (rf->off == w->size) {
            uint64_t stamp;
            memcpy(&stamp, rf->hdr, 8);
            if (stamp != 0) {
                struct timespec ts;
                uint64_t now;
                clock_gettime(CLOCK_MONOTONIC, &ts);
                now = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
                if (now > stamp) {
                    uint64_t rtt = now - stamp;
                    uint32_t us = (uint32_t)(rtt / 1000);
                    atomic_fetch_add(&w->rtt_sum_ns, rtt);
                    atomic_fetch_add(&w->rtt_cnt, 1);
                    if (us > atomic_load(&w->rtt_max_us)) atomic_store(&w->rtt_max_us, us);
                    w->hist[us < HIST_BUCKETS ? us : HIST_BUCKETS - 1]++;
                }
            }
            atomic_fetch_add(&w->rx_msgs, 1);
            rf->off = 0;
        }
    }
}

static int wait_fd(int fd, int timeout_ms)
{
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    int rc;
    uint64_t count;
    do {
        rc = poll(&pfd, 1, timeout_ms);
    } while (rc < 0 && errno == EINTR);
    if (rc > 0)
        while (read(fd, &count, sizeof(count)) == sizeof(count)) { }
    return rc;
}

static void *worker_main(void *arg)
{
    struct worker *w = arg;
    dmesh_channel_t *ch = w->ch;
    dmesh_eq_t *eq = NULL;
    dmesh_qp_t *qp = NULL;
    dmesh_event_t events[EVENT_BATCH];
    struct reframer rf = {0};
    uint64_t inflight = 0; /* echo: bytes posted but not yet received back */
    int blocked = 0, eq_fd;
    double t_end, t0;

    /* One channel per process (dmesh.h); each thread owns its EQ and QP. */
    eq = dmesh_create_eq(ch);
    qp = eq ? dmesh_create_qp(eq, w->service) : NULL;
    eq_fd = eq ? dmesh_eq_fd(eq) : -1;
    if (!eq || !qp || eq_fd < 0) {
        fprintf(stderr, "worker %d: DPUmesh setup failed: %s\n", w->idx, strerror(errno));
        w->failed = 1;
        return NULL;
    }
    w->post_max = (uint32_t)dmesh_post_max(ch);
    if (w->size > w->post_max) {
        fprintf(stderr, "worker %d: BENCH_SIZE %u exceeds post max %u\n", w->idx, w->size, w->post_max);
        w->failed = 1;
        goto out;
    }

    while (now_sec() < w->start_at) usleep(100);
    t0 = now_sec();
    t_end = t0 + w->duration;

    while (!atomic_load(w->stop)) {
        int n, i, progressed = 0;
        double now = now_sec();
        if (now >= t_end) break;

        /* Post: sink streams until the library pushes back (EAGAIN -> TX_READY);
         * echo keeps `window` bytes in flight. */
        if (!blocked) {
            int budget = 64;
            while (budget-- > 0 && (w->mode == MODE_SINK || inflight + w->size <= w->window)) {
                uint8_t *b = dmesh_alloc(qp, w->size);
                if (!b) {
                    if (errno == EAGAIN) { blocked = 1; break; }
                    fprintf(stderr, "worker %d: dmesh_alloc: %s\n", w->idx, strerror(errno));
                    w->failed = 1;
                    goto done;
                }
                if (w->size >= 8) {
                    struct timespec ts;
                    uint64_t stamp;
                    clock_gettime(CLOCK_MONOTONIC, &ts);
                    stamp = (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
                    memcpy(b, &stamp, 8);
                }
                if (dmesh_post_send(qp, b, w->size) != 0) {
                    fprintf(stderr, "worker %d: dmesh_post_send: %s\n", w->idx, strerror(errno));
                    w->failed = 1;
                    goto done;
                }
                atomic_fetch_add(&w->posts, 1);
                atomic_fetch_add(&w->tx_bytes, w->size);
                inflight += w->size;
                progressed = 1;
            }
            /* Echo: do not let the 500 us tail timer add to the RTT. */
            if (w->mode == MODE_ECHO && progressed) dmesh_flush(qp);
        }

        while ((n = dmesh_poll_eq(eq, events, EVENT_BATCH)) > 0) {
            progressed = 1;
            for (i = 0; i < n; i++) {
                dmesh_event_t *ev = &events[i];
                switch (ev->type) {
                case DMESH_EVENT_RECV:
                    atomic_fetch_add(&w->rx_bytes, ev->len);
                    if (w->mode == MODE_ECHO) {
                        rx_feed(w, &rf, ev->buf, ev->len);
                        inflight = inflight > ev->len ? inflight - ev->len : 0;
                    }
                    dmesh_release_rx_buffer(ch, ev);
                    break;
                case DMESH_EVENT_TX_READY:
                    atomic_fetch_add(&w->tx_ready, 1);
                    blocked = 0;
                    break;
                case DMESH_EVENT_RECV_FIN:
                case DMESH_EVENT_TX_ERROR:
                    fprintf(stderr, "worker %d: stream ended (event %d)\n", w->idx, ev->type);
                    w->failed = 1;
                    goto done;
                default:
                    break;
                }
            }
        }

        if (!progressed) {
            /* Nothing to do until the library signals: bounded wait so the
             * duration check and a lost TX_READY cannot hang the worker. */
            wait_fd(eq_fd, 1);
            if (blocked) blocked = 0; /* retry alloc after the wait */
        }
    }
done:
    w->wall = now_sec() - t0;
    if (w->mode == MODE_ECHO && !w->failed) {
        /* Retire the replies already in flight so the byte counts match. */
        double until = now_sec() + 1.0;
        while (inflight > 0 && now_sec() < until) {
            int n, i;
            while ((n = dmesh_poll_eq(eq, events, EVENT_BATCH)) > 0)
                for (i = 0; i < n; i++)
                    if (events[i].type == DMESH_EVENT_RECV) {
                        atomic_fetch_add(&w->rx_bytes, events[i].len);
                        rx_feed(w, &rf, events[i].buf, events[i].len);
                        inflight = inflight > events[i].len ? inflight - events[i].len : 0;
                        dmesh_release_rx_buffer(ch, &events[i]);
                    }
            wait_fd(eq_fd, 1);
        }
    }
out:
    dmesh_destroy_qp(qp);
    dmesh_destroy_eq(eq);
    return NULL;
}

static uint32_t percentile(uint32_t *hist, uint64_t total, double p)
{
    uint64_t target = (uint64_t)((double)total * p), acc = 0;
    uint32_t i;
    for (i = 0; i < HIST_BUCKETS; i++) {
        acc += hist[i];
        if (acc >= target && total) return i;
    }
    return 0;
}

int main(void)
{
    int threads = atoi(env_or("BENCH_THREADS", "1"));
    uint32_t size = (uint32_t)atoi(env_or("BENCH_SIZE", "8192"));
    double duration = atof(env_or("BENCH_DURATION", "10"));
    int mode = strcmp(env_or("BENCH_MODE", "sink"), "echo") == 0 ? MODE_ECHO : MODE_SINK;
    uint32_t window = (uint32_t)atoi(env_or("BENCH_WINDOW", "262144"));
    const char *service = env_or("BENCH_SERVICE", "dma-echo");
    struct worker w[MAX_THREADS];
    pthread_t tids[MAX_THREADS];
    int joined[MAX_THREADS] = {0};
    atomic_int stop = 0;
    uint64_t p_posts = 0, p_tx = 0, p_rx = 0, p_msgs = 0;
    double start_at, last, t0;
    int i, alive;

    if (threads < 1 || threads > MAX_THREADS || size < 8 || duration <= 0) {
        fprintf(stderr, "usage: BENCH_THREADS=1..%d BENCH_SIZE>=8 BENCH_DURATION>0 BENCH_MODE=sink|echo %s\n",
                MAX_THREADS, "dma_bench");
        return 2;
    }
    if (getenv("DPUMESH_SERVICE")) {
        fprintf(stderr, "unset DPUMESH_SERVICE: dma_bench is a client\n");
        return 2;
    }
    memset(w, 0, sizeof(w));
    dmesh_channel_t *ch = dmesh_create_channel();
    if (!ch) {
        fprintf(stderr, "dmesh_create_channel: %s\n", strerror(errno));
        return 1;
    }
    start_at = now_sec() + 0.5 + 0.05 * threads; /* QP setup takes ms each */
    for (i = 0; i < threads; i++) {
        w[i].ch = ch;
        w[i].idx = i; w[i].mode = mode; w[i].size = size; w[i].duration = duration;
        w[i].start_at = start_at; w[i].service = service; w[i].stop = &stop; w[i].window = window;
        w[i].hist = calloc(HIST_BUCKETS, sizeof(uint32_t));
        if (!w[i].hist || pthread_create(&tids[i], NULL, worker_main, &w[i]) != 0) {
            fprintf(stderr, "failed to start worker %d\n", i);
            return 1;
        }
    }
    fprintf(stderr, "dma_bench: threads=%d size=%u mode=%s duration=%.0fs window=%u service=%s\n", threads,
            size, mode == MODE_ECHO ? "echo" : "sink", duration, window, service);

    t0 = last = now_sec();
    for (;;) {
        uint64_t posts = 0, tx = 0, rx = 0, msgs = 0;
        double now, el;
        usleep(1000000);
        now = now_sec();
        el = now - last;
        last = now;
        for (i = 0; i < threads; i++) {
            posts += atomic_load(&w[i].posts); tx += atomic_load(&w[i].tx_bytes);
            rx += atomic_load(&w[i].rx_bytes); msgs += atomic_load(&w[i].rx_msgs);
        }
        if (posts != p_posts || rx != p_rx) {
            double tx_msgs = (double)(posts - p_posts) / el;
            fprintf(stderr, "HOST_BENCH_RATE tx: %.0f msg/s (%.0f DMA/s, %.2f Gbps)", tx_msgs,
                    tx_msgs * ((size + WIRE_DESC_MAX - 1) / WIRE_DESC_MAX),
                    (double)(tx - p_tx) * 8.0 / el / 1e9);
            if (mode == MODE_ECHO)
                fprintf(stderr, ", rx: %.0f msg/s (%.2f Gbps)", (double)(msgs - p_msgs) / el,
                        (double)(rx - p_rx) * 8.0 / el / 1e9);
            fputc('\n', stderr);
        }
        p_posts = posts; p_tx = tx; p_rx = rx; p_msgs = msgs;
        alive = 0;
        for (i = 0; i < threads; i++) {
            if (!joined[i] && pthread_tryjoin_np(tids[i], NULL) == 0) joined[i] = 1;
            if (!joined[i]) alive++;
        }
        if (alive == 0) break;
        if (now - t0 > duration + 15.0) {
            fprintf(stderr, "watchdog: stopping workers\n");
            atomic_store(&stop, 1);
        }
    }

    {
        uint64_t posts = 0, tx = 0, rx = 0, msgs = 0, rtt_sum = 0, rtt_cnt = 0, tx_ready = 0;
        uint32_t *hist = calloc(HIST_BUCKETS, sizeof(uint32_t)), rtt_max = 0, j;
        double wall = 0;
        int failed = 0;
        for (i = 0; i < threads; i++) {
            posts += w[i].posts; tx += w[i].tx_bytes; rx += w[i].rx_bytes; msgs += w[i].rx_msgs;
            rtt_sum += w[i].rtt_sum_ns; rtt_cnt += w[i].rtt_cnt; tx_ready += w[i].tx_ready;
            if (w[i].rtt_max_us > rtt_max) rtt_max = w[i].rtt_max_us;
            if (w[i].wall > wall) wall = w[i].wall;
            failed += w[i].failed;
            for (j = 0; j < HIST_BUCKETS; j++) hist[j] += w[i].hist[j];
        }
        if (wall <= 0) wall = duration;
        printf("HOST_BENCH_DONE mode=%s threads=%d size=%u wall_sec=%.2f posts=%llu tx_msg_per_sec=%.0f "
               "tx_dma_per_sec=%.0f tx_gbps=%.2f rx_msg_per_sec=%.0f rx_gbps=%.2f tx_ready_events=%llu failed=%d",
               mode == MODE_ECHO ? "echo" : "sink", threads, size, wall, (unsigned long long)posts,
               (double)posts / wall, (double)posts * ((size + WIRE_DESC_MAX - 1) / WIRE_DESC_MAX) / wall,
               (double)tx * 8.0 / wall / 1e9, (double)msgs / wall, (double)rx * 8.0 / wall / 1e9,
               (unsigned long long)tx_ready, failed);
        if (rtt_cnt)
            printf(" rtt_avg_us=%.1f rtt_p50_us=%u rtt_p99_us=%u rtt_max_us=%u", (double)rtt_sum / (double)rtt_cnt / 1e3,
                   percentile(hist, rtt_cnt, 0.50), percentile(hist, rtt_cnt, 0.99), rtt_max);
        printf("\n");
        free(hist);
        dmesh_destroy_channel(ch);
        return failed ? 1 : 0;
    }
}
