// dmesh-router-cpp — HTTP/2 router on the DPUMesh DMA datapath, C++ + nghttp2.
//
// Functional twin of the Rust/hyper `dmesh-router`: it accepts connections that
// arrive over PCIe DMA from the host, terminates HTTP/2 on them, and forwards
// each request to a backend the host provides over a second DMA channel. The
// datapath below is the same C code both routers drive (shim.c + comch_server.c);
// only the HTTP engine differs — libnghttp2 here, hyper/h2 there.
//
// The loop mirrors run_dpu_worker_event_driven() in DPUMesh/dpu_worker.c and the
// Rust Driver::run(): arm both progress engines, drain control, drain data with
// a budget, advance the state machine, then pump the sessions.

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <string>

#include <sys/epoll.h>
#include <unistd.h>

#include "router.hpp"

using namespace dmesh;

// Max consumer-PE events drained per iteration, matching DATA_DRAIN_BUDGET in
// dpu_worker.c and the Rust driver: bounds each wakeup so the control path
// cannot starve.
static constexpr int kDataDrainBudget = 8192;

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int) { g_stop = 1; }

static int64_t now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<int64_t>(ts.tv_sec) * 1000 + ts.tv_nsec / 1000000;
}

// Same one-per-second datapath report the Rust router logs, so benchmark output
// from the two implementations lines up.
static void report_stats(struct objects *objs, int64_t *last_ms, int64_t prev[5]) {
    const int64_t now = now_ms();
    const int64_t elapsed = now - *last_ms;
    if (elapsed < 1000) {
        return;
    }
    int64_t cur[5] = {0, 0, 0, 0, 0};
    dmesh_doca_stats_get(objs, &cur[0], &cur[1], &cur[2], &cur[3], &cur[4]);
    if (cur[0] != prev[0] || cur[1] != prev[1]) {
        const double secs = static_cast<double>(elapsed) / 1000.0;
        log_info("datapath stats recv_msgs_per_s=%lld recv_gbps=%.2f sent_msgs_per_s=%lld "
                 "dma_pending=%lld dma_dropped=%lld",
                 static_cast<long long>((cur[1] - prev[1]) / secs),
                 (static_cast<double>(cur[2] - prev[2]) * 8.0) / secs / 1e9,
                 static_cast<long long>((cur[0] - prev[0]) / secs),
                 static_cast<long long>(cur[3]), static_cast<long long>(cur[4]));
    }
    std::memcpy(prev, cur, sizeof(cur));
    *last_ms = now;
}

int main() {
    Config cfg;
    std::string error;
    if (!Config::from_env(&cfg, &error)) {
        std::fprintf(stderr, "Invalid configuration: %s\n", error.c_str());
        return 64; // EX_USAGE
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);
    signal(SIGPIPE, SIG_IGN);

    // Stage 1: open the device and start the comch server the host shim dials.
    struct objects *objs = nullptr;
    int32_t rc = dmesh_doca_init(cfg.dev_pci.c_str(), cfg.rep_pci.c_str(), cfg.server_name.c_str(),
                                 &objs);
    if (rc != DOCA_SUCCESS || objs == nullptr) {
        std::fprintf(stderr, "DOCA comch initialization failure: %s\n",
                     doca_error_get_descr(static_cast<doca_error_t>(rc)));
        return 1;
    }
    log_info("comch server started server=%s dev=%s rep=%s backend_proto=h2",
             cfg.server_name.c_str(), cfg.dev_pci.c_str(), cfg.rep_pci.c_str());

    // Stage 2: build the shared infrastructure (DPA pool, consumer PE, DMA
    // engine) before serving connections; this also makes the data PE fd
    // available for epoll registration.
    enum dmesh_doca_init_state state = DMESH_DOCA_STATE_SERVER_STARTED;
    if (dmesh_doca_ctrl_advance(objs, &state) != DOCA_SUCCESS ||
        state != DMESH_DOCA_STATE_RUNNING) {
        std::fprintf(stderr, "dmesh infrastructure did not reach RUNNING\n");
        return 1;
    }
    log_info("infrastructure ready");

    int ctrl_fd = -1;
    int data_fd = -1;
    if (dmesh_doca_ctrl_get_fd(objs, &ctrl_fd) != DOCA_SUCCESS ||
        dmesh_doca_data_get_fd(objs, &data_fd) != DOCA_SUCCESS) {
        std::fprintf(stderr, "failed to get progress-engine notification fds\n");
        return 1;
    }

    int epfd = -1;
    if (!cfg.busy_poll) {
        epfd = epoll_create1(0);
        if (epfd < 0) {
            std::fprintf(stderr, "epoll_create1: %s\n", std::strerror(errno));
            return 1;
        }
        struct epoll_event ev;
        std::memset(&ev, 0, sizeof(ev));
        ev.events = EPOLLIN;
        ev.data.fd = ctrl_fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, ctrl_fd, &ev) != 0) {
            std::fprintf(stderr, "epoll_ctl(ctrl): %s\n", std::strerror(errno));
            return 1;
        }
        ev.data.fd = data_fd;
        if (epoll_ctl(epfd, EPOLL_CTL_ADD, data_fd, &ev) != 0) {
            std::fprintf(stderr, "epoll_ctl(data): %s\n", std::strerror(errno));
            return 1;
        }
    } else {
        log_info("busy-poll mode (progress engines polled, fds unused)");
    }

    Router router(objs, cfg);
    int64_t stats_last = now_ms();
    int64_t stats_prev[5] = {0, 0, 0, 0, 0};
    dmesh_doca_stats_get(objs, &stats_prev[0], &stats_prev[1], &stats_prev[2], &stats_prev[3],
                         &stats_prev[4]);

    while (g_stop == 0) {
        // Arm first so events pending now (or arriving during the drains below)
        // signal the fds; the eager drain closes the race where a setup step
        // already consumed the awaited event.
        if (!cfg.busy_poll) {
            if (dmesh_doca_ctrl_arm(objs) != DOCA_SUCCESS ||
                dmesh_doca_data_arm(objs) != DOCA_SUCCESS) {
                std::fprintf(stderr, "failed to arm progress engines\n");
                break;
            }
        }

        if (dmesh_doca_ctrl_drain(objs) != DOCA_SUCCESS) {
            std::fprintf(stderr, "control-path drain failed\n");
            break;
        }
        int drained = 0;
        dmesh_doca_data_clear_and_drain(objs, data_fd, kDataDrainBudget, &drained);

        if (dmesh_doca_ctrl_advance(objs, &state) != DOCA_SUCCESS ||
            state == DMESH_DOCA_STATE_ERROR) {
            std::fprintf(stderr, "control-path advance failed\n");
            break;
        }

        router.poll_slots();
        router.pump();
        report_stats(objs, &stats_last, stats_prev);

        // Budget exhausted: more datapath work is pending, don't sleep.
        if (drained >= kDataDrainBudget || cfg.busy_poll) {
            continue;
        }

        // Sleep until either PE signals. The 1ms cap is the same safety net the
        // Rust driver uses: the notification fd does not re-signal while a
        // notification is already pending, so a lost edge would otherwise stall
        // the datapath. Under load the loop never reaches here.
        struct epoll_event evs[2];
        const int n = epoll_wait(epfd, evs, 2, 1);
        for (int i = 0; i < n; i++) {
            if (evs[i].data.fd == ctrl_fd) {
                dmesh_doca_ctrl_clear_and_drain(objs, ctrl_fd);
            }
            // Data-path events are processed by the bounded drain at the top of
            // the next iteration.
        }
    }

    log_info("shutting down");
    if (epfd >= 0) {
        close(epfd);
    }
    dmesh_doca_comch_destroy(objs);
    return 0;
}
