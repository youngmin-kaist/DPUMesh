#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

#include <google/protobuf/stubs/common.h>
#include "hello.pb.h"

using dpumesh::bench::HelloRequest;

struct Options {
    double duration_s = 5.0;
    double warmup_s = 1.0;
    double report_interval_s = 1.0;

    size_t name_len = 16;
    size_t scores = 8;
    size_t pool = 1;

    uint64_t check_interval = 1024;
    int cpu = -1;
};

struct RunResult {
    double sec;
    uint64_t iters;
    uint64_t bytes;
};

static bool parse_u64_arg(const char *arg, const char *key, uint64_t *out) {
    const size_t n = std::strlen(key);

    if (std::strncmp(arg, key, n) != 0 || arg[n] != '=') {
        return false;
    }

    char *end = nullptr;
    unsigned long long v = std::strtoull(arg + n + 1, &end, 10);

    if (end == arg + n + 1 || *end != '\0') {
        return false;
    }

    *out = static_cast<uint64_t>(v);
    return true;
}

static bool parse_i32_arg(const char *arg, const char *key, int *out) {
    uint64_t tmp = 0;

    if (!parse_u64_arg(arg, key, &tmp)) {
        return false;
    }

    if (tmp > static_cast<uint64_t>(std::numeric_limits<int>::max())) {
        return false;
    }

    *out = static_cast<int>(tmp);
    return true;
}

static bool parse_f64_arg(const char *arg, const char *key, double *out) {
    const size_t n = std::strlen(key);

    if (std::strncmp(arg, key, n) != 0 || arg[n] != '=') {
        return false;
    }

    char *end = nullptr;
    double v = std::strtod(arg + n + 1, &end);

    if (end == arg + n + 1 || *end != '\0' || !std::isfinite(v)) {
        return false;
    }

    *out = v;
    return true;
}

static void usage(const char *prog) {
    std::cerr
        << "Usage: " << prog << " [options]\n"
        << "Options:\n"
        << "  --duration_s=N         measurement duration in seconds, default 5.0\n"
        << "  --warmup_s=N           warmup duration in seconds, default 1.0\n"
        << "  --report_interval_s=N  periodic throughput report interval, default 1.0\n"
        << "  --name_len=N           string name length, default 16\n"
        << "  --scores=N             number of packed u32 scores, default 8\n"
        << "  --pool=N               number of prebuilt messages, default 1\n"
        << "  --check_interval=N     time check interval, default 1024\n"
        << "  --cpu=N                pin benchmark thread to CPU N, Linux only\n";
}

static Options parse_args(int argc, char **argv) {
    Options opt;

    for (int i = 1; i < argc; i++) {
        uint64_t v = 0;

        if (parse_f64_arg(argv[i], "--duration_s", &opt.duration_s)) continue;
        if (parse_f64_arg(argv[i], "--warmup_s", &opt.warmup_s)) continue;
        if (parse_f64_arg(argv[i], "--report_interval_s", &opt.report_interval_s)) continue;

        if (parse_u64_arg(argv[i], "--name_len", &v)) {
            opt.name_len = static_cast<size_t>(v);
            continue;
        }

        if (parse_u64_arg(argv[i], "--scores", &v)) {
            opt.scores = static_cast<size_t>(v);
            continue;
        }

        if (parse_u64_arg(argv[i], "--pool", &v)) {
            opt.pool = static_cast<size_t>(v);
            continue;
        }

        if (parse_u64_arg(argv[i], "--check_interval", &v)) {
            opt.check_interval = v;
            continue;
        }

        if (parse_i32_arg(argv[i], "--cpu", &opt.cpu)) continue;

        if (std::strcmp(argv[i], "--help") == 0 ||
            std::strcmp(argv[i], "-h") == 0) {
            usage(argv[0]);
            std::exit(0);
        }

        std::cerr << "Unknown option: " << argv[i] << "\n";
        usage(argv[0]);
        std::exit(1);
    }

    if (opt.duration_s <= 0.0) {
        std::cerr << "--duration_s must be > 0\n";
        std::exit(1);
    }

    if (opt.warmup_s < 0.0) {
        std::cerr << "--warmup_s must be >= 0\n";
        std::exit(1);
    }

    if (opt.report_interval_s < 0.0) {
        std::cerr << "--report_interval_s must be >= 0\n";
        std::exit(1);
    }

    if (opt.pool == 0) {
        std::cerr << "--pool must be >= 1\n";
        std::exit(1);
    }

    if (opt.check_interval == 0) {
        std::cerr << "--check_interval must be >= 1\n";
        std::exit(1);
    }

    return opt;
}

static void pin_cpu_if_requested(int cpu) {
#ifdef __linux__
    if (cpu < 0) {
        return;
    }

    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);

    int rc = pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
    if (rc != 0) {
        std::cerr << "pthread_setaffinity_np failed, rc=" << rc << "\n";
        std::exit(1);
    }
#else
    (void)cpu;
#endif
}

static double now_sec() {
    using clock = std::chrono::steady_clock;
    auto t = clock::now().time_since_epoch();
    return std::chrono::duration<double>(t).count();
}

static std::string make_name(size_t len, size_t seed) {
    std::string s(len, 'a');

    for (size_t i = 0; i < len; i++) {
        s[i] = static_cast<char>('a' + ((seed + i) % 26));
    }

    return s;
}

static void build_request(HelloRequest *req,
                          uint64_t id,
                          size_t name_len,
                          size_t scores_count,
                          size_t seed) {
    req->Clear();

    req->set_id(id);
    req->set_name(make_name(name_len, seed));

    auto *scores = req->mutable_scores();
    scores->Reserve(static_cast<int>(scores_count));

    /*
     * 1..100 범위의 u32 값을 사용합니다.
     * 대부분 1-byte varint로 encoding되므로 packed scores payload 크기는
     * scores_count와 거의 같습니다.
     */
    for (size_t i = 0; i < scores_count; i++) {
        scores->Add(static_cast<uint32_t>((i + seed) % 100 + 1));
    }
}

int main(int argc, char **argv) {
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    Options opt = parse_args(argc, argv);
    pin_cpu_if_requested(opt.cpu);

    std::vector<HelloRequest> reqs(opt.pool);
    std::vector<int> sizes(opt.pool);

    size_t min_size = std::numeric_limits<size_t>::max();
    size_t max_size = 0;

    for (size_t i = 0; i < opt.pool; i++) {
        /*
         * id를 1..100 범위로 제한해 pool 크기가 커져도 id varint 길이가
         * 불필요하게 바뀌지 않도록 합니다.
         */
        const uint64_t id = static_cast<uint64_t>((i % 100) + 1);

        build_request(&reqs[i], id, opt.name_len, opt.scores, i);

        size_t sz = reqs[i].ByteSizeLong();
        if (sz == 0) {
            std::cerr << "Serialized message size is zero\n";
            return 1;
        }

        if (sz > static_cast<size_t>(std::numeric_limits<int>::max())) {
            std::cerr << "Serialized message too large\n";
            return 1;
        }

        sizes[i] = static_cast<int>(sz);
        min_size = std::min(min_size, sz);
        max_size = std::max(max_size, sz);
    }

    std::vector<uint8_t> out(max_size + 64, 0);

    volatile uint64_t sink = 0;

    auto run_for = [&](double duration_s,
                       bool measured,
                       bool report_progress) -> RunResult {
        uint64_t iters = 0;
        uint64_t bytes = 0;
        size_t idx = 0;

        const double t0 = now_sec();
        const double deadline = t0 + duration_s;

        double last_report_t = t0;
        uint64_t last_report_iters = 0;
        uint64_t last_report_bytes = 0;

        double next_report_t = t0 + opt.report_interval_s;

        while (true) {
            for (uint64_t j = 0; j < opt.check_interval; j++) {
                const HelloRequest &req = reqs[idx];
                const int sz = sizes[idx];

                bool ok = req.SerializeToArray(out.data(), sz);
                if (!ok) {
                    std::cerr << "SerializeToArray failed\n";
                    std::exit(1);
                }

                /*
                 * Dead-store elimination 방지용 관찰입니다.
                 * 전체 checksum을 계산하면 benchmark가 checksum 비용을 포함하므로
                 * 첫 byte와 마지막 byte만 읽습니다.
                 */
                sink += out[0];
                sink += out[static_cast<size_t>(sz - 1)];

                bytes += static_cast<uint64_t>(sz);
                iters++;

                idx++;
                if (idx == opt.pool) {
                    idx = 0;
                }
            }

            const double now = now_sec();

            if (measured &&
                report_progress &&
                opt.report_interval_s > 0.0 &&
                now >= next_report_t) {
                const double interval_s = now - last_report_t;
                const uint64_t interval_iters = iters - last_report_iters;
                const uint64_t interval_bytes = bytes - last_report_bytes;

                const double interval_req_s =
                    static_cast<double>(interval_iters) / interval_s;
                const double interval_gb_s =
                    static_cast<double>(interval_bytes) / interval_s / 1e9;

                const double elapsed_s = now - t0;
                const double cumulative_req_s =
                    static_cast<double>(iters) / elapsed_s;
                const double cumulative_gb_s =
                    static_cast<double>(bytes) / elapsed_s / 1e9;

                std::cerr
                    << "progress"
                    << " elapsed_s=" << elapsed_s
                    << " interval_s=" << interval_s
                    << " interval_iters=" << interval_iters
                    << " interval_req_s=" << interval_req_s
                    << " interval_GB_s=" << interval_gb_s
                    << " cumulative_iters=" << iters
                    << " cumulative_req_s=" << cumulative_req_s
                    << " cumulative_GB_s=" << cumulative_gb_s
                    << "\n";

                last_report_t = now;
                last_report_iters = iters;
                last_report_bytes = bytes;

                do {
                    next_report_t += opt.report_interval_s;
                } while (next_report_t <= now);
            }

            if (now >= deadline) {
                break;
            }
        }

        const double t1 = now_sec();

        if (!measured) {
            return {0.0, iters, bytes};
        }

        return {t1 - t0, iters, bytes};
    };

    if (opt.warmup_s > 0.0) {
        run_for(opt.warmup_s, false, false);
    }

    RunResult r = run_for(opt.duration_s, true, true);

    const double req_per_sec = static_cast<double>(r.iters) / r.sec;
    const double ns_per_req = r.sec * 1e9 / static_cast<double>(r.iters);
    const double gb_per_sec = static_cast<double>(r.bytes) / r.sec / 1e9;

    std::cout << "schema=HelloRequest\n";
    std::cout << "serializer=google_protobuf_cpp_SerializeToArray\n";
    std::cout << "target_duration_s=" << opt.duration_s << "\n";
    std::cout << "actual_time_s=" << r.sec << "\n";
    std::cout << "warmup_s=" << opt.warmup_s << "\n";
    std::cout << "report_interval_s=" << opt.report_interval_s << "\n";
    std::cout << "measured_iters=" << r.iters << "\n";
    std::cout << "measured_bytes=" << r.bytes << "\n";
    std::cout << "pool=" << opt.pool << "\n";
    std::cout << "name_len=" << opt.name_len << "\n";
    std::cout << "scores=" << opt.scores << "\n";
    std::cout << "serialized_size_B_first=" << sizes[0] << "\n";
    std::cout << "serialized_size_B_min=" << min_size << "\n";
    std::cout << "serialized_size_B_max=" << max_size << "\n";
    std::cout << "throughput_req_s=" << req_per_sec << "\n";
    std::cout << "ns_per_req=" << ns_per_req << "\n";
    std::cout << "throughput_GB_s=" << gb_per_sec << "\n";
    std::cout << "check_interval=" << opt.check_interval << "\n";
    std::cout << "sink=" << sink << "\n";

    google::protobuf::ShutdownProtobufLibrary();
    return 0;
}