#include "offload_benchmark.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <limits>
#include <numeric>
#include <vector>

namespace {

static inline uint32_t align_up_u32(uint32_t value, uint32_t align)
{
    return (value + align - 1U) & ~(align - 1U);
}

static inline double ns_to_us(uint64_t ns)
{
    return static_cast<double>(ns) / 1000.0;
}

static double percentile_us(std::vector<uint64_t> lat_ns, double p)
{
    if (lat_ns.empty())
        return 0.0;

    std::sort(lat_ns.begin(), lat_ns.end());
    const double pos = (p / 100.0) * static_cast<double>(lat_ns.size() - 1);
    const size_t idx = static_cast<size_t>(pos);
    return ns_to_us(lat_ns[idx]);
}

static demo::HelloRequest make_request(const GrpcOffloadBenchConfig &cfg)
{
    demo::HelloRequest req{};
    req.id_ = 1;
    req.name_.assign(cfg.name_len, 'a');
    req.scores_.resize(cfg.scores_count);
    for (uint32_t i = 0; i < cfg.scores_count; ++i)
        req.scores_[i] = cfg.score_seed + i;
    return req;
}

static uint32_t hello_request_flat_msg_len(const demo::HelloRequest &req)
{
    uint32_t off = sizeof(HelloRequestFlat);

    off += static_cast<uint32_t>(req.name_.size());
    off = align_up_u32(off, alignof(uint32_t));
    off += static_cast<uint32_t>(req.scores_.size() * sizeof(uint32_t));
    return off;
}

static void print_failure_details(const demo::HelloRequest &req,
                                  const std::vector<uint32_t> &request_ids,
                                  const std::vector<ProtoCompletion> &cpls)
{
    const uint32_t host_msg_len = hello_request_flat_msg_len(req);

    std::fprintf(stderr,
                 "grpc_offload_bench failure: host_msg_len=%u out_cap=%u name_len=%zu scores_count=%zu\n",
                 host_msg_len,
                 GRPC_DPA_MAX_ENCODED_BUF_SIZE,
                 req.name_.size(),
                 req.scores_.size());

    for (size_t i = 0; i < cpls.size(); ++i) {
        std::fprintf(stderr,
                     "  cpl[%zu]: request_id=%u status=%d encoded_len=%u\n",
                     i,
                     cpls[i].request_id,
                     cpls[i].status,
                     cpls[i].encoded_len);
    }

    if (cpls.empty()) {
        for (size_t i = 0; i < request_ids.size(); ++i) {
            std::fprintf(stderr,
                         "  req[%zu]: request_id=%u no completion returned\n",
                         i,
                         request_ids[i]);
        }
    }
}

} // namespace

doca_error_t grpc_proto_run_simple_benchmark(GrpcProtoRuntime *rt,
                                              const GrpcOffloadBenchConfig *cfg,
                                              GrpcOffloadBenchStats *stats)
{
    if (rt == nullptr || cfg == nullptr || stats == nullptr)
        return DOCA_ERROR_INVALID_VALUE;
    if (cfg->measure_iters == 0)
        return DOCA_ERROR_INVALID_VALUE;
    if (cfg->submit_batch_size == 0)
        return DOCA_ERROR_INVALID_VALUE;

    *stats = GrpcOffloadBenchStats{};

    demo::HelloRequest req = make_request(*cfg);
    std::vector<uint8_t> encoded;
    std::vector<std::vector<uint8_t>> encoded_batch;
    std::vector<ProtoCompletion> cpls;
    std::vector<demo::HelloRequest> req_batch;
    std::vector<uint32_t> request_ids;
    uint32_t next_request_id = 0;
    req_batch.reserve(cfg->submit_batch_size);
    request_ids.reserve(cfg->submit_batch_size);
    cpls.reserve(cfg->submit_batch_size);

    for (uint32_t i = 0; i < cfg->warmup_iters; ) {
        const uint32_t chunk = std::min(cfg->submit_batch_size, cfg->warmup_iters - i);
        req_batch.assign(chunk, req);
        request_ids.resize(chunk);
        for (uint32_t j = 0; j < chunk; ++j)
            request_ids[j] = next_request_id + j;
        if (chunk == 1U)
            (void)grpc_proto_serialize_hello(rt, req, next_request_id, encoded, nullptr);
        else
            (void)grpc_proto_serialize_hello_batch(rt, req_batch, request_ids, encoded_batch, nullptr);
        next_request_id += chunk;
        i += chunk;
    }

    std::vector<uint64_t> latencies_ns;
    latencies_ns.reserve(cfg->measure_iters);

    const auto wall_start = std::chrono::steady_clock::now();
    for (uint32_t i = 0; i < cfg->measure_iters; ) {
        const uint32_t chunk = std::min(cfg->submit_batch_size, cfg->measure_iters - i);
        req_batch.assign(chunk, req);
        request_ids.resize(chunk);
        for (uint32_t j = 0; j < chunk; ++j)
            request_ids[j] = next_request_id + j;
        doca_error_t ret;

        const auto t0 = std::chrono::steady_clock::now();
        cpls.clear();
        if (chunk == 1U) {
            ProtoCompletion cpl{};
            ret = grpc_proto_serialize_hello(rt, req, next_request_id, encoded, &cpl);
            cpls.push_back(cpl);
        } else {
            ret = grpc_proto_serialize_hello_batch(rt, req_batch, request_ids, encoded_batch, &cpls);
        }
        const auto t1 = std::chrono::steady_clock::now();

        const uint64_t lat_ns = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count());
        const uint64_t per_req_lat_ns = lat_ns / chunk;

        for (uint32_t j = 0; j < chunk; ++j) {
            stats->total_iters++;
            latencies_ns.push_back(per_req_lat_ns);
        }

        if (ret == DOCA_SUCCESS) {
            bool chunk_ok = true;

            if (chunk == 1U) {
                if (!cpls.empty() && cpls[0].status != 0)
                    chunk_ok = false;
            } else {
                for (uint32_t j = 0; j < chunk; ++j) {
                    if (j >= cpls.size() || cpls[j].status != 0) {
                        chunk_ok = false;
                        break;
                    }
                }
            }

            if (chunk_ok) {
                stats->success_iters += chunk;
                if (chunk == 1U) {
                    stats->bytes_total += encoded.size();
                } else {
                    for (uint32_t j = 0; j < chunk; ++j)
                        stats->bytes_total += encoded_batch[j].size();
                }
            } else {
                stats->failed_iters += chunk;
                print_failure_details(req, request_ids, cpls);
            }
        } else {
            stats->failed_iters += chunk;
            print_failure_details(req, request_ids, cpls);
        }
        next_request_id += chunk;
        i += chunk;
    }
    const auto wall_end = std::chrono::steady_clock::now();

    stats->wall_time_sec =
        std::chrono::duration_cast<std::chrono::duration<double>>(wall_end - wall_start).count();

    if (!latencies_ns.empty()) {
        uint64_t min_ns = std::numeric_limits<uint64_t>::max();
        uint64_t max_ns = 0;
        long double sum_ns = 0;
        for (uint64_t ns : latencies_ns) {
            min_ns = std::min(min_ns, ns);
            max_ns = std::max(max_ns, ns);
            sum_ns += ns;
        }

        stats->latency_us_avg = ns_to_us(static_cast<uint64_t>(sum_ns / latencies_ns.size()));
        stats->latency_us_min = ns_to_us(min_ns);
        stats->latency_us_max = ns_to_us(max_ns);
        stats->latency_us_p50 = percentile_us(latencies_ns, 50.0);
        stats->latency_us_p95 = percentile_us(latencies_ns, 95.0);
        stats->latency_us_p99 = percentile_us(latencies_ns, 99.0);
    }

    if (stats->wall_time_sec > 0.0) {
        stats->throughput_req_per_sec =
            static_cast<double>(stats->success_iters) / stats->wall_time_sec;
        stats->throughput_mib_per_sec =
            (static_cast<double>(stats->bytes_total) / (1024.0 * 1024.0)) / stats->wall_time_sec;
    }

    return (stats->success_iters == 0) ? DOCA_ERROR_DRIVER : DOCA_SUCCESS;
}

void grpc_proto_print_benchmark_report(const GrpcOffloadBenchConfig *cfg,
                                       const GrpcOffloadBenchStats *stats)
{
    if (cfg == nullptr || stats == nullptr)
        return;

    std::printf("\n=== gRPC Offload Simple Benchmark ===\n");
    std::printf("workload: name_len=%u, scores_count=%u, warmup=%u, measure=%u, submit_batch=%u\n",
                cfg->name_len,
                cfg->scores_count,
                cfg->warmup_iters,
                cfg->measure_iters,
                cfg->submit_batch_size);

    std::printf("result: success=%u, failed=%u, wall=%.6f sec\n",
                stats->success_iters,
                stats->failed_iters,
                stats->wall_time_sec);

    std::printf("latency(us): avg=%.3f, p50=%.3f, p95=%.3f, p99=%.3f, min=%.3f, max=%.3f\n",
                stats->latency_us_avg,
                stats->latency_us_p50,
                stats->latency_us_p95,
                stats->latency_us_p99,
                stats->latency_us_min,
                stats->latency_us_max);

    std::printf("throughput: %.2f req/s, %.2f MiB/s\n",
                stats->throughput_req_per_sec,
                stats->throughput_mib_per_sec);
    std::printf("======================================\n");
}

#ifdef GRPC_OFFLOAD_BENCH_STANDALONE_MAIN
int main()
{
    std::printf("This benchmark needs an initialized GrpcProtoRuntime from your DOCA runtime path.\\n");
    return 0;
}
#endif
