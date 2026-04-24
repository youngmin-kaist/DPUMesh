#pragma once

#include <doca_error.h>

#include <cstddef>
#include <cstdint>

#include "host_agent.h"

struct GrpcOffloadBenchConfig {
    uint32_t warmup_iters;
    uint32_t measure_iters;
    uint32_t submit_batch_size;
    uint32_t name_len;
    uint32_t scores_count;
    uint32_t score_seed;
};

struct GrpcOffloadBenchStats {
    uint32_t total_iters;
    uint32_t success_iters;
    uint32_t failed_iters;

    uint64_t bytes_total;
    double wall_time_sec;

    double latency_us_avg;
    double latency_us_p50;
    double latency_us_p95;
    double latency_us_p99;
    double latency_us_min;
    double latency_us_max;

    double throughput_req_per_sec;
    double throughput_mib_per_sec;
};

doca_error_t grpc_proto_run_simple_benchmark(GrpcProtoRuntime *rt,
                                              const GrpcOffloadBenchConfig *cfg,
                                              GrpcOffloadBenchStats *stats);

void grpc_proto_print_benchmark_report(const GrpcOffloadBenchConfig *cfg,
                                       const GrpcOffloadBenchStats *stats);
