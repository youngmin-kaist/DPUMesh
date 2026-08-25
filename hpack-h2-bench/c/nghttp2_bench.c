/* nghttp2 h2-termination round-trip bench — the exact analog of
 * hyper_bench.rs: a real nghttp2 SERVER session + real nghttp2 CLIENT
 * session wired memory-to-memory (mem_send -> mem_recv, no sockets), same
 * DeathStarBench gRPC request (POST /search.Search/Nearby, srv-search,
 * content-type/user-agent/te + fresh uber-trace-id per request), same 10B
 * response body, one long-lived connection, m streams in flight.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <nghttp2/nghttp2.h>

#define N_TIDS 256
static char tids[N_TIDS][80];

/* same deterministic jaeger-ids as hyper_bench.rs */
static unsigned long long mix(unsigned long long x)
{
    unsigned long long v = x ^ 0x9e3779b97f4a7c15ULL;
    v *= 0xbf58476d1ce4e5b9ULL;
    v ^= v >> 27;
    return v * 0x94d049bb133111ebULL;
}

static void gen_tids(void)
{
    for (int i = 0; i < N_TIDS; i++)
        snprintf(tids[i], sizeof(tids[i]), "%016llx:%016llx:%016llx:%d",
                 mix(i), mix(i * 3 + 1), mix(i * 7 + 2), i & 1);
}

struct ctx {
    nghttp2_session *cl, *sv;
    long total, submitted, done, inflight, m;
    long sv_header_fields;      /* server-side decoded fields (sanity) */
    long cl_header_fields;
};

#define NV(NAME, VALUE) \
    { (uint8_t *)(NAME), (uint8_t *)(VALUE), sizeof(NAME) - 1, \
      sizeof(VALUE) - 1, NGHTTP2_NV_FLAG_NONE }
#define NVL(NAME, VALUE, VLEN) \
    { (uint8_t *)(NAME), (uint8_t *)(VALUE), sizeof(NAME) - 1, \
      (VLEN), NGHTTP2_NV_FLAG_NONE }

static void submit_one(struct ctx *c)
{
    const char *tid = tids[c->submitted % N_TIDS];
    nghttp2_nv nva[] = {
        NV(":method", "POST"),
        NV(":scheme", "http"),
        NV(":path", "/search.Search/Nearby"),
        NV(":authority", "srv-search"),
        NV("content-type", "application/grpc"),
        NV("user-agent", "grpc-go/1.56.3"),
        NV("te", "trailers"),
        NVL("uber-trace-id", tid, strlen(tid)),
    };
    int rv = nghttp2_submit_request(c->cl, NULL, nva, 8, NULL, NULL);
    if (rv < 0) { fprintf(stderr, "submit_request: %s\n", nghttp2_strerror(rv)); exit(1); }
    c->submitted++; c->inflight++;
}

/* ---- server side ---- */

static const uint8_t RESP_BODY[10] = "\0\0\0\0\x05hello";

static ssize_t body_read(nghttp2_session *s, int32_t sid, uint8_t *buf,
                         size_t len, uint32_t *flags, nghttp2_data_source *src,
                         void *ud)
{
    (void)s; (void)sid; (void)src; (void)ud;
    size_t n = sizeof(RESP_BODY) < len ? sizeof(RESP_BODY) : len;
    memcpy(buf, RESP_BODY, n);
    *flags = NGHTTP2_DATA_FLAG_EOF;
    return (ssize_t)n;
}

static int sv_on_frame_recv(nghttp2_session *s, const nghttp2_frame *f, void *ud)
{
    struct ctx *c = ud;
    (void)c;
    if (f->hd.type == NGHTTP2_HEADERS &&
        f->headers.cat == NGHTTP2_HCAT_REQUEST &&
        (f->hd.flags & NGHTTP2_FLAG_END_STREAM)) {
        nghttp2_nv nva[] = {
            NV(":status", "200"),
            NV("content-type", "application/grpc"),
        };
        nghttp2_data_provider dp = { .source = { .ptr = 0 }, .read_callback = body_read };
        int rv = nghttp2_submit_response(s, f->hd.stream_id, nva, 2, &dp);
        if (rv != 0) { fprintf(stderr, "submit_response: %s\n", nghttp2_strerror(rv)); exit(1); }
    }
    return 0;
}

static int sv_on_header(nghttp2_session *s, const nghttp2_frame *f,
                        const uint8_t *name, size_t nlen,
                        const uint8_t *value, size_t vlen, uint8_t flags, void *ud)
{
    struct ctx *c = ud;
    (void)s; (void)f; (void)name; (void)nlen; (void)value; (void)vlen; (void)flags;
    c->sv_header_fields++;      /* touch each decoded field (typical app) */
    return 0;
}

/* ---- client side ---- */

static int cl_on_header(nghttp2_session *s, const nghttp2_frame *f,
                        const uint8_t *name, size_t nlen,
                        const uint8_t *value, size_t vlen, uint8_t flags, void *ud)
{
    struct ctx *c = ud;
    (void)s; (void)f; (void)name; (void)nlen; (void)value; (void)vlen; (void)flags;
    c->cl_header_fields++;
    return 0;
}

static int cl_on_stream_close(nghttp2_session *s, int32_t sid, uint32_t ec, void *ud)
{
    struct ctx *c = ud;
    (void)s; (void)sid;
    if (ec != 0) { fprintf(stderr, "stream closed err=%u\n", ec); exit(1); }
    c->done++; c->inflight--;
    while (c->inflight < c->m && c->submitted < c->total)
        submit_one(c);
    return 0;
}

/* ---- pump: client <-> server, memory to memory ---- */

static void pump(struct ctx *c)
{
    const uint8_t *data;
    ssize_t n;
    int progress = 1;
    while (progress) {
        progress = 0;
        while ((n = nghttp2_session_mem_send(c->cl, &data)) > 0) {
            ssize_t r = nghttp2_session_mem_recv(c->sv, data, (size_t)n);
            if (r != n) { fprintf(stderr, "sv recv %zd\n", r); exit(1); }
            progress = 1;
        }
        while ((n = nghttp2_session_mem_send(c->sv, &data)) > 0) {
            ssize_t r = nghttp2_session_mem_recv(c->cl, data, (size_t)n);
            if (r != n) { fprintf(stderr, "cl recv %zd\n", r); exit(1); }
            progress = 1;
        }
    }
}

static double now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e9 + ts.tv_nsec;
}

int main(int argc, char **argv)
{
    long iters = argc > 1 ? atol(argv[1]) : 100000;
    long m = argc > 2 ? atol(argv[2]) : 1;
    struct ctx c = {0};
    c.m = m;
    gen_tids();

    nghttp2_session_callbacks *cb;
    nghttp2_session_callbacks_new(&cb);
    nghttp2_session_callbacks_set_on_header_callback(cb, cl_on_header);
    nghttp2_session_callbacks_set_on_stream_close_callback(cb, cl_on_stream_close);
    nghttp2_session_client_new(&c.cl, cb, &c);
    nghttp2_session_callbacks_del(cb);

    nghttp2_session_callbacks_new(&cb);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cb, sv_on_frame_recv);
    nghttp2_session_callbacks_set_on_header_callback(cb, sv_on_header);
    nghttp2_session_server_new(&c.sv, cb, &c);
    nghttp2_session_callbacks_del(cb);

    nghttp2_settings_entry st[] = {
        { NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 1024 },
    };
    nghttp2_submit_settings(c.cl, NGHTTP2_FLAG_NONE, st, 1);
    nghttp2_submit_settings(c.sv, NGHTTP2_FLAG_NONE, st, 1);

    /* warmup: 256 requests to settle dynamic tables/settings */
    c.total = 256; c.m = m;
    while (c.inflight < c.m && c.submitted < c.total) submit_one(&c);
    pump(&c);
    if (c.done != 256 || c.sv_header_fields != 256 * 8) {
        fprintf(stderr, "warmup FAIL done=%ld sv_fields=%ld\n", c.done, c.sv_header_fields);
        return 1;
    }

    /* timed run */
    c.total = 256 + iters;
    double t0 = now_ns();
    while (c.inflight < c.m && c.submitted < c.total) submit_one(&c);
    pump(&c);
    double t1 = now_ns();
    if (c.done != c.total) { fprintf(stderr, "FAIL done=%ld\n", c.done); return 1; }

    printf("nghttp2 h2 round-trip m=%-3ld: %8.1f ns/req  (%.0f req/s, n=%ld) "
           "sv_fields/req=%.1f cl_fields/req=%.1f\n",
           m, (t1 - t0) / iters, iters / ((t1 - t0) / 1e9), iters,
           (double)c.sv_header_fields / c.done, (double)c.cl_header_fields / c.done);
    return 0;
}
