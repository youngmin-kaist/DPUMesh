/* ARM bench: DeathStarBench gRPC headers —
 * selective walk vs FULL decode+materialize vs decode+re-encode (termination
 * baseline). Blocks cycle through 256 pre-generated steady blocks so the
 * dynamic table churns exactly as a live grpc-go connection would.
 */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "hpack_walk.h"
#include "dsb_blocks.h"
#include "hpack_term.h"

static double now_ns(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1e9 + ts.tv_nsec;
}

#define STEADY(i) (dsb_steady_data + dsb_steady_off[i]), \
                  (dsb_steady_off[(i) + 1] - dsb_steady_off[(i)])
#define NI(i)     (dsb_ni_steady_data + dsb_ni_steady_off[i]), \
                  (dsb_ni_steady_off[(i) + 1] - dsb_ni_steady_off[(i)])

int main(int argc, char **argv)
{
    long iters = argc > 1 ? atol(argv[1]) : 2000000;
    static struct hw_state ws;
    static struct hw_out out;
    static struct ht_table dec, enc, dec2;
    static struct ht_field fields[HT_MAX_FIELDS], fields2[HT_MAX_FIELDS];
    static unsigned char blk[512];
    double t0, t1;
    long i;
    int rc, nf, nf2, el;
    volatile int sink = 0;

    /* ---------- correctness ---------- */
    hw_init(&ws);
    rc = hw_walk(dsb_first, sizeof(dsb_first), &ws, &out, 0);
    printf("walk first : rc=%d have=%x method=%.*s path=%.*s auth=%.*s\n",
           rc, out.have, out.mlen, out.method, out.plen, out.path,
           out.alen, out.auth);
    for (i = 0; i < 2 * DSB_STEADY_N; i++) {
        if (i % DSB_STEADY_N == 0) {          /* connection replay boundary */
            hw_init(&ws);
            hw_walk(dsb_first, sizeof(dsb_first), &ws, &out, 0);
        }
        rc = hw_walk(STEADY(i % DSB_STEADY_N), &ws, &out, 0);
        if (rc || out.have != 7) { printf("walk FAIL at %ld rc=%d have=%x\n", i, rc, out.have); return 1; }
    }
    printf("walk steady: all %d blocks ok, path=%.*s auth=%.*s\n",
           DSB_STEADY_N, out.plen, out.path, out.alen, out.auth);

    /* roundtrip: decode -> encode -> decode(with fresh table) -> compare */
    ht_init(&dec); ht_init(&enc); ht_init(&dec2);
    rc = ht_decode(dsb_first, sizeof(dsb_first), &dec, fields, &nf);
    el = ht_encode(fields, nf, &enc, blk, sizeof(blk));
    rc |= ht_decode(blk, el, &dec2, fields2, &nf2);
    if (rc || nf != nf2 || memcmp(fields, fields2, sizeof(fields))) {
        printf("roundtrip FAIL first rc=%d nf=%d nf2=%d\n", rc, nf, nf2); return 1;
    }
    for (i = 0; i < DSB_STEADY_N; i++) {
        rc = ht_decode(STEADY(i), &dec, fields, &nf);
        el = ht_encode(fields, nf, &enc, blk, sizeof(blk));
        rc |= ht_decode(blk, el, &dec2, fields2, &nf2);
        if (rc || el <= 0 || nf != nf2 || memcmp(fields, fields2, sizeof(fields))) {
            printf("roundtrip FAIL at %ld rc=%d el=%d\n", i, rc, el); return 1;
        }
    }
    printf("roundtrip  : all %d blocks ok (decode->reencode->decode equal), "
           "nf=%d reenc_len(last)=%d\n", DSB_STEADY_N, nf, el);

    /* ---------- timings ---------- */
    /* selective walk */
    {
    long fails = 0;
    t0 = now_ns();
    for (i = 0; i < iters; i++) {
        if (i % DSB_STEADY_N == 0) {
            hw_init(&ws);
            hw_walk(dsb_first, sizeof(dsb_first), &ws, &out, 0);
        }
        fails += hw_walk(STEADY(i % DSB_STEADY_N), &ws, &out, 0) != 0;
        sink += out.plen;
    }
    t1 = now_ns();
    printf("SELECTIVE walk      : %7.1f ns/block (fails=%ld)\n", (t1 - t0) / iters, fails);
    }

    /* full decode + materialize */
    {
    long fails = 0;
    t0 = now_ns();
    for (i = 0; i < iters; i++) {
        if (i % DSB_STEADY_N == 0) {
            ht_init(&dec);
            ht_decode(dsb_first, sizeof(dsb_first), &dec, fields, &nf);
        }
        fails += ht_decode(STEADY(i % DSB_STEADY_N), &dec, fields, &nf) != 0;
        sink += nf;
    }
    t1 = now_ns();
    printf("FULL decode         : %7.1f ns/block (fails=%ld)\n", (t1 - t0) / iters, fails);
    }

    /* termination baseline: decode + re-encode */
    {
    long fails = 0;
    t0 = now_ns();
    for (i = 0; i < iters; i++) {
        if (i % DSB_STEADY_N == 0) {
            ht_init(&dec); ht_init(&enc);
            ht_decode(dsb_first, sizeof(dsb_first), &dec, fields, &nf);
            ht_encode(fields, nf, &enc, blk, sizeof(blk));
        }
        fails += ht_decode(STEADY(i % DSB_STEADY_N), &dec, fields, &nf) != 0;
        el = ht_encode(fields, nf, &enc, blk, sizeof(blk));
        fails += el <= 0;
        sink += el;
    }
    t1 = now_ns();
    printf("DECODE + RE-ENCODE  : %7.1f ns/block (fails=%ld)\n", (t1 - t0) / iters, fails);
    }

    /* ================= NI variant: trace-id WITHOUT indexing ============ */
    hw_init(&ws);
    hw_walk(dsb_ni_first, sizeof(dsb_ni_first), &ws, &out, 0);
    for (i = 0; i < 2 * DSB_STEADY_N; i++) {
        if (i % DSB_STEADY_N == 0) {
            hw_init(&ws);
            hw_walk(dsb_ni_first, sizeof(dsb_ni_first), &ws, &out, 0);
        }
        rc = hw_walk(NI(i % DSB_STEADY_N), &ws, &out, 0);
        if (rc || out.have != 7) { printf("NI walk FAIL at %ld rc=%d have=%x\n", i, rc, out.have); return 1; }
    }
    ht_init(&dec); ht_init(&enc); ht_init(&dec2);
    ht_decode(dsb_ni_first, sizeof(dsb_ni_first), &dec, fields, &nf);
    el = ht_encode(fields, nf, &enc, blk, sizeof(blk));
    if (ht_decode(blk, el, &dec2, fields2, &nf2)) {
        printf("NI first roundtrip FAIL\n"); return 1;
    }
    for (i = 0; i < DSB_STEADY_N; i++) {
        rc = ht_decode(NI(i), &dec, fields, &nf);
        el = ht_encode(fields, nf, &enc, blk, sizeof(blk));
        rc |= ht_decode(blk, el, &dec2, fields2, &nf2);
        if (rc || el <= 0 || nf != nf2 || memcmp(fields, fields2, sizeof(fields))) {
            printf("NI roundtrip FAIL at %ld rc=%d el=%d\n", i, rc, el); return 1;
        }
    }
    printf("NI correctness: walk + roundtrip ok (reenc_len=%d)\n", el);

    {
    long fails = 0;
    t0 = now_ns();
    for (i = 0; i < iters; i++) {
        if (i % DSB_STEADY_N == 0) {
            hw_init(&ws);
            hw_walk(dsb_ni_first, sizeof(dsb_ni_first), &ws, &out, 0);
        }
        fails += hw_walk(NI(i % DSB_STEADY_N), &ws, &out, 0) != 0;
        sink += out.plen;
    }
    t1 = now_ns();
    printf("NI SELECTIVE walk   : %7.1f ns/block (fails=%ld)\n", (t1 - t0) / iters, fails);
    }
    {
    long fails = 0;
    t0 = now_ns();
    for (i = 0; i < iters; i++) {
        if (i % DSB_STEADY_N == 0) {
            ht_init(&dec);
            ht_decode(dsb_ni_first, sizeof(dsb_ni_first), &dec, fields, &nf);
        }
        fails += ht_decode(NI(i % DSB_STEADY_N), &dec, fields, &nf) != 0;
        sink += nf;
    }
    t1 = now_ns();
    printf("NI FULL decode      : %7.1f ns/block (fails=%ld)\n", (t1 - t0) / iters, fails);
    }
    {
    long fails = 0;
    t0 = now_ns();
    for (i = 0; i < iters; i++) {
        if (i % DSB_STEADY_N == 0) {
            ht_init(&dec); ht_init(&enc);
            ht_decode(dsb_ni_first, sizeof(dsb_ni_first), &dec, fields, &nf);
            ht_encode(fields, nf, &enc, blk, sizeof(blk));
        }
        fails += ht_decode(NI(i % DSB_STEADY_N), &dec, fields, &nf) != 0;
        el = ht_encode(fields, nf, &enc, blk, sizeof(blk));
        fails += el <= 0;
        sink += el;
    }
    t1 = now_ns();
    printf("NI DEC + RE-ENCODE  : %7.1f ns/block (fails=%ld)\n", (t1 - t0) / iters, fails);
    }

    /* first block (per-connection setup) */
    t0 = now_ns();
    for (i = 0; i < iters / 10; i++) {
        hw_init(&ws);
        hw_walk(dsb_first, sizeof(dsb_first), &ws, &out, 0);
        sink += out.plen;
    }
    t1 = now_ns();
    printf("first blk selective : %7.1f ns\n", (t1 - t0) / (iters / 10));

    return sink == -1;
}
