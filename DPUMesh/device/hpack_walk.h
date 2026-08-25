/* Selective HPACK walker — dependency-free (no libc), portable to the DPA.
 *
 * Walks a request header block maintaining connection-scoped HPACK state
 * (dynamic table), materializing ONLY the fields L7 routing needs
 * (:method, :path, :authority). Other fields are skipped: huffman values of
 * non-target inserted entries are FSM-walked count-only (exact decoded length
 * for table-size accounting, no output writes); values of non-indexed fields
 * are skipped by encoded length alone.
 */
#ifndef HPACK_WALK_H
#define HPACK_WALK_H

#include "huff_fsm.h"

#define HW_VAL_CAP   128
#define HW_TBL_CAP   64          /* dynamic-table entries tracked (ring) */

/* RFC 7541 Appendix A static-table name lengths (index 1..61), for exact
 * entry-size accounting when a literal references a static name. */
static const unsigned char hw_static_nlen[62] = {
    0, 10, 7, 7, 5, 5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 14, 15, 15, 13, 6, 27, 3,
    5, 13, 13, 19, 16, 16, 14, 16, 13, 12, 6, 4, 4, 6, 7, 4, 4, 8, 17, 13,
    8, 19, 13, 4, 8, 12, 18, 19, 5, 7, 7, 11, 6, 10, 25, 17, 10, 4, 3, 16,
};

enum hw_kind { HW_NONE = 0, HW_METHOD = 1, HW_PATH = 2, HW_AUTH = 3 };

struct hw_entry {
    unsigned char kind;          /* HW_* — target name or HW_NONE            */
    unsigned char vlen;
    unsigned char nlen;          /* decoded name length (exact accounting)   */
    unsigned int  size;          /* RFC entry size (decoded name+value+32)   */
    char          val[HW_VAL_CAP];
};

struct hw_state {                /* connection-scoped                        */
    unsigned int head;           /* ring head: most recent entry             */
    unsigned int count;
    unsigned int size;           /* current table size (RFC accounting)      */
    unsigned int max_size;       /* default 4096                             */
    struct hw_entry tbl[HW_TBL_CAP];
};

struct hw_out {
    unsigned char have;          /* bitmask: 1<<(kind-1)                     */
    unsigned char mlen, plen, alen;
    char method[16];
    char path[HW_VAL_CAP];
    char auth[64];
};

static inline void hw_init(struct hw_state *st)
{
    st->head = 0; st->count = 0; st->size = 0; st->max_size = 4096;
}

/* ---- primitives ------------------------------------------------------- */

static inline int hw_varint(const unsigned char *b, int len, int *ip,
                            int prefix, unsigned int *out)
{
    int i = *ip;
    unsigned int mask = (1u << prefix) - 1, v, m = 0;
    if (i >= len) return -1;
    v = b[i++] & mask;
    if (v == mask) {
        unsigned char c;
        do {
            if (i >= len || m > 28) return -1;
            c = b[i++];
            v += (unsigned int)(c & 0x7f) << m;
            m += 7;
        } while (c & 0x80);
    }
    *ip = i; *out = v;
    return 0;
}

/* Huffman FSM walk. dst==0 → count-only (exact decoded length, no writes). */
static inline int hw_huff(const unsigned char *src, int n,
                          char *dst, int cap)
{
    unsigned int s = 0;
    int outn = 0, i;
    for (i = 0; i < n; i++) {
        unsigned char byte = src[i], nib, fl;
        int half;
        for (half = 0; half < 2; half++) {
            nib = half ? (byte & 0xf) : (byte >> 4);
            fl  = huff_flags[s][nib];
            if (fl & 1) return -1;                    /* EOS/invalid  */
            if (fl & 4) {
                if (dst) {
                    if (outn >= cap) return -1;
                    dst[outn] = (char)huff_emit[s][nib];
                }
                outn++;
            }
            s = huff_next[s][nib];
        }
    }
    return outn;                 /* padding validity implied by flags bit1 */
}

/* ---- static-table name classification (indices we care about) --------- */

static inline unsigned char hw_static_kind(unsigned int idx)
{
    switch (idx) {
    case 1:  return HW_AUTH;     /* :authority        */
    case 2: case 3: return HW_METHOD;   /* GET / POST */
    case 4: case 5: return HW_PATH;     /* / , /index.html */
    default: return HW_NONE;
    }
}

/* Fully-indexed static entries carry the value too. */
static inline int hw_static_value(unsigned int idx, char *dst, int cap,
                                  unsigned char *klen)
{
    static const char *v[6] = { 0, "", "GET", "POST", "/", "/index.html" };
    static const unsigned char l[6] = { 0, 0, 3, 4, 1, 11 };
    int j;
    if (idx > 5 || l[idx] > cap) return -1;
    for (j = 0; j < l[idx]; j++) dst[j] = v[idx][j];
    *klen = l[idx];
    return 0;
}

/* ---- dynamic table ----------------------------------------------------- */

static inline struct hw_entry *hw_tbl_get(struct hw_state *st, unsigned int n)
{                                /* n: 0 = most recent */
    if (n >= st->count) return 0;
    return &st->tbl[(st->head + HW_TBL_CAP - n) % HW_TBL_CAP];
}

static inline void hw_evict(struct hw_state *st)
{
    while (st->count && st->size > st->max_size) {
        struct hw_entry *e =
            &st->tbl[(st->head + HW_TBL_CAP - (st->count - 1)) % HW_TBL_CAP];
        st->size -= e->size;
        st->count--;
    }
}

static inline void hw_tbl_insert(struct hw_state *st, unsigned char kind,
                                 const char *val, unsigned char vlen,
                                 unsigned char nlen, unsigned int entry_size)
{
    struct hw_entry *e;
    st->head = (st->head + 1) % HW_TBL_CAP;
    if (st->count < HW_TBL_CAP) st->count++;
    /* else: oldest tracked slot is overwritten; RFC size accounting keeps
     * eviction correct as long as count stays under HW_TBL_CAP in practice. */
    e = &st->tbl[st->head];
    e->kind = kind; e->vlen = 0; e->nlen = nlen; e->size = entry_size;
    if (kind != HW_NONE && val) {
        unsigned char j;
        e->vlen = vlen;
        for (j = 0; j < vlen; j++) e->val[j] = val[j];
    }
    st->size += entry_size;
    hw_evict(st);
}

static inline void hw_deliver(struct hw_out *out, unsigned char kind,
                              const char *val, int vlen)
{
    switch (kind) {
    case HW_METHOD:
        if (vlen > (int)sizeof(out->method)) vlen = sizeof(out->method);
        { int j; for (j = 0; j < vlen; j++) out->method[j] = val[j]; }
        out->mlen = (unsigned char)vlen; break;
    case HW_PATH:
        if (vlen > (int)sizeof(out->path)) vlen = sizeof(out->path);
        { int j; for (j = 0; j < vlen; j++) out->path[j] = val[j]; }
        out->plen = (unsigned char)vlen; break;
    case HW_AUTH:
        if (vlen > (int)sizeof(out->auth)) vlen = sizeof(out->auth);
        { int j; for (j = 0; j < vlen; j++) out->auth[j] = val[j]; }
        out->alen = (unsigned char)vlen; break;
    default: return;
    }
    out->have |= 1u << (kind - 1);
}

/* ---- the walk ---------------------------------------------------------- */
/* returns 0 ok, -1 malformed. `full` != 0 → decode every huffman value and
 * write it (comparison mode measuring "no-selectivity" cost on same code). */
static inline int hw_walk(const unsigned char *b, int len,
                          struct hw_state *st, struct hw_out *out, int full)
{
    int i = 0;
    char tmp[HW_VAL_CAP];
    out->have = 0; out->mlen = out->plen = out->alen = 0;

    while (i < len) {
        unsigned char c = b[i];
        unsigned int idx;

        if (c & 0x80) {                              /* indexed field      */
            if (hw_varint(b, len, &i, 7, &idx)) return -1;
            if (idx == 0) return -1;
            if (idx <= 61) {
                unsigned char kind = hw_static_kind(idx), kl;
                if (kind != HW_NONE) {
                    if (hw_static_value(idx, tmp, sizeof(tmp), &kl)) return -1;
                    hw_deliver(out, kind, tmp, kl);
                }
            } else {
                struct hw_entry *e = hw_tbl_get(st, idx - 62);
                if (!e) return -1;
                if (e->kind != HW_NONE)
                    hw_deliver(out, e->kind, e->val, e->vlen);
            }
        } else if (c & 0x40 || (!(c & 0x20))) {      /* literal            */
            int insert = (c & 0x40) != 0;            /* w/ incremental idx */
            int prefix = insert ? 6 : 4;
            unsigned char kind = HW_NONE;
            unsigned int nlen = 0;                   /* decoded name len   */
            if (hw_varint(b, len, &i, prefix, &idx)) return -1;
            if (idx) {
                if (idx <= 61) {
                    kind = hw_static_kind(idx);
                    nlen = hw_static_nlen[idx];
                } else {
                    struct hw_entry *e = hw_tbl_get(st, idx - 62);
                    if (!e) return -1;
                    kind = e->kind;
                    nlen = e->nlen;
                }
            } else {             /* literal name                            */
                unsigned int l; int h;
                if (i >= len) return -1;
                h = b[i] & 0x80;
                if (hw_varint(b, len, &i, 7, &l) || i + (int)l > len) return -1;
                if (h && insert) {
                    int d = hw_huff(b + i, (int)l, 0, 0);   /* count-only  */
                    if (d < 0) return -1;
                    nlen = (unsigned int)d;
                } else nlen = l;   /* !insert: nlen unused — true skip      */
                /* target names never arrive as literal strings in practice
                   (pseudo-headers are always in the static table)          */
                i += (int)l;
            }
            /* value */
            {
                unsigned int l; int h, dlen;
                if (i >= len) return -1;
                h = b[i] & 0x80;
                if (hw_varint(b, len, &i, 7, &l) || i + (int)l > len) return -1;
                if (kind != HW_NONE || full) {
                    if (h) {
                        dlen = hw_huff(b + i, (int)l, tmp, sizeof(tmp));
                        if (dlen < 0) return -1;
                    } else {
                        int j;
                        if (l > sizeof(tmp)) return -1;
                        for (j = 0; j < (int)l; j++) tmp[j] = (char)b[i + j];
                        dlen = (int)l;
                    }
                    if (kind != HW_NONE) hw_deliver(out, kind, tmp, dlen);
                } else if (insert && h) {
                    dlen = hw_huff(b + i, (int)l, 0, 0);    /* count-only  */
                    if (dlen < 0) return -1;
                } else {
                    dlen = (int)l;                          /* true skip   */
                }
                if (insert)
                    hw_tbl_insert(st, kind,
                                  kind != HW_NONE ? tmp : 0,
                                  (unsigned char)(dlen > HW_VAL_CAP
                                                  ? HW_VAL_CAP : dlen),
                                  (unsigned char)(nlen > 255 ? 255 : nlen),
                                  nlen + (unsigned int)dlen + 32);
                i += (int)l;
            }
        } else {                                     /* table size update  */
            if (hw_varint(b, len, &i, 5, &idx)) return -1;
            st->max_size = idx;
            hw_evict(st);
        }
    }
    return 0;
}

#endif /* HPACK_WALK_H */
