#include "relay3.hpp"

#include <cstdio>
#include <cstring>

#include "router.hpp"

namespace dmesh {

// ---------------------------------------------------------------------------
// HpTable
// ---------------------------------------------------------------------------

uint64_t HpTable::insert(HpEntry &&e) {
    e.serial = ++serial_;
    size_ += e.size;
    entries_.push_front(std::move(e));
    const uint64_t abs = inserts_++;
    evict();
    return abs;
}

HpEntry *HpTable::get(uint32_t idx) {
    if (idx < 62) return nullptr;
    const size_t off = idx - 62;
    return off < entries_.size() ? &entries_[off] : nullptr;
}

void HpTable::evict() {
    while (!entries_.empty() && size_ > max_) {
        size_ -= entries_.back().size;
        entries_.pop_back();
    }
}

// ---------------------------------------------------------------------------
// transcoder
// ---------------------------------------------------------------------------

static inline void wr_int(std::vector<uint8_t> &out, uint8_t first_bits, int prefix, uint32_t v) {
    const uint32_t m = (1u << prefix) - 1;
    if (v < m) {
        out.push_back(static_cast<uint8_t>(first_bits | v));
        return;
    }
    out.push_back(static_cast<uint8_t>(first_bits | m));
    v -= m;
    while (v >= 128) {
        out.push_back(static_cast<uint8_t>((v & 127) | 128));
        v >>= 7;
    }
    out.push_back(static_cast<uint8_t>(v));
}

static inline void wr_str(std::vector<uint8_t> &out, bool huff, const uint8_t *s, size_t n) {
    wr_int(out, huff ? 0x80 : 0x00, 7, static_cast<uint32_t>(n));
    out.insert(out.end(), s, s + n);
}

static uint8_t classify_name(const uint8_t *name, size_t n) {
    if (n == 5 && std::memcmp(name, ":path", 5) == 0) return HW_PATH;
    if (n == 7 && std::memcmp(name, ":method", 7) == 0) return HW_METHOD;
    if (n == 10 && std::memcmp(name, ":authority", 10) == 0) return HW_AUTH;
    return HW_NONE;
}

static void deliver(PolicyFields *pf, uint8_t kind, const char *v, size_t n) {
    if (pf == nullptr || kind == HW_NONE) return;
    std::string &dst = kind == HW_PATH ? pf->path : kind == HW_METHOD ? pf->method : pf->authority;
    dst.assign(v, n);
    pf->have |= 1u << (kind - 1);
}

// Emit `e`'s name as a name reference: static index if it has one, else the
// literal (encoded) bytes.
static void wr_name(std::vector<uint8_t> &out, uint8_t first_bits, int prefix, const HpEntry &e) {
    if (e.name_static != 0) {
        wr_int(out, first_bits, prefix, e.name_static);
    } else {
        wr_int(out, first_bits, prefix, 0);
        wr_str(out, e.name_huff, e.name_enc.data(), e.name_enc.size());
    }
}

// Resolve a dynamic mirror entry against the encoder table: hit → index, miss → 0.
static uint32_t enc_lookup(HpTable &enc, HpEntry &e, uint32_t mirror_tid) {
    for (const HpEntry::Cache &c : e.cache) {
        if (c.tid != enc.tid()) continue;
        const uint32_t j = enc.index_of(c.abs);
        if (j == 0) continue;
        HpEntry *t = enc.get(j);
        if (t != nullptr && t->src_tid == mirror_tid && t->src_serial == e.serial) return j;
    }
    return 0;
}

static void enc_insert_from(HpTable &enc, HpEntry &e, uint32_t mirror_tid) {
    HpEntry t;
    t.name_static = e.name_static;
    t.name_enc = e.name_enc;
    t.name_huff = e.name_huff;
    t.name_dec_len = e.name_dec_len;
    t.val_enc = e.val_enc;
    t.val_huff = e.val_huff;
    t.val_dec_len = e.val_dec_len;
    t.size = e.size;
    t.src_tid = mirror_tid;
    t.src_serial = e.serial;
    const uint64_t abs = enc.insert(std::move(t));
    HpEntry::Cache &c = e.cache[e.cache_rr++ & 3];
    c.tid = enc.tid();
    c.abs = abs;
}

// Real implementation with an optional encoder (nullptr = mirror-only walk).
bool transcode_impl(const uint8_t *b, int len, HpTable &mirror, HpTable *enc,
                           std::vector<uint8_t> &out, PolicyFields *pf, TranscodeStats &ts) {
    int i = 0;
    char tmp[HW_VAL_CAP];
    if (pf) { pf->have = 0; }
    ts.in_bytes += static_cast<uint64_t>(len);
    while (i < len) {
        const uint8_t c = b[i];
        unsigned int idx = 0;
        ts.fields++;
        if (c & 0x80) { // ---- indexed
            if (hw_varint(b, len, &i, 7, &idx) || idx == 0) return false;
            if (idx <= 61) {
                ts.static_idx++;
                const uint8_t kind = hw_static_kind(idx);
                if (kind != HW_NONE && pf) {
                    unsigned char kl = 0;
                    if (hw_static_value(idx, tmp, sizeof(tmp), &kl)) return false;
                    deliver(pf, kind, tmp, kl);
                }
                if (enc) wr_int(out, 0x80, 7, idx);
                continue;
            }
            HpEntry *e = mirror.get(idx);
            if (e == nullptr) return false;
            if (e->kind != HW_NONE && pf) deliver(pf, e->kind, e->val_dec.data(), e->val_dec.size());
            if (enc) {
                const uint32_t j = enc_lookup(*enc, *e, mirror.tid());
                if (j != 0) {
                    ts.dyn_hit++;
                    wr_int(out, 0x80, 7, j);
                } else {
                    ts.dyn_miss++;
                    wr_name(out, 0x40, 6, *e);
                    wr_str(out, e->val_huff, e->val_enc.data(), e->val_enc.size());
                    enc_insert_from(*enc, *e, mirror.tid());
                }
            }
            continue;
        }
        if ((c & 0xe0) == 0x20) { // ---- dynamic table size update (peer-local; never forwarded)
            if (hw_varint(b, len, &i, 5, &idx)) return false;
            mirror.set_max(idx);
            ts.size_updates++;
            continue;
        }
        // ---- literal: 01 (insert), 0000 (no index), 0001 (never index)
        const bool insert = (c & 0x40) != 0;
        const bool never = !insert && (c & 0x10) != 0;
        const int prefix = insert ? 6 : 4;
        if (hw_varint(b, len, &i, prefix, &idx)) return false;
        HpEntry m; // the field as the peer encoded it
        uint8_t kind = HW_NONE;
        if (idx != 0) {
            if (idx <= 61) {
                m.name_static = static_cast<uint8_t>(idx);
                m.name_dec_len = hw_static_nlen[idx];
                kind = hw_static_kind(idx);
            } else {
                HpEntry *ne = mirror.get(idx);
                if (ne == nullptr) return false;
                m.name_static = ne->name_static;
                m.name_enc = ne->name_enc;
                m.name_huff = ne->name_huff;
                m.name_dec_len = ne->name_dec_len;
                kind = ne->kind;
            }
        } else {
            if (i >= len) return false;
            const int h = b[i] & 0x80;
            unsigned int l = 0;
            if (hw_varint(b, len, &i, 7, &l) || i + static_cast<int>(l) > len) return false;
            m.name_enc.assign(b + i, b + i + l);
            m.name_huff = h != 0;
            if (h) {
                const int d = hw_huff(b + i, static_cast<int>(l), tmp, sizeof(tmp));
                if (d < 0) return false;
                m.name_dec_len = static_cast<uint32_t>(d);
                kind = classify_name(reinterpret_cast<const uint8_t *>(tmp), static_cast<size_t>(d));
            } else {
                m.name_dec_len = l;
                kind = classify_name(b + i, l);
            }
            i += static_cast<int>(l);
        }
        // value
        {
            if (i >= len) return false;
            const int h = b[i] & 0x80;
            unsigned int l = 0;
            if (hw_varint(b, len, &i, 7, &l) || i + static_cast<int>(l) > len) return false;
            m.val_huff = h != 0;
            if (kind != HW_NONE) {
                int dlen;
                if (h) {
                    dlen = hw_huff(b + i, static_cast<int>(l), tmp, sizeof(tmp));
                    if (dlen < 0) return false;
                } else {
                    if (l > sizeof(tmp)) return false;
                    std::memcpy(tmp, b + i, l);
                    dlen = static_cast<int>(l);
                }
                m.val_dec_len = static_cast<uint32_t>(dlen);
                if (pf) deliver(pf, kind, tmp, static_cast<size_t>(dlen));
                if (insert) m.val_dec.assign(tmp, static_cast<size_t>(dlen));
            } else if (insert && h) {
                const int dlen = hw_huff(b + i, static_cast<int>(l), nullptr, 0); // count-only
                if (dlen < 0) return false;
                m.val_dec_len = static_cast<uint32_t>(dlen);
            } else {
                m.val_dec_len = l;
            }
            m.kind = kind;
            m.size = m.name_dec_len + m.val_dec_len + 32;
            // output: same representation class, name re-resolved, value bytes verbatim
            if (enc) {
                const uint8_t first = insert ? 0x40 : (never ? 0x10 : 0x00);
                wr_name(out, first, prefix, m);
                wr_str(out, m.val_huff, b + i, l);
                if (insert) ts.lit_insert++; else if (never) ts.never_indexed++; else ts.lit_noindex++;
            }
            if (insert) {
                m.val_enc.assign(b + i, b + i + l);
                if (enc) {
                    // mirror entry gets a serial on insert; encoder copy references it
                    HpEntry copy = m; // for the encoder
                    mirror.insert(std::move(m));
                    // An entry larger than the table max is evicted on insert
                    // (RFC 7541 §4.4) — legal, so the head may not be ours.
                    HpEntry *me = mirror.get(62);
                    if (me != nullptr) {
                        copy.src_tid = mirror.tid();
                        copy.src_serial = me->serial;
                        const uint64_t abs = enc->insert(std::move(copy));
                        HpEntry::Cache &cc = me->cache[me->cache_rr++ & 3];
                        cc.tid = enc->tid();
                        cc.abs = abs;
                    } else {
                        enc->insert(std::move(copy));
                    }
                } else {
                    mirror.insert(std::move(m));
                }
            }
            i += static_cast<int>(l);
        }
    }
    ts.out_bytes += out.size();
    return true;
}

// ---------------------------------------------------------------------------
// H2Mux — frame plumbing (same rules as relay v2, with N:M stream mapping)
// ---------------------------------------------------------------------------

static constexpr uint8_t kData = 0x0, kHeaders = 0x1, kPriority = 0x2, kRstStream = 0x3,
                         kSettings = 0x4, kPushPromise = 0x5, kPing = 0x6, kGoaway = 0x7,
                         kWindowUpdate = 0x8, kContinuation = 0x9;
static constexpr uint8_t kFlagEndStream = 0x1, kFlagAck = 0x1, kFlagEndHeaders = 0x4,
                         kFlagPadded = 0x8, kFlagPriority = 0x20;
static constexpr uint16_t kSetTableSize = 1, kSetMaxStreams = 3, kSetInitWindow = 4, kSetMaxFrame = 5;
static constexpr uint32_t kErrProtocol = 0x1, kErrCancel = 0x8, kErrRefused = 0x7;
static const uint8_t kPreface[24] = {'P', 'R', 'I', ' ', '*', ' ', 'H', 'T', 'T', 'P', '/', '2',
                                     '.', '0', '\r', '\n', '\r', '\n', 'S', 'M', '\r', '\n', '\r', '\n'};
static inline uint32_t rd24(const uint8_t *p) { return (uint32_t(p[0]) << 16) | (uint32_t(p[1]) << 8) | p[2]; }
static inline uint32_t rd32(const uint8_t *p) { return (uint32_t(p[0]) << 24) | (uint32_t(p[1]) << 16) | (uint32_t(p[2]) << 8) | p[3]; }
static inline void wr32(uint8_t *p, uint32_t v) { p[0] = uint8_t(v >> 24); p[1] = uint8_t(v >> 16); p[2] = uint8_t(v >> 8); p[3] = uint8_t(v); }
static inline void wr_hdr(uint8_t *h, uint32_t len, uint8_t type, uint8_t flags, uint32_t sid) {
    h[0] = uint8_t(len >> 16); h[1] = uint8_t(len >> 8); h[2] = uint8_t(len); h[3] = type; h[4] = flags; wr32(h + 5, sid & 0x7fffffffu);
}

H2Mux::~H2Mux() {
    for (auto &e : legs_) {
        for (auto &s : e.second->streams) {
            if (e.second->is_client) delete s.second; // owned via the client leg
        }
        delete e.second;
    }
}

std::string H2Mux::tstats_line() const {
    char buf[320];
    std::snprintf(buf, sizeof(buf),
                  "transcode fields=%llu static=%llu dyn_hit=%llu dyn_miss=%llu lit_insert=%llu lit_noidx=%llu never=%llu "
                  "size_upd=%llu in_bytes=%llu out_bytes=%llu",
                  (unsigned long long)ts_.fields, (unsigned long long)ts_.static_idx, (unsigned long long)ts_.dyn_hit,
                  (unsigned long long)ts_.dyn_miss, (unsigned long long)ts_.lit_insert, (unsigned long long)ts_.lit_noindex,
                  (unsigned long long)ts_.never_indexed, (unsigned long long)ts_.size_updates,
                  (unsigned long long)ts_.in_bytes, (unsigned long long)ts_.out_bytes);
    return buf;
}

void H2Mux::add_leg(RelayChannel *ch) {
    MuxLeg *leg = new MuxLeg(next_tid_++);
    leg->ch = ch;
    leg->is_client = !ch->is_backend();
    leg->key = ch->key();
    leg->tx_enc.set_max(kOurTableSize);
    legs_[ch->slot()] = leg;
    if (leg->is_client) {
        leg->preface_left = sizeof(kPreface);
        send_settings(*leg);
    } else {
        emit_raw(*leg, kPreface, sizeof(kPreface));
        send_settings(*leg);
        backends_.push_back(leg);
    }
    ch->pair(ch); // mark used (no peer semantics in mux mode)
}

void H2Mux::remove_leg(int slot) {
    auto it = legs_.find(slot);
    if (it == legs_.end()) return;
    MuxLeg *leg = it->second;
    if (leg->is_client) {
        for (auto &s : leg->streams) {
            MuxStream *st = s.second;
            if (st->b != nullptr) {
                st->b->streams.erase(st->b_sid);
                send_rst(*st->b, uint32_t(st->b_sid), kErrCancel);
            }
            delete st;
        }
    } else {
        for (auto &s : leg->streams) {
            MuxStream *st = s.second;
            st->b = nullptr;
            if (st->c != nullptr) {
                send_rst(*st->c, uint32_t(st->c_sid), kErrRefused);
                st->c->streams.erase(st->c_sid);
                stats_.streams_closed++;
                delete st;
            }
        }
        for (size_t k = 0; k < backends_.size(); k++) {
            if (backends_[k] == leg) { backends_.erase(backends_.begin() + k); break; }
        }
    }
    legs_.erase(it);
    delete leg;
}

MuxLeg *H2Mux::pick_backend(uint64_t key) {
    if (backends_.empty()) return nullptr;
    for (size_t n = 0; n < backends_.size(); n++) {
        MuxLeg *b = backends_[(rr_++) % backends_.size()];
        if (!b->goaway_recv && (b->key == key || (have_default_ && b->key == default_key_))) return b;
    }
    return nullptr;
}

void H2Mux::send_settings(MuxLeg &to) {
    uint8_t p[24];
    const struct { uint16_t id; uint32_t v; } iv[4] = {{kSetTableSize, kOurTableSize}, {kSetMaxStreams, kOurMaxStreams},
                                                     {kSetInitWindow, kOurInitWindow}, {kSetMaxFrame, kOurMaxFrame}};
    for (int i = 0; i < 4; i++) { p[i * 6] = uint8_t(iv[i].id >> 8); p[i * 6 + 1] = uint8_t(iv[i].id); wr32(p + i * 6 + 2, iv[i].v); }
    emit(to, kSettings, 0, 0, p, sizeof(p));
    send_window_update(to, 0, kOurConnWindow - 65535);
}
void H2Mux::send_window_update(MuxLeg &to, uint32_t sid, uint32_t inc) { uint8_t p[4]; wr32(p, inc & 0x7fffffffu); emit(to, kWindowUpdate, 0, sid, p, 4); stats_.window_updates_out++; }
void H2Mux::send_rst(MuxLeg &to, uint32_t sid, uint32_t code) { uint8_t p[4]; wr32(p, code); emit(to, kRstStream, 0, sid, p, 4); stats_.rst_out++; }
void H2Mux::send_goaway(MuxLeg &to, uint32_t last_sid, uint32_t code) {
    if (to.goaway_sent) return;
    uint8_t p[8]; wr32(p, last_sid); wr32(p + 4, code); emit(to, kGoaway, 0, 0, p, 8); to.goaway_sent = true;
}
// ":status NNN" via static name index 8 + literal-without-indexing raw value: valid under any table state.
void H2Mux::send_status(MuxLeg &to, uint32_t sid, const char *s) {
    const uint8_t blk[5] = {0x08, 0x03, uint8_t(s[0]), uint8_t(s[1]), uint8_t(s[2])};
    emit(to, kHeaders, kFlagEndHeaders | kFlagEndStream, sid, blk, 5);
}

void H2Mux::emit_raw(MuxLeg &to, const uint8_t *bytes, size_t n) {
    if (to.ch == nullptr || to.ch->tx().dead()) return;
    if (to.outq_off < to.outq.size()) { to.outq.insert(to.outq.end(), bytes, bytes + n); return; }
    const size_t k = to.ch->tx().push(bytes, n);
    if (k < n) { stats_.stalls++; to.outq.assign(bytes + k, bytes + n); to.outq_off = 0; }
}

void H2Mux::emit(MuxLeg &to, uint8_t type, uint8_t flags, uint32_t sid, const uint8_t *payload, uint32_t plen) {
    uint8_t h[9];
    if (plen > to.peer_max_frame) {
        if (type == kData || type == kHeaders || type == kContinuation) {
            stats_.reframed++;
            uint32_t off = 0;
            while (off < plen) {
                const uint32_t n = (plen - off) < to.peer_max_frame ? (plen - off) : to.peer_max_frame;
                const bool last = off + n == plen;
                uint8_t f = 0, t = type;
                if (type == kData) { f = last ? (flags & kFlagEndStream) : 0; }
                else { t = off == 0 ? type : kContinuation; f = last ? (flags & kFlagEndHeaders) : 0; if (off == 0 && type == kHeaders) f |= flags & kFlagEndStream; }
                wr_hdr(h, n, t, f, sid); emit_raw(to, h, 9); emit_raw(to, payload + off, n); off += n;
            }
            return;
        }
        leg_error(to, "frame exceeds peer MAX_FRAME_SIZE");
        return;
    }
    wr_hdr(h, plen, type, flags, sid);
    emit_raw(to, h, 9);
    if (plen > 0) emit_raw(to, payload, plen);
}

void H2Mux::flush(MuxLeg &to) {
    if (to.ch == nullptr || to.ch->tx().dead()) { to.outq.clear(); to.outq_off = 0; return; }
    if (to.outq_off < to.outq.size()) {
        const size_t k = to.ch->tx().push(to.outq.data() + to.outq_off, to.outq.size() - to.outq_off);
        to.outq_off += k;
        if (to.outq_off >= to.outq.size()) { to.outq.clear(); to.outq_off = 0; }
    }
}

void H2Mux::leg_error(MuxLeg &leg, const char *why) {
    stats_.proto_errors++;
    log_warn("mux slot=%d (%s): %s", leg.ch ? leg.ch->slot() : -1, leg.is_client ? "client" : "backend", why);
    send_goaway(leg, uint32_t(leg.last_peer_sid), kErrProtocol);
    leg.goaway_recv = true; // stop using it
}

void H2Mux::close_stream(MuxStream *st) {
    if (st->b != nullptr) st->b->streams.erase(st->b_sid);
    if (st->c != nullptr) st->c->streams.erase(st->c_sid);
    stats_.streams_closed++;
    delete st;
}

void H2Mux::pump() {
    for (auto &e : legs_) flush(*e.second);
    for (auto &e : legs_) {
        MuxLeg &leg = *e.second;
        if (replay_held(leg)) intake(leg);
    }
    for (auto &e : legs_) flush(*e.second);
}

bool H2Mux::replay_held(MuxLeg &from) {
    while (!from.held.empty()) {
        std::vector<uint8_t> &fr = from.held.front();
        if (!on_frame(from, fr.data(), fr.data() + 9, uint32_t(fr.size() - 9))) return false;
        from.held.pop_front();
    }
    return true;
}

void H2Mux::intake(MuxLeg &from) {
    const uint8_t *seg = nullptr;
    uint32_t len = 0;
    while (from.ch->pop(&seg, &len)) {
        const uint8_t *p = seg;
        size_t n = len;
        if (from.preface_left > 0) {
            const size_t k = n < from.preface_left ? n : from.preface_left;
            if (std::memcmp(p, kPreface + (sizeof(kPreface) - from.preface_left), k) != 0) { leg_error(from, "bad preface"); return; }
            from.preface_left -= k; p += k; n -= k;
        }
        while (n > 0) {
            if (!from.acc.empty()) {
                const size_t need = from.acc.size() < 9 ? 9 - from.acc.size() : (9 + rd24(from.acc.data())) - from.acc.size();
                const size_t k = n < need ? n : need;
                from.acc.insert(from.acc.end(), p, p + k); p += k; n -= k;
                if (from.acc.size() < 9) continue;
                const uint32_t plen = rd24(from.acc.data());
                if (plen > kOurMaxFrame) { leg_error(from, "frame too large"); return; }
                if (from.acc.size() < 9 + plen) continue;
                std::vector<uint8_t> fr; fr.swap(from.acc);
                if (!on_frame(from, fr.data(), fr.data() + 9, plen)) from.held.push_back(std::move(fr));
                continue;
            }
            if (n < 9) { from.acc.assign(p, p + n); break; }
            const uint32_t plen = rd24(p);
            if (plen > kOurMaxFrame) { leg_error(from, "frame too large"); return; }
            if (n < 9 + plen) { from.acc.assign(p, p + n); break; }
            if (from.held.empty()) { if (!on_frame(from, p, p + 9, plen)) from.held.emplace_back(p, p + 9 + plen); }
            else from.held.emplace_back(p, p + 9 + plen);
            p += 9 + plen; n -= 9 + plen;
        }
    }
}

bool H2Mux::on_frame(MuxLeg &from, const uint8_t *hdr, const uint8_t *payload, uint32_t plen) {
    const uint8_t type = hdr[3], flags = hdr[4];
    const uint32_t sid = rd32(hdr + 5) & 0x7fffffffu;
    stats_.frames_in++;
    if (from.in_block && (type != kContinuation || sid != from.block_sid)) { leg_error(from, "expected CONTINUATION"); return true; }
    switch (type) {
    case kData: return on_data(from, sid, flags, payload, plen);
    case kHeaders: case kContinuation: on_headers(from, sid, type, flags, payload, plen); return true;
    case kPriority: return true;
    case kRstStream: on_rst(from, sid, payload, plen); return true;
    case kSettings: on_settings(from, flags, payload, plen); return true;
    case kPushPromise: leg_error(from, "PUSH_PROMISE"); return true;
    case kPing: on_ping(from, flags, payload, plen); return true;
    case kGoaway: on_goaway(from, payload, plen); return true;
    case kWindowUpdate: on_window_update(from, sid, payload, plen); return true;
    default: return true;
    }
}

bool H2Mux::on_data(MuxLeg &from, uint32_t sid, uint8_t flags, const uint8_t *p, uint32_t n) {
    if (sid == 0) { leg_error(from, "DATA on stream 0"); return true; }
    from.recv_unacked += n;
    if (from.recv_unacked >= int64_t(kOurConnWindow / 4)) { send_window_update(from, 0, uint32_t(from.recv_unacked)); from.recv_unacked = 0; }
    auto it = from.streams.find(int32_t(sid));
    MuxStream *st = it == from.streams.end() ? nullptr : it->second;
    stats_.data_frames++;
    if (st == nullptr) return true;
    if (from.is_client) {
        if (st->denied) { if (flags & kFlagEndStream) close_stream(st); return true; }
        if (st->b == nullptr) { // request not dispatched yet (block still in flight)
            st->early_data.emplace_back(p, p + n);
            if (flags & kFlagEndStream) st->req_done = true;
            return true;
        }
        MuxLeg &to = *st->b;
        if (st->b_send_win < int64_t(n) || to.send_window < int64_t(n)) { stats_.window_held++; return false; }
        st->b_send_win -= n; to.send_window -= n;
        st->c_recv_unacked += n;
        if (st->c_recv_unacked >= int64_t(kOurInitWindow / 2)) { send_window_update(from, sid, uint32_t(st->c_recv_unacked)); st->c_recv_unacked = 0; }
        emit(to, kData, flags & kFlagEndStream, uint32_t(st->b_sid), p, n);
        if (flags & kFlagEndStream) st->req_done = true;
        return true;
    }
    // backend -> client
    if (st->c == nullptr) return true;
    MuxLeg &to = *st->c;
    if (st->c_send_win < int64_t(n) || to.send_window < int64_t(n)) { stats_.window_held++; return false; }
    st->c_send_win -= n; to.send_window -= n;
    st->b_recv_unacked += n;
    if (st->b_recv_unacked >= int64_t(kOurInitWindow / 2)) { send_window_update(from, sid, uint32_t(st->b_recv_unacked)); st->b_recv_unacked = 0; }
    emit(to, kData, flags & kFlagEndStream, uint32_t(st->c_sid), p, n);
    if (flags & kFlagEndStream) { st->res_done = true; close_stream(st); }
    return true;
}

void H2Mux::on_headers(MuxLeg &from, uint32_t sid, uint8_t type, uint8_t flags, const uint8_t *p, uint32_t n) {
    if (sid == 0) { leg_error(from, "HEADERS on stream 0"); return; }
    if (type == kHeaders) {
        if (from.is_client && from.streams.count(int32_t(sid)) == 0) {
            if (int32_t(sid) <= from.last_peer_sid || (sid & 1u) == 0) { leg_error(from, "bad new stream id"); return; }
            MuxStream *st = new MuxStream();
            st->c = &from; st->c_sid = int32_t(sid);
            st->c_send_win = from.peer_init_window;
            from.streams[st->c_sid] = st;
            from.last_peer_sid = st->c_sid;
            stats_.streams_opened++;
        }
        from.in_block = (flags & kFlagEndHeaders) == 0;
        from.block_sid = sid;
        from.block_flags = flags;
        from.block.clear();
        // strip padding / priority for the walk
        const uint8_t *q = p; uint32_t left = n, pad = 0;
        if (flags & kFlagPadded) { if (left < 1) { leg_error(from, "bad padding"); return; } pad = *q++; left--; }
        if (flags & kFlagPriority) { if (left < 5) { leg_error(from, "bad priority"); return; } q += 5; left -= 5; }
        if (pad > left) { leg_error(from, "padding"); return; }
        from.block.insert(from.block.end(), q, q + (left - pad));
    } else {
        stats_.continuation++;
        from.in_block = (flags & kFlagEndHeaders) == 0;
        from.block.insert(from.block.end(), p, p + n);
    }
    if (flags & kFlagEndHeaders) on_block_complete(from);
}

void H2Mux::on_block_complete(MuxLeg &from) {
    stats_.header_blocks++;
    auto it = from.streams.find(int32_t(from.block_sid));
    MuxStream *st = it == from.streams.end() ? nullptr : it->second;
    const bool end_stream = (from.block_flags & kFlagEndStream) != 0;
    if (from.is_client) {
        if (st == nullptr) { transcode_impl(from.block.data(), int(from.block.size()), from.rx_mirror, nullptr, tbuf_, nullptr, ts_); return; }
        if (st->b == nullptr && !st->denied) {
            // First block of the request: choose the backend now (per request).
            MuxLeg *b = pick_backend(from.key);
            if (b == nullptr) {
                transcode_impl(from.block.data(), int(from.block.size()), from.rx_mirror, nullptr, tbuf_, nullptr, ts_);
                stats_.no_path++; // reuse counter: "no backend"
                send_status(from, uint32_t(st->c_sid), "503");
                close_stream(st);
                return;
            }
            // Transcode once, for real, against the chosen backend's encoder table.
            // A deny is decided afterwards; since the encoder table has already
            // absorbed this block's insertions, the block is still sent to the
            // backend and immediately cancelled (RST_STREAM) so its decoder stays
            // in sync — the same limitation as relay v2 (a header-only request
            // reaches the backend handler before the cancel).
            tbuf_.clear();
            if (!transcode_impl(from.block.data(), int(from.block.size()), from.rx_mirror, &b->tx_enc, tbuf_, &pf_, ts_)) {
                stats_.walk_fail++; leg_error(from, "malformed header block"); return;
            }
            if ((pf_.have & (1u << (HW_PATH - 1))) == 0 ||
                !policy_->match(pf_.path.data(), int(pf_.path.size()), pf_.authority.data(), int(pf_.authority.size()))) {
                stats_.deny++;
                st->denied = true;
                const uint32_t bsid = uint32_t(b->next_sid); b->next_sid += 2;
                emit(*b, kHeaders, kFlagEndHeaders | (end_stream ? kFlagEndStream : 0), bsid, tbuf_.data(), uint32_t(tbuf_.size()));
                send_rst(*b, bsid, kErrCancel);
                send_status(from, uint32_t(st->c_sid), "403");
                if (end_stream) close_stream(st);
                return;
            }
            stats_.allow++;
            st->b = b; st->b_sid = b->next_sid; b->next_sid += 2;
            st->b_send_win = b->peer_init_window;
            b->streams[st->b_sid] = st;
            emit(*b, kHeaders, kFlagEndHeaders | (end_stream ? kFlagEndStream : 0), uint32_t(st->b_sid), tbuf_.data(), uint32_t(tbuf_.size()));
            if (end_stream) st->req_done = true;
            for (auto &d : st->early_data) emit(*b, kData, 0, uint32_t(st->b_sid), d.data(), uint32_t(d.size()));
            if (!st->early_data.empty() && st->req_done) emit(*b, kData, kFlagEndStream, uint32_t(st->b_sid), nullptr, 0);
            st->early_data.clear();
            return;
        }
        // trailers (or block on a denied stream): mirror always, forward if live
        tbuf_.clear();
        HpTable *enc = (st->b != nullptr && !st->denied) ? &st->b->tx_enc : nullptr;
        if (!transcode_impl(from.block.data(), int(from.block.size()), from.rx_mirror, enc, tbuf_, nullptr, ts_)) { stats_.walk_fail++; leg_error(from, "malformed trailer block"); return; }
        if (enc) { emit(*st->b, kHeaders, kFlagEndHeaders | (end_stream ? kFlagEndStream : 0), uint32_t(st->b_sid), tbuf_.data(), uint32_t(tbuf_.size())); }
        if (end_stream) { st->req_done = true; if (st->denied) close_stream(st); }
        return;
    }
    // backend -> client: response headers / trailers
    tbuf_.clear();
    HpTable *enc = (st != nullptr && st->c != nullptr) ? &st->c->tx_enc : nullptr;
    if (!transcode_impl(from.block.data(), int(from.block.size()), from.rx_mirror, enc, tbuf_, nullptr, ts_)) { stats_.walk_fail++; leg_error(from, "malformed response block"); return; }
    if (enc) {
        emit(*st->c, kHeaders, kFlagEndHeaders | (end_stream ? kFlagEndStream : 0), uint32_t(st->c_sid), tbuf_.data(), uint32_t(tbuf_.size()));
        if (end_stream) { st->res_done = true; close_stream(st); }
    }
}

void H2Mux::on_settings(MuxLeg &from, uint8_t flags, const uint8_t *p, uint32_t n) {
    if (flags & kFlagAck) return;
    if (n % 6 != 0) { leg_error(from, "bad SETTINGS"); return; }
    stats_.settings_in++;
    for (uint32_t i = 0; i < n; i += 6) {
        const uint16_t id = uint16_t((p[i] << 8) | p[i + 1]);
        const uint32_t v = rd32(p + i + 2);
        switch (id) {
        case kSetTableSize: from.tx_enc.set_max(v < kOurTableSize ? v : kOurTableSize); break; // our encoder toward this peer
        case kSetInitWindow: {
            const int64_t delta = int64_t(v) - int64_t(from.peer_init_window);
            from.peer_init_window = v;
            for (auto &s : from.streams) { if (from.is_client) s.second->c_send_win += delta; else s.second->b_send_win += delta; }
            break;
        }
        case kSetMaxFrame: if (v >= 16384 && v <= 16777215) from.peer_max_frame = v; break;
        default: break;
        }
    }
    emit(from, kSettings, kFlagAck, 0, nullptr, 0);
}

void H2Mux::on_window_update(MuxLeg &from, uint32_t sid, const uint8_t *p, uint32_t n) {
    if (n != 4) { leg_error(from, "bad WINDOW_UPDATE"); return; }
    const uint32_t inc = rd32(p) & 0x7fffffffu;
    if (sid == 0) { from.send_window += inc; return; }
    auto it = from.streams.find(int32_t(sid));
    if (it == from.streams.end()) return;
    if (from.is_client) it->second->c_send_win += inc; else it->second->b_send_win += inc;
}

void H2Mux::on_rst(MuxLeg &from, uint32_t sid, const uint8_t *p, uint32_t n) {
    if (n != 4 || sid == 0) { leg_error(from, "bad RST_STREAM"); return; }
    stats_.rst_in++;
    if (from.in_block && from.block_sid == sid) { leg_error(from, "RST inside block"); return; }
    auto it = from.streams.find(int32_t(sid));
    if (it == from.streams.end()) return;
    MuxStream *st = it->second;
    const uint32_t code = rd32(p);
    if (from.is_client) { if (st->b != nullptr) send_rst(*st->b, uint32_t(st->b_sid), code); }
    else { if (st->c != nullptr) send_rst(*st->c, uint32_t(st->c_sid), code); }
    close_stream(st);
}

void H2Mux::on_ping(MuxLeg &from, uint8_t flags, const uint8_t *p, uint32_t n) {
    if (n != 8) { leg_error(from, "bad PING"); return; }
    if (flags & kFlagAck) return;
    stats_.pings++;
    emit(from, kPing, kFlagAck, 0, p, 8);
}

void H2Mux::on_goaway(MuxLeg &from, const uint8_t *p, uint32_t n) {
    if (n < 8) { leg_error(from, "bad GOAWAY"); return; }
    stats_.goaway++;
    from.goaway_recv = true; // no new streams toward/from it
}

} // namespace dmesh
