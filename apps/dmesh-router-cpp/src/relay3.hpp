// relay v3 — HPACK re-indexing transcoder with N:M multiplexing.
//
// Unlike relay v2 (raw block passthrough, which forces 1:1 connection pinning),
// v3 keeps TWO tables per connection:
//   rx mirror  — replica of the PEER's encoder table, updated by walking every
//                block the peer sends (name + encoded value bytes are stored,
//                values are never Huffman-decoded except policy fields);
//   tx encoder — OUR encoder table toward that peer, fully under our control.
// A request block from client C is translated representation-by-representation
// into a block for backend B: static indices pass through, dynamic indices are
// resolved in C's mirror and re-resolved against B's encoder table (index hit →
// new index, miss → literal with the ORIGINAL encoded value bytes and an insert
// into B's table). Never-indexed literals stay never-indexed (RFC 7541 §6.2.3).
// Because the client mirror and the backend encoder are decoupled, requests
// from many clients can share one backend connection, be load-balanced per
// request, reordered, dropped (deny/503) or have headers added/removed.

#pragma once

#include <cstdint>
#include <deque>
#include <string>
#include <unordered_map>
#include <vector>

#include "relay.hpp" // RelayChannel, RelayStats, RelayPolicy, hpack_walk.h primitives

namespace dmesh {

// ---- HPACK tables ---------------------------------------------------------

struct HpEntry {
    // name: static index (1..61) or literal bytes (encoded as received)
    uint8_t name_static = 0;
    std::vector<uint8_t> name_enc;
    bool name_huff = false;
    uint32_t name_dec_len = 0;
    // value: encoded bytes as received (Huffman or raw), never decoded here
    std::vector<uint8_t> val_enc;
    bool val_huff = false;
    uint32_t val_dec_len = 0;
    uint32_t size = 0;      // RFC entry size (decoded lengths + 32)
    uint8_t kind = HW_NONE; // policy field kind, for mirror entries
    std::string val_dec;    // decoded value, only kept for policy kinds
    uint64_t serial = 0;    // unique per table
    // encoder-side: provenance for hit validation
    uint32_t src_tid = 0;
    uint64_t src_serial = 0;
    // mirror-side: cache of where this entry was inserted in encoder tables
    struct Cache { uint32_t tid = 0; uint64_t abs = 0; };
    Cache cache[4];
    uint8_t cache_rr = 0;
};

class HpTable {
  public:
    explicit HpTable(uint32_t tid) : tid_(tid) {}
    uint32_t tid() const { return tid_; }
    void set_max(uint32_t m) { max_ = m; evict(); }
    uint32_t max() const { return max_; }
    // Push a new entry at the head; returns its absolute insertion number.
    uint64_t insert(HpEntry &&e);
    // Dynamic index (62-based) -> entry, or nullptr.
    HpEntry *get(uint32_t idx);
    // Absolute insertion number -> current dynamic index, 0 if evicted.
    uint32_t index_of(uint64_t abs) const {
        if (abs >= inserts_) return 0;
        const uint64_t off = inserts_ - 1 - abs;
        return off < entries_.size() ? static_cast<uint32_t>(62 + off) : 0;
    }
    uint64_t next_abs() const { return inserts_; }
    size_t count() const { return entries_.size(); }

  private:
    void evict();
    uint32_t tid_;
    uint32_t max_ = 4096;
    uint32_t size_ = 0;
    uint64_t inserts_ = 0;
    uint64_t serial_ = 0;
    std::deque<HpEntry> entries_; // front = newest
};

struct PolicyFields {
    uint8_t have = 0;
    std::string method, path, authority;
};

struct TranscodeStats {
    uint64_t fields = 0, static_idx = 0, dyn_hit = 0, dyn_miss = 0, lit_insert = 0, lit_noindex = 0,
             never_indexed = 0, size_updates = 0, in_bytes = 0, out_bytes = 0;
};


// ---- the multiplexer --------------------------------------------------------

struct MuxStream;

struct MuxLeg {
    RelayChannel *ch = nullptr;
    bool is_client = false;
    uint64_t key = 0;
    HpTable rx_mirror; // peer encoder replica
    HpTable tx_enc;    // our encoder toward the peer
    uint32_t peer_max_frame = 16384, peer_init_window = 65535;
    int64_t send_window = 65535, recv_unacked = 0;
    std::vector<uint8_t> outq;
    size_t outq_off = 0;
    std::deque<std::vector<uint8_t>> held;
    size_t preface_left = 0;
    std::vector<uint8_t> acc;
    // header block in flight from this peer
    bool in_block = false;
    uint32_t block_sid = 0;
    uint8_t block_flags = 0; // END_STREAM of the HEADERS frame
    std::vector<uint8_t> block;
    std::unordered_map<int32_t, MuxStream *> streams; // by this leg's stream id
    int32_t next_sid = 1;   // backend legs: next request stream id we open
    int32_t last_peer_sid = 0;
    bool goaway_sent = false, goaway_recv = false;
    MuxLeg(uint32_t tid) : rx_mirror(tid * 2), tx_enc(tid * 2 + 1) {}
};

struct MuxStream {
    MuxLeg *c = nullptr;
    int32_t c_sid = 0;
    MuxLeg *b = nullptr; // null until dispatched
    int32_t b_sid = 0;
    bool denied = false, req_done = false, res_done = false;
    int64_t c_send_win = 65535, b_send_win = 65535;
    int64_t c_recv_unacked = 0, b_recv_unacked = 0;
    // request DATA that arrived before the block was dispatched (rare: HEADERS
    // without END_STREAM followed by DATA in the same segment). Kept in order.
    std::vector<std::vector<uint8_t>> early_data;
};

class H2Mux {
  public:
    explicit H2Mux(const RelayPolicy *policy, bool have_default, uint64_t default_key)
        : policy_(policy), have_default_(have_default), default_key_(default_key) {}
    ~H2Mux();
    void add_leg(RelayChannel *ch);
    void remove_leg(int slot);
    void pump();
    RelayStats &stats() { return stats_; }
    const TranscodeStats &tstats() const { return ts_; }
    std::string tstats_line() const;

  private:
    void intake(MuxLeg &from);
    bool replay_held(MuxLeg &from);
    bool on_frame(MuxLeg &from, const uint8_t *hdr, const uint8_t *payload, uint32_t plen);
    bool on_data(MuxLeg &from, uint32_t sid, uint8_t flags, const uint8_t *p, uint32_t n);
    void on_headers(MuxLeg &from, uint32_t sid, uint8_t type, uint8_t flags, const uint8_t *p, uint32_t n);
    void on_block_complete(MuxLeg &from);
    void dispatch_request(MuxLeg &c, MuxStream *st);
    void on_settings(MuxLeg &from, uint8_t flags, const uint8_t *p, uint32_t n);
    void on_window_update(MuxLeg &from, uint32_t sid, const uint8_t *p, uint32_t n);
    void on_rst(MuxLeg &from, uint32_t sid, const uint8_t *p, uint32_t n);
    void on_ping(MuxLeg &from, uint8_t flags, const uint8_t *p, uint32_t n);
    void on_goaway(MuxLeg &from, const uint8_t *p, uint32_t n);
    MuxLeg *pick_backend(uint64_t key);

    void emit(MuxLeg &to, uint8_t type, uint8_t flags, uint32_t sid, const uint8_t *payload, uint32_t plen);
    void emit_raw(MuxLeg &to, const uint8_t *bytes, size_t n);
    void send_settings(MuxLeg &to);
    void send_window_update(MuxLeg &to, uint32_t sid, uint32_t inc);
    void send_rst(MuxLeg &to, uint32_t sid, uint32_t code);
    void send_goaway(MuxLeg &to, uint32_t last_sid, uint32_t code);
    void send_status(MuxLeg &to, uint32_t sid, const char *status3);
    void flush(MuxLeg &to);
    void leg_error(MuxLeg &leg, const char *why);
    void close_stream(MuxStream *st);

    const RelayPolicy *policy_;
    bool have_default_;
    uint64_t default_key_;
    std::unordered_map<int, MuxLeg *> legs_; // by slot
    std::vector<MuxLeg *> backends_;
    size_t rr_ = 0;
    RelayStats stats_;
    TranscodeStats ts_;
    std::vector<uint8_t> tbuf_; // transcode output scratch
    PolicyFields pf_;
    uint32_t next_tid_ = 1;

    static constexpr uint32_t kOurMaxFrame = 16384;
    static constexpr uint32_t kOurInitWindow = 1u << 20;
    static constexpr uint32_t kOurConnWindow = 4u << 20;
    static constexpr uint32_t kOurTableSize = 4096;
    static constexpr uint32_t kOurMaxStreams = 1000;
};

} // namespace dmesh
