//! Pure-Rust port of the zero-alloc selective walker (hpack_walk.h) and the
//! lean termination baseline (hpack_term.h): fixed inline buffers, no heap,
//! same algorithms — measuring whether Rust matches the C numbers.
#[path = "../dsb_blocks.rs"]
mod dsb_blocks;
#[path = "../huff_tables.rs"]
mod huff;

use dsb_blocks::*;
use huff::*;
use std::time::Instant;

const VAL_CAP: usize = 128;
const TBL_CAP: usize = 64;

const STATIC_NLEN: [u8; 62] = [
    0, 10, 7, 7, 5, 5, 7, 7, 7, 7, 7, 7, 7, 7, 7, 14, 15, 15, 13, 6, 27, 3,
    5, 13, 13, 19, 16, 16, 14, 16, 13, 12, 6, 4, 4, 6, 7, 4, 4, 8, 17, 13,
    8, 19, 13, 4, 8, 12, 18, 19, 5, 7, 7, 11, 6, 10, 25, 17, 10, 4, 3, 16,
];

// ---------- primitives ----------

#[inline]
fn varint(b: &[u8], i: &mut usize, prefix: u32) -> Option<u32> {
    let mask = (1u32 << prefix) - 1;
    if *i >= b.len() { return None; }
    let mut v = (b[*i] as u32) & mask;
    *i += 1;
    if v == mask {
        let mut m = 0u32;
        loop {
            if *i >= b.len() || m > 28 { return None; }
            let c = b[*i]; *i += 1;
            v += ((c & 0x7f) as u32) << m;
            m += 7;
            if c & 0x80 == 0 { break; }
        }
    }
    Some(v)
}

/// huffman FSM walk; dst None → count-only
#[inline]
fn hw_huff(src: &[u8], mut dst: Option<&mut [u8]>) -> Option<usize> {
    let mut s = 0usize;
    let mut n = 0usize;
    for &byte in src {
        for half in 0..2 {
            let nib = if half == 0 { byte >> 4 } else { byte & 0xf } as usize;
            let fl = HUFF_FLAGS[s][nib];
            if fl & 1 != 0 { return None; }
            if fl & 4 != 0 {
                if let Some(d) = dst.as_deref_mut() {
                    if n >= d.len() { return None; }
                    d[n] = HUFF_EMIT[s][nib];
                }
                n += 1;
            }
            s = HUFF_NEXT[s][nib] as usize;
        }
    }
    Some(n)
}

// ---------- selective walker ----------

#[derive(Clone, Copy)]
struct WEntry { kind: u8, vlen: u8, nlen: u8, size: u32, val: [u8; VAL_CAP] }
impl Default for WEntry {
    fn default() -> Self { WEntry { kind: 0, vlen: 0, nlen: 0, size: 0, val: [0; VAL_CAP] } }
}

struct WState { head: usize, count: usize, size: u32, max: u32, tbl: [WEntry; TBL_CAP] }
impl WState {
    fn new() -> Self { WState { head: 0, count: 0, size: 0, max: 4096, tbl: [WEntry::default(); TBL_CAP] } }
    #[inline] fn get(&self, n: usize) -> Option<&WEntry> {
        if n >= self.count { return None; }
        Some(&self.tbl[(self.head + TBL_CAP - n) % TBL_CAP])
    }
    #[inline] fn evict(&mut self) {
        while self.count > 0 && self.size > self.max {
            let e = &self.tbl[(self.head + TBL_CAP - (self.count - 1)) % TBL_CAP];
            self.size -= e.size;
            self.count -= 1;
        }
    }
    #[inline] fn insert(&mut self, kind: u8, val: &[u8], nlen: u8, size: u32) {
        self.head = (self.head + 1) % TBL_CAP;
        if self.count < TBL_CAP { self.count += 1; }
        let e = &mut self.tbl[self.head];
        e.kind = kind; e.nlen = nlen; e.size = size;
        e.vlen = val.len().min(VAL_CAP) as u8;
        e.val[..e.vlen as usize].copy_from_slice(&val[..e.vlen as usize]);
        self.size += size;
        self.evict();
    }
}

struct WOut { have: u8, mlen: u8, plen: u8, alen: u8,
              method: [u8; 16], path: [u8; VAL_CAP], auth: [u8; 64] }
impl Default for WOut {
    fn default() -> Self {
        WOut { have: 0, mlen: 0, plen: 0, alen: 0,
               method: [0; 16], path: [0; VAL_CAP], auth: [0; 64] }
    }
}

#[inline]
fn static_kind(idx: u32) -> u8 {
    match idx { 1 => 3, 2 | 3 => 1, 4 | 5 => 2, _ => 0 }
}

#[inline]
fn deliver(out: &mut WOut, kind: u8, val: &[u8]) {
    match kind {
        1 => { let n = val.len().min(16); out.method[..n].copy_from_slice(&val[..n]); out.mlen = n as u8; }
        2 => { let n = val.len().min(VAL_CAP); out.path[..n].copy_from_slice(&val[..n]); out.plen = n as u8; }
        3 => { let n = val.len().min(64); out.auth[..n].copy_from_slice(&val[..n]); out.alen = n as u8; }
        _ => return,
    }
    out.have |= 1 << (kind - 1);
}

fn hw_walk(b: &[u8], st: &mut WState, out: &mut WOut, full: bool) -> Result<(), ()> {
    let mut i = 0usize;
    let mut tmp = [0u8; VAL_CAP];
    out.have = 0; out.mlen = 0; out.plen = 0; out.alen = 0;
    while i < b.len() {
        let c = b[i];
        if c & 0x80 != 0 {                                   // indexed
            let idx = varint(b, &mut i, 7).ok_or(())?;
            if idx == 0 { return Err(()); }
            if idx <= 61 {
                let kind = static_kind(idx);
                if kind != 0 {
                    let v: &[u8] = match idx { 1 => b"", 2 => b"GET", 3 => b"POST", 4 => b"/", _ => b"/index.html" };
                    deliver(out, kind, v);
                }
            } else {
                let (kind, vlen, val);
                { let e = st.get((idx - 62) as usize).ok_or(())?;
                  kind = e.kind; vlen = e.vlen as usize; val = e.val; }
                if kind != 0 { deliver(out, kind, &val[..vlen]); }
            }
        } else if c & 0x40 != 0 || c & 0x20 == 0 {           // literal
            let insert = c & 0x40 != 0;
            let prefix = if insert { 6 } else { 4 };
            let idx = varint(b, &mut i, prefix).ok_or(())?;
            let (kind, nlen): (u8, u32) = if idx != 0 {
                if idx <= 61 { (static_kind(idx), STATIC_NLEN[idx as usize] as u32) }
                else {
                    let e = st.get((idx - 62) as usize).ok_or(())?;
                    (e.kind, e.nlen as u32)
                }
            } else {                                          // literal name
                if i >= b.len() { return Err(()); }
                let h = b[i] & 0x80 != 0;
                let l = varint(b, &mut i, 7).ok_or(())? as usize;
                if i + l > b.len() { return Err(()); }
                let nl = if h && insert {
                    hw_huff(&b[i..i + l], None).ok_or(())? as u32
                } else { l as u32 };
                i += l;
                (0, nl)
            };
            // value
            if i >= b.len() { return Err(()); }
            let h = b[i] & 0x80 != 0;
            let l = varint(b, &mut i, 7).ok_or(())? as usize;
            if i + l > b.len() { return Err(()); }
            let dlen: usize;
            if kind != 0 || full {
                dlen = if h { hw_huff(&b[i..i + l], Some(&mut tmp)).ok_or(())? }
                       else { if l > VAL_CAP { return Err(()); } tmp[..l].copy_from_slice(&b[i..i + l]); l };
                if kind != 0 { deliver(out, kind, &tmp[..dlen]); }
            } else if insert && h {
                dlen = hw_huff(&b[i..i + l], None).ok_or(())?;   // count-only
            } else {
                dlen = l;                                         // true skip
            }
            if insert {
                let v = if kind != 0 { &tmp[..dlen.min(VAL_CAP)] } else { &[][..] };
                st.insert(kind, v, nlen.min(255) as u8, nlen + dlen as u32 + 32);
            }
            i += l;
        } else {                                             // table size update
            let v = varint(b, &mut i, 5).ok_or(())?;
            st.max = v;
            st.evict();
        }
    }
    Ok(())
}

// ---------- lean termination (full decode + re-encode) ----------

#[derive(Clone, Copy)]
struct Field { nlen: u8, vlen: u8, noindex: bool, name: [u8; 24], val: [u8; VAL_CAP] }
impl Default for Field {
    fn default() -> Self { Field { nlen: 0, vlen: 0, noindex: false, name: [0; 24], val: [0; VAL_CAP] } }
}

#[derive(Clone, Copy)]
struct TEntry { nlen: u8, vlen: u8, name: [u8; 24], val: [u8; VAL_CAP] }
impl Default for TEntry {
    fn default() -> Self { TEntry { nlen: 0, vlen: 0, name: [0; 24], val: [0; VAL_CAP] } }
}

struct Table { head: usize, count: usize, size: u32, max: u32, tbl: [TEntry; TBL_CAP] }
impl Table {
    fn new() -> Self { Table { head: 0, count: 0, size: 0, max: 4096, tbl: [TEntry::default(); TBL_CAP] } }
    #[inline] fn get(&self, n: usize) -> Option<&TEntry> {
        if n >= self.count { return None; }
        Some(&self.tbl[(self.head + TBL_CAP - n) % TBL_CAP])
    }
    #[inline] fn evict(&mut self) {
        while self.count > 0 && self.size > self.max {
            let e = &self.tbl[(self.head + TBL_CAP - (self.count - 1)) % TBL_CAP];
            self.size -= e.nlen as u32 + e.vlen as u32 + 32;
            self.count -= 1;
        }
    }
    fn insert(&mut self, name: &[u8], val: &[u8]) {
        self.head = (self.head + 1) % TBL_CAP;
        if self.count < TBL_CAP { self.count += 1; }
        let e = &mut self.tbl[self.head];
        e.nlen = name.len() as u8; e.vlen = val.len() as u8;
        e.name[..name.len()].copy_from_slice(name);
        e.val[..val.len()].copy_from_slice(val);
        self.size += name.len() as u32 + val.len() as u32 + 32;
        self.evict();
    }
}

fn ht_static(idx: u32) -> Option<(&'static [u8], &'static [u8])> {
    Some(match idx {
        1 => (b":authority", b""),
        2 => (b":method", b"GET"),
        3 => (b":method", b"POST"),
        4 => (b":path", b"/"),
        6 => (b":scheme", b"http"),
        7 => (b":scheme", b"https"),
        31 => (b"content-type", b""),
        58 => (b"user-agent", b""),
        _ => return None,
    })
}

fn read_str(b: &[u8], i: &mut usize, dst: &mut [u8]) -> Option<usize> {
    if *i >= b.len() { return None; }
    let h = b[*i] & 0x80 != 0;
    let l = varint(b, i, 7)? as usize;
    if *i + l > b.len() { return None; }
    let n = if h { hw_huff(&b[*i..*i + l], Some(dst))? }
            else { if l > dst.len() { return None; } dst[..l].copy_from_slice(&b[*i..*i + l]); l };
    *i += l;
    Some(n)
}

fn ht_decode(b: &[u8], t: &mut Table, out: &mut [Field; 16]) -> Result<usize, ()> {
    let mut i = 0usize;
    let mut nf = 0usize;
    while i < b.len() {
        let c = b[i];
        if nf >= 16 { return Err(()); }
        let f = &mut out[nf];
        if c & 0x80 != 0 {
            let idx = varint(b, &mut i, 7).ok_or(())?;
            if idx == 0 { return Err(()); }
            f.noindex = false;
            if idx <= 61 {
                let (n, v) = ht_static(idx).ok_or(())?;
                f.nlen = n.len() as u8; f.vlen = v.len() as u8;
                f.name[..n.len()].copy_from_slice(n);
                f.val[..v.len()].copy_from_slice(v);
            } else {
                let e = *t.get((idx - 62) as usize).ok_or(())?;
                f.nlen = e.nlen; f.vlen = e.vlen;
                f.name = e.name; f.val = e.val;
            }
            nf += 1;
        } else if c & 0x40 != 0 || c & 0x20 == 0 {
            let insert = c & 0x40 != 0;
            let prefix = if insert { 6 } else { 4 };
            let idx = varint(b, &mut i, prefix).ok_or(())?;
            let nl = if idx != 0 {
                if idx <= 61 {
                    let (n, _) = ht_static(idx).ok_or(())?;
                    f.name[..n.len()].copy_from_slice(n);
                    n.len()
                } else {
                    let e = *t.get((idx - 62) as usize).ok_or(())?;
                    f.name = e.name;
                    e.nlen as usize
                }
            } else {
                read_str(b, &mut i, &mut f.name).ok_or(())?
            };
            let vl = read_str(b, &mut i, &mut f.val).ok_or(())?;
            f.nlen = nl as u8; f.vlen = vl as u8;
            f.noindex = !insert;
            if insert {
                let (name, val) = (f.name, f.val);
                t.insert(&name[..nl], &val[..vl]);
            }
            nf += 1;
        } else {
            let v = varint(b, &mut i, 5).ok_or(())?;
            t.max = v;
            t.evict();
        }
    }
    Ok(nf)
}

// encoder
struct BitBuf<'a> { out: &'a mut [u8], len: usize }
impl<'a> BitBuf<'a> {
    #[inline] fn put(&mut self, b: u8) -> Result<(), ()> {
        if self.len >= self.out.len() { return Err(()); }
        self.out[self.len] = b; self.len += 1; Ok(())
    }
    fn varint(&mut self, mut v: u32, prefix: u32, first: u8) -> Result<(), ()> {
        let mask = (1u32 << prefix) - 1;
        if v < mask { return self.put(first | v as u8); }
        self.put(first | mask as u8)?;
        v -= mask;
        while v >= 128 { self.put(0x80 | (v & 0x7f) as u8)?; v >>= 7; }
        self.put(v as u8)
    }
    fn string(&mut self, s: &[u8]) -> Result<(), ()> {
        let hl = (s.iter().map(|&c| HUFF_ENC_BITS[c as usize] as usize).sum::<usize>() + 7) / 8;
        if hl < s.len() {
            self.varint(hl as u32, 7, 0x80)?;
            let mut acc = 0u64; let mut nbits = 0u32;
            for &c in s {
                acc = (acc << HUFF_ENC_BITS[c as usize]) | HUFF_ENC_CODE[c as usize] as u64;
                nbits += HUFF_ENC_BITS[c as usize] as u32;
                while nbits >= 8 { nbits -= 8; self.put((acc >> nbits) as u8)?; }
            }
            if nbits > 0 {
                let pad = 8 - nbits;
                self.put(((acc << pad) | ((1u64 << pad) - 1)) as u8)?;
            }
            Ok(())
        } else {
            self.varint(s.len() as u32, 7, 0x00)?;
            for &c in s { self.put(c)?; }
            Ok(())
        }
    }
}

fn static_exact(f: &Field) -> u32 {
    let n = &f.name[..f.nlen as usize];
    let v = &f.val[..f.vlen as usize];
    match (n, v) {
        (b":method", b"POST") => 3, (b":method", b"GET") => 2,
        (b":scheme", b"http") => 6, (b":scheme", b"https") => 7,
        _ => 0,
    }
}

fn static_name(f: &Field) -> u32 {
    match &f.name[..f.nlen as usize] {
        b":authority" => 1, b":path" => 4, b"content-type" => 31, b"user-agent" => 58,
        _ => 0,
    }
}

fn ht_encode(fields: &[Field], t: &mut Table, out: &mut [u8]) -> Result<usize, ()> {
    let mut bb = BitBuf { out, len: 0 };
    for f in fields {
        let n = &f.name[..f.nlen as usize];
        let v = &f.val[..f.vlen as usize];
        if f.noindex {
            let mut namehit = 0u32;
            for i in 0..t.count {
                let e = t.get(i).unwrap();
                if e.nlen == f.nlen && &e.name[..e.nlen as usize] == n { namehit = 62 + i as u32; break; }
            }
            if namehit != 0 { bb.varint(namehit, 4, 0x00)?; }
            else {
                let s = static_name(f);
                bb.varint(s, 4, 0x00)?;
                if s == 0 { bb.string(n)?; }
            }
            bb.string(v)?;
            continue;
        }
        let sidx = static_exact(f);
        if sidx != 0 { bb.varint(sidx, 7, 0x80)?; continue; }
        let mut hit = 0u32; let mut namehit = 0u32;
        for i in 0..t.count {
            let e = t.get(i).unwrap();
            if e.nlen == f.nlen && &e.name[..e.nlen as usize] == n {
                if e.vlen == f.vlen && &e.val[..e.vlen as usize] == v { hit = 62 + i as u32; break; }
                if namehit == 0 { namehit = 62 + i as u32; }
            }
        }
        if hit != 0 { bb.varint(hit, 7, 0x80)?; continue; }
        if namehit != 0 { bb.varint(namehit, 6, 0x40)?; }
        else {
            let s = static_name(f);
            bb.varint(s, 6, 0x40)?;
            if s == 0 { bb.string(n)?; }
        }
        bb.string(v)?;
        t.insert(n, v);
    }
    Ok(bb.len)
}

// ---------- bench ----------

fn bench_walk(name: &str, first: &[u8], steady: &[&[u8]], iters: usize, full: bool) {
    let mut st = WState::new();
    let mut out = WOut::default();
    let n = steady.len();
    let mut fails = 0u64;
    let mut sink = 0u64;
    let t0 = Instant::now();
    for i in 0..iters {
        if i % n == 0 {
            st = WState::new();
            let _ = hw_walk(first, &mut st, &mut out, full);
        }
        if hw_walk(steady[i % n], &mut st, &mut out, full).is_err() { fails += 1; }
        sink += out.plen as u64;
    }
    let el = t0.elapsed();
    println!("{name:<22}: {:7.1} ns/block (fails={fails}, sink={sink})",
             el.as_nanos() as f64 / iters as f64);
    assert_eq!(fails, 0);
}

fn bench_term(name: &str, first: &[u8], steady: &[&[u8]], iters: usize, reencode: bool) {
    let mut dec = Table::new();
    let mut enc = Table::new();
    let mut fields = [Field::default(); 16];
    let mut blk = [0u8; 512];
    let n = steady.len();
    let mut fails = 0u64;
    let mut sink = 0u64;
    let t0 = Instant::now();
    for i in 0..iters {
        if i % n == 0 {
            dec = Table::new(); enc = Table::new();
            let nf = ht_decode(first, &mut dec, &mut fields).unwrap();
            if reencode { let _ = ht_encode(&fields[..nf], &mut enc, &mut blk); }
        }
        match ht_decode(steady[i % n], &mut dec, &mut fields) {
            Ok(nf) => {
                if reencode {
                    match ht_encode(&fields[..nf], &mut enc, &mut blk) {
                        Ok(el) => sink += el as u64,
                        Err(_) => fails += 1,
                    }
                } else { sink += nf as u64; }
            }
            Err(_) => fails += 1,
        }
    }
    let el = t0.elapsed();
    println!("{name:<22}: {:7.1} ns/block (fails={fails}, sink={sink})",
             el.as_nanos() as f64 / iters as f64);
    assert_eq!(fails, 0);
}

fn main() {
    let iters: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(2_000_000);

    // correctness
    let mut st = WState::new();
    let mut out = WOut::default();
    hw_walk(FIRST, &mut st, &mut out, false).unwrap();
    hw_walk(STEADY[0], &mut st, &mut out, false).unwrap();
    println!("correctness: have={:x} method={} path={} auth={}",
             out.have,
             std::str::from_utf8(&out.method[..out.mlen as usize]).unwrap(),
             std::str::from_utf8(&out.path[..out.plen as usize]).unwrap(),
             std::str::from_utf8(&out.auth[..out.alen as usize]).unwrap());
    assert_eq!(out.have, 7);

    bench_walk("RS SELECTIVE", FIRST, STEADY, iters, false);
    bench_term("RS FULL decode", FIRST, STEADY, iters, false);
    bench_term("RS DEC+RE-ENCODE", FIRST, STEADY, iters, true);
    bench_walk("RS NI SELECTIVE", NI_FIRST, NI_STEADY, iters, false);
    bench_term("RS NI FULL decode", NI_FIRST, NI_STEADY, iters, false);
    bench_term("RS NI DEC+RE-ENC", NI_FIRST, NI_STEADY, iters, true);
}
