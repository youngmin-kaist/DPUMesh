//! Measures the REAL h2 crate's HPACK decoder/encoder on the same
//! DeathStarBench gRPC header blocks used by the C walker / lean-C
//! termination benches — connection-replay semantics (fresh contexts at
//! every 256-block cycle boundary), same as the C benches.
mod dsb_blocks;

use bytes::BytesMut;
use dsb_blocks::*;
use h2::hpack::{Decoder, Encoder, Header};
use http::header::HeaderName;
use std::io::Cursor;
use std::time::Instant;

fn to_opt(h: Header) -> Header<Option<HeaderName>> {
    match h {
        Header::Field { name, value } => Header::Field { name: Some(name), value },
        Header::Authority(a) => Header::Authority(a),
        Header::Method(m) => Header::Method(m),
        Header::Scheme(s) => Header::Scheme(s),
        Header::Path(p) => Header::Path(p),
        Header::Protocol(p) => Header::Protocol(p),
        Header::Status(s) => Header::Status(s),
    }
}

fn decode_block(dec: &mut Decoder, buf: &mut BytesMut, blk: &[u8],
                out: &mut Vec<Header>) -> usize {
    buf.clear();
    buf.extend_from_slice(blk);
    out.clear();
    let mut n = 0;
    dec.decode(&mut Cursor::new(buf), |h| {
        n += 1;
        out.push(h);
    })
    .expect("decode failed");
    n
}

fn bench(name: &str, first: &[u8], steady: &[&[u8]], iters: usize, reencode: bool) {
    let n_steady = steady.len();
    let mut buf = BytesMut::with_capacity(512);
    let mut dst = BytesMut::with_capacity(512);
    let mut fields: Vec<Header> = Vec::with_capacity(16);
    let mut dec = Decoder::new(4096);
    let mut enc = Encoder::new(4096, 0);
    let mut sink = 0usize;

    let t0 = Instant::now();
    for i in 0..iters {
        if i % n_steady == 0 {
            // connection replay boundary: fresh contexts + first block
            dec = Decoder::new(4096);
            enc = Encoder::new(4096, 0);
            let n = decode_block(&mut dec, &mut buf, first, &mut fields);
            if reencode {
                dst.clear();
                enc.encode(fields.drain(..).map(to_opt), &mut dst);
            }
            sink += n;
        }
        let n = decode_block(&mut dec, &mut buf, steady[i % n_steady], &mut fields);
        if reencode {
            dst.clear();
            enc.encode(fields.drain(..).map(to_opt), &mut dst);
            sink += dst.len();
        }
        sink += n;
    }
    let el = t0.elapsed();
    println!("{name:<28}: {:8.1} ns/block  (sink={sink})",
             el.as_nanos() as f64 / iters as f64);
}

fn main() {
    let iters: usize = std::env::args().nth(1)
        .and_then(|s| s.parse().ok()).unwrap_or(500_000);

    // correctness: first + one steady block decode to 8 fields, path extracted
    let mut dec = Decoder::new(4096);
    let mut buf = BytesMut::with_capacity(512);
    let mut fields = Vec::new();
    let n1 = decode_block(&mut dec, &mut buf, FIRST, &mut fields);
    let n2 = decode_block(&mut dec, &mut buf, STEADY[0], &mut fields);
    let path = fields.iter().find_map(|h| match h {
        Header::Path(p) => Some(format!("{:?}", p)),
        _ => None,
    });
    println!("correctness: first={n1} fields, steady={n2} fields, path={path:?}");
    assert_eq!((n1, n2), (8, 8));

    bench("h2 DECODE (inserted)", FIRST, STEADY, iters, false);
    bench("h2 DECODE+RE-ENCODE (ins.)", FIRST, STEADY, iters, true);
    bench("h2 DECODE (NI)", NI_FIRST, NI_STEADY, iters, false);
    bench("h2 DECODE+RE-ENCODE (NI)", NI_FIRST, NI_STEADY, iters, true);
}
