//! Full hyper h2 round-trip on the DSB gRPC workload: real hyper server +
//! real hyper client connected by an in-memory duplex pipe (no network, no
//! DMA). One request = client-side encode + server-side decode (+ response
//! the other way) + h2 stream state + HeaderMap + per-stream task spawns —
//! i.e. exactly the h2/hyper machinery a proxy pays per request (one
//! server-termination + one client-termination), minus routing/tower/driver.
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::{TokioExecutor, TokioIo};
use std::time::Instant;

fn trace_id(i: usize) -> String {
    // deterministic jaeger-style ids, same shape as the C/h2 benches
    let h = |x: usize| -> u64 {
        let mut v = x as u64 ^ 0x9e37_79b9_7f4a_7c15;
        v = v.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        v ^= v >> 27;
        v.wrapping_mul(0x94d0_49bb_1331_11eb)
    };
    format!("{:016x}:{:016x}:{:016x}:{}", h(i), h(i * 3 + 1), h(i * 7 + 2), i & 1)
}

fn mk_req(tid: &str) -> Request<Empty<Bytes>> {
    Request::builder()
        .method("POST")
        .uri("http://srv-search/search.Search/Nearby")
        .header("content-type", "application/grpc")
        .header("user-agent", "grpc-go/1.56.3")
        .header("te", "trailers")
        .header("uber-trace-id", tid)
        .body(Empty::new())
        .unwrap()
}

fn main() {
    let iters: usize = std::env::args().nth(1).and_then(|s| s.parse().ok()).unwrap_or(100_000);
    let m: usize = std::env::args().nth(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let tids: Vec<String> = (0..256).map(trace_id).collect();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async move {
        let (c_io, s_io) = tokio::io::duplex(1 << 20);

        // real hyper h2 server with a trivial grpc-ish service
        tokio::spawn(async move {
            let svc = service_fn(|req: Request<hyper::body::Incoming>| async move {
                let _ = req.into_body().collect().await; // drain request
                Ok::<_, hyper::Error>(
                    Response::builder()
                        .header("content-type", "application/grpc")
                        .body(Full::new(Bytes::from_static(b"\0\0\0\0\x05hello")))
                        .unwrap(),
                )
            });
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(s_io), svc)
                .await;
        });

        // real hyper h2 client
        let (sender, conn) =
            hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(c_io))
                .await
                .unwrap();
        tokio::spawn(async move { let _ = conn.await; });

        // warmup
        {
            let mut s = sender.clone();
            for i in 0..256usize {
                let resp = s.send_request(mk_req(&tids[i % 256])).await.unwrap();
                let _ = resp.into_body().collect().await;
            }
        }

        // optional CPU profiling: PROF=<path.pb> wraps the timed loop
        let guard = std::env::var("PROF").ok().map(|_| {
            pprof::ProfilerGuardBuilder::default()
                .frequency(499)
                .blocklist(&["libc", "libgcc", "pthread", "vdso"])
                .build()
                .unwrap()
        });

        // timed: m concurrent in-flight request loops (h2 streams multiplexed)
        let per = iters / m;
        let t0 = Instant::now();
        let mut js = Vec::new();
        for w in 0..m {
            let mut s = sender.clone();
            let tids = tids.clone();
            js.push(tokio::spawn(async move {
                for i in 0..per {
                    let resp = s.send_request(mk_req(&tids[(w * per + i) % 256])).await.unwrap();
                    let _ = resp.into_body().collect().await;
                }
            }));
        }
        for j in js {
            j.await.unwrap();
        }
        let el = t0.elapsed();
        let n = per * m;
        println!(
            "hyper h2 round-trip m={m:<3}: {:8.1} ns/req  ({:.0} req/s, n={n})",
            el.as_nanos() as f64 / n as f64,
            n as f64 / el.as_secs_f64()
        );
        if let (Some(g), Ok(path)) = (guard, std::env::var("PROF")) {
            use pprof::protos::Message;
            let profile = g.report().build().unwrap().pprof().unwrap();
            let mut body = Vec::new();
            profile.write_to_vec(&mut body).unwrap();
            std::fs::write(&path, &body).unwrap();
            println!("profile written: {path}");
        }
    });
}
