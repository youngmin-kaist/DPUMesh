# bench-results

One Markdown file per benchmark run, written by the `bench` subagent.
Filename: `YYYY-MM-DD_HHMMSS_<label>.md`. Each file records the environment
(git SHA, binary timestamps, DOCA version, node/PCI/EU-partition state, the
exact env + harness command) and the result (throughput, p50/p99, per-core
busy%, proxy L7 metrics, raw wrk2/h2load lines), then a one-line verdict.
A run whose conditions were not written down is not a result.
