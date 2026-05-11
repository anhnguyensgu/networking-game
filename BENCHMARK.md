# Benchmark Results

Date: 2026-05-07

Milestone commit:

```text
00d8a68 Add configurable client worker pool
```

## Environment

```text
CPU: Apple M4 Pro
Physical CPUs: 14
Logical CPUs: 14
Server: 127.0.0.1:43120
File descriptor limit during earlier tests: ulimit -n = 256
File descriptor limit during 500-client nonblocking test: ulimit -n = 4096
```

The benchmark uses persistent connections: each benchmark client opens one TCP connection, sends requests repeatedly for the duration, then closes the socket.

Early single-threaded results used the older connect-per-request benchmark. Those numbers are kept for milestone history, but they are not directly comparable to the later persistent-connection benchmark.

## Build Commands

```sh
odin build server -collection:game=. -out:bin/server -o:speed -microarch:native
odin build bench -collection:game=. -out:bin/bench -o:speed -microarch:native
```

## Architecture Summary

| Milestone | Server Model | Benchmark Model | Best Observed Req/s | Notes |
|---|---|---|---:|---|
| Single-threaded | `accept` then `handle_client` inline | Connect per request | 4614.35 | Very unstable; many dial failures in later runs |
| Thread-per-client | Spawn one handler thread per accepted client | Persistent connections | 74275.36 | Hit fd-limit symptoms at 256+ clients |
| Worker pool | Fixed workers plus accepted-client queue | Persistent connections | 78805.47 | Best result in short worker-count sweep |
| Non-blocking IO | `core:nbio` event loop plus request worker queue | Persistent connections | 108850.81 | Current working tree; 500 clients, no failures |

## Single-Threaded Baseline

This was measured before thread-per-client. At this point the benchmark opened a new TCP connection for every request, so each request paid TCP dial/close cost.

Server model:

```text
accept client
handle_client(client, source)
accept next client
```

Command:

```sh
./bin/bench --clients 1 --duration-ms 3000 --timeout-ms 2000
```

| Run | Clients | Attempts | OK | Failed | Dial Failed | Req/s |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 13845 | 13844 | 1 | 1 | 4614.35 |
| 2 | 1 | 1290 | 16 | 1274 | 1274 | 5.33 |
| 3 | 1 | 3650 | 2510 | 1140 | 1140 | 836.27 |

Takeaway: the result was unstable because the old benchmark measured connection churn, not only request handling. Dial failures dominated bad runs.

## Thread-Per-Client Baseline

This was measured before the configurable worker-pool milestone, using persistent benchmark clients.

Server model:

```text
accept client
spawn one thread for that client
worker thread owns the socket until disconnect
```

Command pattern:

```sh
./bin/bench --clients N --duration-ms 3000 --timeout-ms 2000
```

| Clients | Attempts | OK | Failed | Dial Failed | Req/s |
|---:|---:|---:|---:|---:|---:|
| 64 | 217823 | 217823 | 0 | 0 | 72517.60 |
| 128 | 217010 | 217010 | 0 | 0 | 72225.35 |
| 256 | 225477 | 225473 | 4 | 3 | 74275.36 |
| 512 | 225467 | 225207 | 260 | 259 | 74129.37 |

Note: 256+ clients are affected by the process file descriptor limit.

## Worker Pool: Queue Size 256

Server command pattern:

```sh
./bin/server --workers 14 --queue-size 256
```

Benchmark command pattern:

```sh
./bin/bench --clients N --duration-ms 3000 --timeout-ms 2000
```

| Clients | Attempts | OK | Failed | Dial Failed | Req/s |
|---:|---:|---:|---:|---:|---:|
| 14 | 233032 | 233032 | 0 | 0 | 77617.80 |
| 32 | 234267 | 234249 | 18 | 0 | 78057.82 |
| 64 | 225252 | 225202 | 50 | 0 | 75026.14 |
| 128 | 235158 | 235044 | 114 | 0 | 78283.95 |
| 256 | 234240 | 233998 | 242 | 3 | 77891.37 |

## Worker Count Sweep

Fixed benchmark load:

```sh
./bin/bench --clients 128 --duration-ms 3000 --timeout-ms 2000
```

Fixed queue size:

```text
queue-size = 256
```

| Workers | Attempts | OK | Failed | Dial Failed | Req/s |
|---:|---:|---:|---:|---:|---:|
| 8 | 217163 | 217043 | 120 | 0 | 72290.99 |
| 14 | 235494 | 235380 | 114 | 0 | 78400.29 |
| 16 | 234715 | 234603 | 112 | 0 | 78138.05 |
| 24 | 235335 | 235231 | 104 | 0 | 78335.98 |
| 32 | 236764 | 236668 | 96 | 0 | 78805.47 |

## Non-Blocking IO: 500 Clients

Server command:

```sh
ulimit -n 4096
./bin/server --workers 14 --queue-size 1024
```

Benchmark command:

```sh
ulimit -n 4096
./bin/bench --clients 500 --duration-ms 3000 --timeout-ms 2000
```

| Clients | Attempts | OK | Failed | Dial Failed | Elapsed ms | Req/s |
|---:|---:|---:|---:|---:|---:|---:|
| 500 | 336991 | 336991 | 0 | 0 | 3095.90 | 108850.81 |

## Non-Blocking IO: Profiled 500 Clients

The profile mode writes a Spall trace and exits after `profile-ms` so buffers flush cleanly. The current trace is focused on the request CPU path: `process_request` and nested JSON encoding.

Server command:

```sh
ulimit -n 4096
./bin/server --workers 14 --queue-size 1024 --profile --profile-ms 7000 --profile-path server.spall
```

Benchmark command:

```sh
ulimit -n 4096
./bin/bench --clients 500 --duration-ms 3000 --timeout-ms 2000
```

| Clients | Attempts | OK | Failed | Dial Failed | Elapsed ms | Req/s | Trace |
|---:|---:|---:|---:|---:|---:|---:|---|
| 500 | 353695 | 353695 | 0 | 0 | 3100.89 | 114062.50 | `server.spall` |

## Takeaways

- Persistent connections removed the unstable TCP connect-per-request bottleneck.
- The worker-pool milestone tops out around 78k req/s on this machine.
- The current non-blocking IO working tree reached 108850.81 req/s at 500 clients with no failures.
- Profile mode is for reading the request hot path, not for replacing the unprofiled throughput baseline.
- Increasing workers from 8 to 14 helps. Increasing beyond 14 gives only a small change.
- Queue size helps absorb accepted clients, but it does not increase the number of active workers.
- The next likely bottleneck is request handling cost: JSON parse/marshal and per-request allocation.
