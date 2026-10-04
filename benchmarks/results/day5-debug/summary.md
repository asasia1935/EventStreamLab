# EventStreamLab Day 5 — Linux Debug architectural comparison

Environment: Linux / WSL2, g++ 13.3.0, CMake 3.28.3, Debug.

- Blocking baseline: `blocking-baseline-v0.1`, commit `19609743ecc1e9e27ce5a72149df998d94ef1d61`.
- Event commit: `a0866f33cb1661f0fb981bfc16eba1e328cac5bd`.
- Both Debug builds succeeded and both CTest suites passed 5/5.

Conditions: 30 FPS, 900 Frames, SLOW connection #1 delay 100 ms, FAST connection
#2. SLOW connected first. Architectures alternated for three SLOW/FAST runs
each, followed by one FAST/FAST control run each. All runs were sequential.

Primary times below are seconds from the monotonic timestamp immediately before
Producer launch to independently observed process termination. Program-internal
`elapsed_ms` is secondary. Safety timeouts were 240 s for SLOW/FAST and 90 s for
FAST/FAST; no performance threshold was used.

Expected wire bytes per Consumer: **104478600** = 870 × 100 KiB + 30 × 500 KiB
+ 900 × 34-byte headers, from the workload/protocol definitions.

All eight recorded runs passed correctness validation: every process exited 0;
Producer, Server and both Consumers reported 900 Frames; no protocol, sequence
or payload errors occurred. Each Event Consumer connection reported the expected
wire-byte total. Complete original summaries and individual logs are unavailable.

## A. SLOW/FAST raw runs

Rounded to three decimals here; the accompanying CSV preserves supplied precision.

| Architecture | Run | Producer (s) | FAST (s) | SLOW (s) | Server (s) |
| --- | ---: | ---: | ---: | ---: | ---: |
| Blocking | 1 | 76.801 | 87.410 | 90.482 | 87.408 |
| Event-Driven | 1 | 29.973 | 29.976 | 90.511 | 86.432 |
| Blocking | 2 | 78.513 | 86.512 | 90.487 | 86.511 |
| Event-Driven | 2 | 29.971 | 29.975 | 90.503 | 86.126 |
| Blocking | 3 | 72.370 | 79.914 | 90.478 | 79.913 |
| Event-Driven | 3 | 29.971 | 29.975 | 90.506 | 86.530 |

## B. Median comparison

Difference = Event − Blocking; percentages use the Blocking median.

| Metric | Blocking median (s) | Event-Driven median (s) | Difference |
| --- | ---: | ---: | ---: |
| Producer | 76.801 | 29.971 | −46.829 s (−61.0%) |
| FAST | 86.512 | 29.975 | −56.537 s (−65.4%) |
| SLOW | 90.482 | 90.506 | +0.024 s (approximately 0%) |
| Server | 86.511 | 86.432 | −0.079 s (approximately −0.1%) |

| Architecture | Metric | Min (s) | Max (s) |
| --- | --- | ---: | ---: |
| Blocking | Producer | 72.370 | 78.513 |
| Event-Driven | Producer | 29.971 | 29.973 |
| Blocking | FAST | 79.914 | 87.410 |
| Event-Driven | FAST | 29.975 | 29.976 |
| Blocking | SLOW | 90.478 | 90.487 |
| Event-Driven | SLOW | 90.503 | 90.511 |
| Blocking | Server | 79.913 | 87.408 |
| Event-Driven | Server | 86.126 | 86.530 |

## C. Event Slow queue/readiness

| Run | Slow EAGAIN | Slow EPOLLOUT | Max pending frames | Max pending bytes |
| --- | ---: | ---: | ---: | ---: |
| 1 | 66 | 66 | 562 | 65313180 |
| 2 | 60 | 60 | 570 | 66164459 |
| 3 | 64 | 64 | 571 | 66491874 |
| Median | 64 | 64 | 570 | 66164459 |

Median peak pending bytes were approximately **63.1 MiB**. These counters measure
unsent wire bytes, not process RSS.

## D. FAST/FAST control

| Architecture | Producer (s) | Consumer #1 (s) | Consumer #2 (s) | Server (s) |
| --- | ---: | ---: | ---: | ---: |
| Blocking | 29.972 | 29.975 | 29.975 | 29.973 |
| Event-Driven | 29.971 | 29.974 | 29.974 | 29.973 |

## Interpretation

In Blocking SLOW/FAST runs, SLOW send blocking delayed FAST and propagated
backpressure to Producer. In Event runs, SLOW remained slow at about 90.5 s,
while FAST and Producer completed at about 30 s. FAST/FAST controls finished at
about 29.97 s for both architectures. These measurements support isolation of
head-of-line blocking and backpressure propagation when a Slow Consumer is
present; they do not establish that epoll is always faster.

Server continues draining SLOW pending TX after Producer EOF, so Event Server
completion remains around 86 s. Bytes accepted by the kernel can remain unread
by SLOW, allowing Server to finish before SLOW completes its reads and delays.

Removing thread blocking retained a growing SLOW queue backlog. The median peak
was 570 Frames / 66164459 bytes; this is the starting observation for Day 6.
No queue limit, drop policy or backpressure policy was applied.

Blocking `sendAll()` timing includes blocking waits. Event active send timing
excludes EPOLLOUT wait time. Those counters have different definitions and are
not used for direct speedup calculations. This is a Linux Debug architectural
comparison; the Release final benchmark remains separate.

Recovery note:
The original Day 5 temporary benchmark directory was removed before repository archival.
This summary was reconstructed from the recorded Day 5 console output.
The benchmark runner was recreated with equivalent experiment semantics.
The original raw_runs.json and per-process log files were not reconstructed.
