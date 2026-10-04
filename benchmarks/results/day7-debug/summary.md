# EventStreamLab Day 7 — Bounded TX Queue and Overflow Isolation

## A. Policy

Per-Consumer logical unsent TX backlog limit: 32 MiB (33554432 bytes). Check before enqueue; disconnect only the overloaded Consumer. This isolates Producer/FAST without silent Frame dropping, and is not a universal policy. Shared Frames, allocator overhead and kernel buffers mean process RSS is not capped at 32 MiB.

Linux Debug, 30 FPS; primary SLOW #1 100 ms / FAST #2, 1800 Frames; control FAST/FAST, 300 Frames. Completion seconds start immediately before Producer launch; event/sample elapsed_ms starts with the Server event loop. No performance threshold.

## B. Primary and C. Control results

| Scenario | Correctness | Producer Frames | FAST #2 Frames | Producer s | FAST #2 s | Server s | #1 exact peak bytes | Overflow events |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| SLOW/FAST | PASS | 1800 | 1800 | 59.976 | 59.979 | 59.977 | 33516597 | 1 |
| FAST/FAST | PASS | 300 | 300 | 9.972 | 9.975 | 9.973 | 512034 | 0 |

Overflow at 16004 ms: Consumer #1, 288 pending Frames / 33516597 pending bytes, incoming 102434 bytes, limit 33554432, policy disconnect.

SLOW outcome: {"expected_policy_disconnect": true, "exit_code": 1, "transport_outcome": "Failed to receive frame payload: Peer closed connection after receiving 80361 of 102400 requested bytes", "frames_received": null, "overflow_incoming_sequence": 481, "note": "A partial-Frame EOF may exit 1 without a final Consumer summary; missing frame counts are not inferred."}. Server SLOW counters remain after queue clear; no new enqueue occurs after invalidating its socket.

## D. Interpretation

Both correctness checks passed. Day 6 unbounded SLOW peak at 1800 Frames was 136,110,018 bytes. Day 7 enforces bounded logical backlog and explicitly isolates the overflowing connection; Producer and FAST completed all 1800 Frames. Compare the measured completion times above without a performance pass threshold. The FAST/FAST control had no overflow. Exact peaks are updated on every enqueue, while ~1 s samples may miss instantaneous peaks.

## E. Tradeoff

- Upstream backpressure: can suit lossless delivery, but SLOW can affect overall latency.
- DROP_NEWEST: retains backlog and loses newest information.
- DROP_OLDEST: favors current state, but requires sequence-gap/protocol semantics.
- Disconnect (selected): isolates other connections and gives up SLOW stream continuity.

Lossless/reliable domains may need a different policy. No Day 8 Release benchmark is included.

Repository artifacts retain summary.csv, events.csv and sample CSVs. The original local benchmark run additionally produced raw_runs.json and per-process logs, which were intentionally not committed. A SLOW exit 1 is accepted only for narrowly recognized transport termination corroborated by Server overflow metrics; validation failures and crashes remain failures.
