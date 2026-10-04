# EventStreamLab Day 8 — Final Release Benchmark

## A. Environment

- Platform: Linux-6.18.33.2-microsoft-standard-WSL2-x86_64-with-glibc2.39
- Kernel: Linux DESKTOP-2O9MI67 6.18.33.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC Thu Jun 18 21:54:43 UTC 2026 x86_64 x86_64 x86_64 GNU/Linux
- Build: Release
- Compiler: c++ (Ubuntu 13.3.0-6ubuntu2~24.04.1) 13.3.0 (/usr/bin/c++)
- CMake: cmake version 3.28.3
- Commit: `adcc3b73b9428580f4f23e3f15cf0d6f7e5121f7`
- CMAKE_CXX_FLAGS_RELEASE: `-O3 -DNDEBUG`
- CMAKE_CXX_FLAGS: ``
- Policy: 32 MiB logical unsent bytes per Consumer, disconnect on overflow.

30 FPS, FAST #2; Consumer #1 FAST or SLOW 100 ms. Independent process completion seconds start immediately before Producer launch; sample/event milliseconds start with the Server event loop. No performance pass threshold.

## B. Scenario A — FAST/FAST 1800

| Run | Producer s | Consumer #1 s | FAST #2 s | Server s | #1 exact peak frames | #1 exact peak bytes | #2 exact peak bytes | #1 overflow |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 59.971 | 59.972 | 59.972 | 59.972 | 1 | 512034 | 512034 | 0 |
| 2 | 59.973 | 59.973 | 59.973 | 59.973 | 1 | 512034 | 512034 | 0 |
| 3 | 59.970 | 59.971 | 59.971 | 59.971 | 1 | 512034 | 512034 | 0 |

| Metric | Median s | Min s | Max s |
| --- | ---: | ---: | ---: |
| producer_completion_s | 59.971 | 59.970 | 59.973 |
| consumer1_completion_s | 59.972 | 59.971 | 59.973 |
| consumer2_completion_s | 59.972 | 59.971 | 59.973 |
| server_completion_s | 59.972 | 59.971 | 59.973 |

## C. Scenario B — SLOW/FAST 300 under-limit

| Run | Producer s | Consumer #1 s | FAST #2 s | Server s | #1 exact peak frames | #1 exact peak bytes | #2 exact peak bytes | #1 overflow |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 9.970 | 30.054 | 9.971 | 26.991 | 167 | 19496592 | 512034 | 0 |

All processes exited 0 and completed 300 Frames. Both overflow/disconnect metrics are 0. SLOW backlog at EOF: 167 Frames / 19496592 bytes; final zero after 17.019 s of TX drain. A Slow Consumer within configured capacity is served through complete drain.

## D. Scenario C — SLOW/FAST 1800 overload

| Run | Producer s | Consumer #1 s | FAST #2 s | Server s | #1 exact peak frames | #1 exact peak bytes | #2 exact peak bytes | #1 overflow |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 59.970 | 20.135 | 59.971 | 59.971 | 288 | 33521741 | 512034 | 1 |
| 2 | 59.970 | 20.138 | 59.971 | 59.971 | 288 | 33521741 | 512034 | 1 |
| 3 | 59.971 | 24.440 | 59.971 | 59.971 | 292 | 33544838 | 512034 | 1 |

| Metric | Median s | Min s | Max s |
| --- | ---: | ---: | ---: |
| producer_completion_s | 59.970 | 59.970 | 59.971 |
| consumer1_completion_s | 20.138 | 20.135 | 24.440 |
| consumer2_completion_s | 59.971 | 59.971 | 59.971 |
| server_completion_s | 59.971 | 59.971 | 59.971 |

| Run | Overflow ms | Consumer | Pending bytes | Incoming bytes | Limit bytes | FAST completed | FAST overflow |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 16300 | 1 | 33521741 | 102434 | 33554432 | 1800 | 0 |
| 2 | 16300 | 1 | 33521741 | 102434 | 33554432 | 1800 | 0 |
| 3 | 17867 | 1 | 33544838 | 102434 | 33554432 | 1800 | 0 |

Run 1 SLOW policy outcome: {"expected_policy_disconnect": true, "exit_code": 1, "transport_outcome": "Failed to receive frame payload: Peer closed connection after receiving 75217 of 102400 requested bytes", "frames_received": null, "overflow_incoming_sequence": 490, "note": "A partial-Frame EOF may exit 1 without a final Consumer summary; missing frame counts are not inferred."}

Run 2 SLOW policy outcome: {"expected_policy_disconnect": true, "exit_code": 1, "transport_outcome": "Failed to receive frame payload: Peer closed connection after receiving 75217 of 102400 requested bytes", "frames_received": null, "overflow_incoming_sequence": 490, "note": "A partial-Frame EOF may exit 1 without a final Consumer summary; missing frame counts are not inferred."}

Run 3 SLOW policy outcome: {"expected_policy_disconnect": true, "exit_code": 1, "transport_outcome": "Failed to receive frame payload: Peer closed connection after receiving 52256 of 102400 requested bytes", "frames_received": null, "overflow_incoming_sequence": 537, "note": "A partial-Frame EOF may exit 1 without a final Consumer summary; missing frame counts are not inferred."}

## E. Final interpretation

All seven correctness checks passed. Normal Consumers did not trigger the bounded queue policy. Under-limit SLOW backlog was tolerated and drained. Sustained overload isolated only the SLOW connection before its logical backlog would exceed 32 MiB; Producer and FAST completed all 1800 Frames after isolation.

The limit applies to per-Consumer logical unsent TX backlog, not process RSS. Shared Frame storage, allocator overhead and kernel socket buffers are separate. Exact peaks track every enqueue; coarse samples can miss instantaneous maxima.

Day 5 is the Blocking versus Event architecture comparison. Day 8 validates the final Release implementation; no new Blocking speedup comparison is made because overload semantics differ.

## F. Caveats

- Linux / WSL loopback, synthetic workload, one Producer and two Consumers; not production network hardware.
- Immediate overload disconnect may terminate SLOW mid-Frame. Exit 1 is accepted only for corroborated EOF/reset transport termination; validation failures and crashes fail correctness.
- Policy disconnect gives up SLOW stream continuity; missing final SLOW Frame counts are not inferred.
- Existing ~1 s queue instrumentation adds observation overhead.
- Completion values are observations, not universal throughput claims.

Correctness: 7/7 recorded runs passed. Repository artifacts retain summary.csv, events.csv and queue sample CSVs. The original local benchmark run additionally produced raw_runs.json and per-process log/JSONL files, which were intentionally not committed.
