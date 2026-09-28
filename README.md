# EventStreamLab

## Project Overview

EventStreamLab is a C++20 project for building and measuring a multi-stream TCP
server. Day 2 establishes a working blocking TCP baseline from a synthetic
Producer, through a Server, to multiple Consumers.

## Engineering Goal

The development cycle is:

**Simple -> Measure -> Problem -> Analyze -> Improve -> Re-measure**

Add complexity when measurements identify a problem that needs it.

## Current Status

- CMake builds `esl_common` as a STATIC library and provides Producer, Server,
  and Consumer executables.
- The shared Protocol uses an explicit 34-byte Big Endian header with
  serialization, deserialization, and magic/version/payload-size validation.
- Blocking socket helpers provide `sendAll()`, exact receive, and clean EOF
  detection distinct from truncated input.
- Producer sends a sequence of Frames at the configured `--fps` for the
  configured `--frames` count. Server validates and forwards each Frame
  sequentially to connected Consumers; Consumers validate the stream and report
  elapsed time.
- Slow Consumers can pause after processing each complete Frame using
  `--mode slow --delay-ms N`.
- The Day 2 Debug build and all five CTest tests passed. Manual stream checks
  covered 5, 35, and 900 Frames.

## Architecture

```text
Synthetic Producer
        |
        v
   Blocking Server
        |
   +----+----+
   |         |
 FAST      SLOW
Consumer  Consumer
```

The Producer connects to the Server's producer port. Consumers connect to its
consumer port. The Server accepts the configured number of Consumers, then
receives each Producer Frame and sends its Header and Payload to each Consumer
in accept order. These connections and Frame transfers are implemented with
blocking TCP sockets.

## Protocol and Workload

`FrameHeader` is an in-memory logical model. Its independent wire layout is 34
bytes, with integer fields in Big Endian order. The validated maximum payload
is 1 MiB.

Normal Frames carry 100 KiB. Every 30th non-zero sequence carries 500 KiB. All
payloads use the deterministic repeating byte pattern `00` through `FF`; no
random data or media codec is involved. The Producer prepares one 500 KiB
buffer and sends the prefix required for each Frame.

The representative workload is 30 FPS for 900 Frames, or about 30 seconds
without blocking delays.

## CLI

```powershell
.\build\Debug\esl_server.exe --producer-port 9000 --consumer-port 9001 --expected-consumers 2
.\build\Debug\esl_producer.exe --host 127.0.0.1 --port 9000 --fps 30 --frames 900
.\build\Debug\esl_consumer.exe --host 127.0.0.1 --port 9001 --mode fast --id 1
.\build\Debug\esl_consumer.exe --host 127.0.0.1 --port 9001 --mode slow --delay-ms 100 --id 10
```

Start the Server first, then the Consumers, then the Producer. Consumer IDs are
local CLI settings; the Server identifies connections only by accept order.

## Build and Test

From Windows Developer PowerShell:

```powershell
cmake -S . -B build
cmake --build build --config Debug
ctest --test-dir build -C Debug --output-on-failure
```

The five tests are `protocol_test`, `clock_test`, `workload_test`, `cli_test`,
and `socket_io_test`. The recorded result is **5/5 passed**.

## Blocking Baseline Benchmark

Measurements below use Windows, a Debug build, and loopback TCP (`127.0.0.1`).
The workload was 30 FPS for 900 Frames, with 100 KiB normal payloads and a 500
KiB payload every 30th Frame. The Server used single-threaded sequential
blocking fan-out to two Consumers. These are baseline observations for
reproducing blocking and backpressure behavior, not final performance figures;
a Release benchmark is still planned.

| Metric | FAST / FAST | SLOW / FAST |
| --- | ---: | ---: |
| Producer elapsed | 29.979 s | 95.780 s |
| Producer total send | 0.192 s | 95.345 s |
| Producer max Frame send | 0.429 ms | 874.048 ms |
| Server elapsed | 29.985 s | 96.662 s |
| Server connection #1 total send | 0.111 s | 95.845 s |
| Server connection #1 max Frame send | 0.337 ms | 761.046 ms |
| Server connection #2 total send | 0.071 s | 0.092 s |
| Server connection #2 max Frame send | 0.319 ms | 0.797 ms |

In the SLOW / FAST run, the 100 ms Slow Consumer connected first, so Server
connection #1 was Slow and connection #2 was Fast. These connection numbers
refer to accept order, not Consumer IDs. Both Consumers received all 900
Frames. Their observed elapsed times were 102.234 s (Slow) and 98.988 s (Fast).
Consumer elapsed time is supplementary because it can include time spent
waiting for the Producer to start.

## Bottleneck Analysis

With two Fast Consumers, each Server connection's cumulative send duration was
small, and the Producer completed in about 30 seconds. Blocking sends did not
form a meaningful bottleneck in that run.

With a Slow Consumer, Server connection #1's total send duration rose from
0.111 s to 95.845 s, while its maximum Frame send rose from 0.337 ms to 761.046
ms. The Fast connection's total send duration remained comparatively small at
0.092 s. This points to the Server waiting on the Slow connection inside its
sequential blocking fan-out; the Fast socket itself was not the slow write.
Because forwarding is sequential on one Server thread, that wait delays later
Consumer sends and the next Producer receive.

Producer total send time also rose from 0.192 s to 95.345 s, with a maximum
Frame send of 874.048 ms. This is consistent with upstream backpressure: while
the Server is blocked forwarding to the Slow Consumer, it reads Producer data
less promptly, and the Producer can then wait in `sendAll()`.

The measured values are application-level `sendAll()` wall durations,
stream elapsed times, and per-connection forwarding durations. OS TCP buffer
sizes and advertised-window changes were not measured directly.

## Day 2 Conclusion and Next Step

The Blocking TCP baseline transfers and validates Frames correctly, but a Slow
Consumer can hold up a single-threaded sequential Server. The measurements show
that this delay reaches the Fast Consumer's overall completion and can
propagate back to the Producer. The next goal is to prevent one connection's
write readiness from stopping progress on other connections. Day 3 will study
and implement Linux non-blocking sockets, `epoll`, and per-connection TX state;
no improvement from that design has been measured yet.
