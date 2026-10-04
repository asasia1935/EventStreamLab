# EventStreamLab Day 6 — Unbounded TX Queue Growth Observation

## A. Experiment conditions

Linux Debug, 30 FPS, SLOW #1 100 ms delay, FAST #2; sequential 300 / 900 / 1800 Frames. Independent process completion is measured from immediately before Producer launch. Coarse ~1 s logging adds observation overhead.

Queue samples use event-loop elapsed_ms, a separate origin from Producer launch. Pending bytes are logical unsent wire bytes, not RSS. Exact peaks use existing Server summary counters; sampled peaks can miss short-lived maxima.

## B. Summary

| Frames | Producer s | FAST s | SLOW s | Server s | SLOW peak frames | SLOW peak bytes | FAST peak frames | FAST peak bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 300 | 9.972 | 9.974 | 30.175 | 23.282 | 125 | 14754072 | 1 | 512034 |
| 900 | 29.974 | 29.974 | 90.511 | 86.536 | 570 | 66139338 | 1 | 512034 |
| 1800 | 59.973 | 59.980 | 181.036 | 176.861 | 1171 | 136110018 | 1 | 512034 |

## C. Observation

- 300 Frames: active SLOW samples changed from 0 to 12879995 bytes. At EOF: 125 Frames / 14754072 bytes. Sample peak: 14754072 bytes at 9970 ms; final zero at 23278 ms, 13.308 s after EOF. FAST sampled peak: 0 bytes.
- 900 Frames: active SLOW samples changed from 0 to 63373620 bytes. At EOF: 561 Frames / 65182214 bytes. Sample peak: 65182214 bytes at 29969 ms; final zero at 86533 ms, 56.564 s after EOF. FAST sampled peak: 0 bytes.
- 1800 Frames: active SLOW samples changed from 0 to 132832266 bytes. At EOF: 1171 Frames / 136110018 bytes. Sample peak: 136110018 bytes at 59974 ms; final zero at 176856 ms, 116.882 s after EOF. FAST sampled peak: 0 bytes.

SLOW exact peak bytes, 300 → 900 → 1800: [14754072, 66139338, 136110018]. Strictly increasing: True. This observation is not a pass threshold.

## D. Interpretation

The samples and peaks support that event-driven processing removed per-connection thread blocking, but an unbounded queue converts sustained consumer rate mismatch into application-level backlog growth. Producer nominal input is 30 Frames/s versus roughly 10 Frames/s SLOW pacing; kernel buffers and variable Frame sizes prevent assuming exactly 20 queued Frames/s. FAST sampled queues remained at the separately reported levels. All queues reached zero after Producer EOF.

## E. Day 7 transition

Queues remain unbounded. Sustained Slow Consumers can create backlog/memory growth risk. Day 7 will consider a queue bound and overload policy; neither is implemented here.

Correctness: 3/3 runs passed. Metadata and complete summaries are in raw_runs.json; samples/ contains both Consumer curves, logs/ contains process stdout and timestamped timelines.
