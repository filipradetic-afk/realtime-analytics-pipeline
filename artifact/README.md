# Operating Artifact: `pipeline_sim.py`

A self-contained, stdlib-only Python simulation of the streaming pipeline's
load-bearing mechanics: bounded ingest buffer, backpressure vs. load-shedding,
tumbling-window aggregation, and per-event end-to-end latency measurement.

## Run it

```bash
python pipeline_sim.py            # backpressure policy (no drops)
python pipeline_sim.py --shed     # load-shedding policy (drops counted)
python pipeline_sim.py --seconds 10 --max-eps 4000
```

No dependencies. Python 3.8+. Finishes in under ~10 seconds on a laptop.

## What it proves

The simulator runs two phases back to back:

1. **Average load** at ~580 events/sec (models 50M events/day).
2. **10x burst** at ~5,800 events/sec (models the spike requirement).

A single stream-processor thread is throttled to a configurable sustained
capacity (`--max-eps`, default 4,000 eps) so the 5,800 eps burst genuinely
exceeds processing capacity and the buffer / policy path is actually exercised
rather than skipped.

Two policies are demonstrated against the same load:

| Policy (flag)              | Data loss | How the design handles overload                              |
| -------------------------- | --------- | ------------------------------------------------------------ |
| Backpressure (default)     | **Zero**  | Producer blocks on a full bounded queue. Latency rises but stays bounded and under 5s. Mirrors Kinesis producer retry/backoff on `ProvisionedThroughputExceeded`. |
| Load-shedding (`--shed`)   | Counted   | On a full queue the event is dropped and counted. Mirrors an explicit 429 at the API tier under sustained overload. |

Sample output (numbers vary slightly per run and per host):

```
Backpressure:  produced 15,968  processed 15,968  dropped 0      p99  1.01 s   PASS
Shed:          produced 19,138  processed 15,774  dropped 3,364  p99  1.00 s   PASS
```

Both keep p99 latency well under the 5-second target while the two mechanics
(zero-loss backpressure vs. explicit counted shedding) are made visible.

## Honest caveats

- This is a **local demonstrator of the design's mechanics, not the AWS system.**
  It does not spin up Kinesis, Flink, or ClickHouse. It reproduces the control
  logic (bounded buffer, backpressure, shedding, windowing, latency accounting)
  so the reasoning behind the AWS choices is inspectable and testable.
- Latencies here are microseconds-to-seconds of in-process queueing, not real
  network + serialization + shuffle costs. The absolute numbers are not a
  prediction of production latency. The **relative behaviour** (bounded buffer
  prevents loss; a saturated processor raises but bounds latency; shedding is a
  deliberate, counted trade-off) is what transfers to the AWS design.
- Time is scaled: the tumbling window is 1 second here to keep the run fast; in
  production it represents a per-minute (or continuous) aggregation.
- Single-process threading means the GIL serializes work. That is fine for a
  mechanics demo. The production system parallelizes across Kinesis shards and
  Flink task slots, which the `--max-eps` knob abstracts.
