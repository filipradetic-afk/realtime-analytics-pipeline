# Results

Actual output from running the operating artifact and its unit tests. Every
number here is copied from a real run on the machine noted below. Nothing is
projected or hand-edited. Absolute latencies are in-process queueing times of
the local simulator, not a prediction of AWS production latency (see the caveats
in [`artifact/README.md`](artifact/README.md) and
[`docs/EVIDENCE-LOG.md`](docs/EVIDENCE-LOG.md)).

## Machine

- Python 3.13.14, CPython, stdlib only (no third-party packages).
- Windows 10 (PowerShell). Single process, GIL-bound.
- Runs finish in under ~10 seconds each; the full test suite in ~20 seconds.

## Unit tests

Command (from the repo root):

```
python -m unittest discover -s artifact -p "test_*.py" -v
```

Result: **12 tests, all passing.**

```
test_zero_drops_and_produced_equals_processed ... ok
test_aggregate_windows_is_pure_and_stable ... ok
test_different_seed_diverges ... ok
test_same_seed_same_aggregates ... ok
test_pct_empty_is_zero ... ok
test_pct_ordering_and_positive ... ok
test_percentiles_helper_matches_pct ... ok
test_no_silent_loss_produced_equals_processed_plus_shed ... ok
test_known_input_expected_counts ... ok
test_total_count_conserved ... ok
test_window_id_left_closed_right_open ... ok
test_window_size_changes_bucketing ... ok

----------------------------------------------------------------------
Ran 12 tests in 20.652s

OK
```

The tests cover: windowed-aggregation correctness against hand-built input,
zero-loss under backpressure (produced == processed), counted shedding
(produced == processed + shed, no silent loss), latency percentile ordering
(p50 <= p95 <= p99, finite and positive), and seed determinism of the
aggregation.

## Benchmark: backpressure (default, no drops)

Command:

```
python artifact/pipeline_sim.py
```

Output:

```
Real-time analytics pipeline - local simulator
----------------------------------------------------
tenants=500  queue_maxsize=4,000  window=1s
avg load=580 eps  burst load=5800 eps  processor_cap=4000 eps
policy=BACKPRESSURE (no drops)

=== Scenario: avg (580 eps) then 10x burst (5,800 eps) ===
  duration (wall)      :   7.58 s
  produced             : 16,606
  processed            : 16,606
  dropped              : 0
  active windows       : 2,834 (window_id x tenant keys)
  throughput           : 2,192 events/sec
  latency p50          :   777.19 ms
  latency p95          :  1084.69 ms
  latency p99          :  1090.51 ms
  latency max          :  1091.93 ms
  latency mean         :   716.89 ms
  <5s p99 target       : PASS (p99 = 1.091 s)

----------------------------------------------------
RESULT: bounded buffer + backpressure -> ZERO data loss under 10x burst.
Note: local demonstrator of design mechanics, not the AWS system.
```

## Benchmark: load-shedding (`--shed`, drops counted)

Command:

```
python artifact/pipeline_sim.py --shed
```

Output:

```
Real-time analytics pipeline - local simulator
----------------------------------------------------
tenants=500  queue_maxsize=4,000  window=1s
avg load=580 eps  burst load=5800 eps  processor_cap=4000 eps
policy=SHED (drop on full)

=== Scenario: avg (580 eps) then 10x burst (5,800 eps) ===
  duration (wall)      :   7.51 s
  produced             : 18,935
  processed            : 15,647
  dropped              : 3,288
  active windows       : 2,755 (window_id x tenant keys)
  throughput           : 2,084 events/sec
  latency p50          :   709.48 ms
  latency p95          :  1008.98 ms
  latency p99          :  1032.10 ms
  latency max          :  1036.68 ms
  latency mean         :   637.14 ms
  <5s p99 target       : PASS (p99 = 1.032 s)

----------------------------------------------------
RESULT: shedding policy shed 3,288 events under overload (explicit, counted, back-pressure-free).
Note: local demonstrator of design mechanics, not the AWS system.
```

## Benchmark: unthrottled consumer (flag check)

Command (confirms `--max-eps 0` and `--seconds` behave as documented):

```
python artifact/pipeline_sim.py --max-eps 0 --seconds 4
```

Output (abridged to the metrics):

```
processor_cap=unthrottled eps  policy=BACKPRESSURE (no drops)
  produced             : 12,747
  processed            : 12,747
  dropped              : 0
  throughput           : 3,037 events/sec
  latency p99          :     5.06 ms
  <5s p99 target       : PASS (p99 = 0.005 s)
```

With no processor cap the queue never saturates, so latency collapses to
single-digit milliseconds and there is nothing to shed. This isolates the
effect of the modeled finite processor capacity in the two runs above.

## Interpretation

- **Zero loss under backpressure.** With the processor capped below the burst
  rate, the bounded queue fills and the producer blocks. Every produced event is
  still processed (16,606 == 16,606, 0 dropped). The spike becomes latency, not
  loss. This is the AWS behaviour that Kinesis retention plus producer backoff
  provides.
- **Shedding is explicit and counted.** Under the same cap, `--shed` drops on a
  full queue and every drop is counted: 18,935 produced == 15,647 processed +
  3,288 shed. No event vanishes silently, which is the property the design
  relies on (per-tenant 429s under sustained overload, not blind loss).
- **p99 stays well under the 5s target** in both policies (~1.03-1.09 s here,
  driven by the 1-second tumbling window plus queue wait), and drops to ~5 ms
  when the processor is unthrottled. The relative behaviour is the point; the
  absolute milliseconds are in-process queueing, not AWS network cost.
- Throughput and exact counts vary a little run to run because the generator is
  clock-paced. The invariants (produced == processed with backpressure;
  produced == processed + shed with shedding; p99 < 5 s) hold every run and are
  asserted by the unit tests.
