#!/usr/bin/env python3
"""
pipeline_sim.py

Local, in-process simulation of the real-time analytics pipeline described in
../docs/ARCHITECTURE.md. It models the load-bearing mechanics of the AWS design
without any external services:

    generator  ->  bounded ingest queue (backpressure)  ->  stream processor
                                                              (tumbling-window
                                                               aggregation)

Goals demonstrated:
  * End-to-end (ingest-timestamp -> aggregation-visible) latency per event.
  * <5s p99 latency at the ~580 eps average load.
  * Survival of a 10x burst (~5,800 eps) with a BOUNDED buffer and an explicit,
    documented backpressure / shedding policy (default: no drops via blocking
    backpressure; optional --shed to demonstrate a load-shedding policy).
  * Reported p50 / p95 / p99 latency, throughput, and dropped-event count.

This is a DEMONSTRATOR of the design's mechanics, not the AWS system. See
artifact/README.md for honest caveats. Stdlib only. Runs in well under 30s.

Usage:
    python pipeline_sim.py                 # default scenario (avg + burst)
    python pipeline_sim.py --shed          # demonstrate load-shedding policy
    python pipeline_sim.py --seconds 8     # change duration
"""

import argparse
import collections
import queue
import random
import statistics
import threading
import time

# ---------------------------------------------------------------------------
# Tuning. The simulator is time-scaled: we do NOT literally sleep for a full
# minute per window. We use a short WINDOW_SECONDS so the run finishes fast,
# while preserving the mechanics (tumbling windows, backpressure, latency).
# ---------------------------------------------------------------------------

WINDOW_SECONDS = 1.0          # tumbling window size (represents "per-minute" in prod)
NUM_TENANTS = 500             # multi-tenant: 500+ customers
QUEUE_MAXSIZE = 4000          # bounded ingest buffer (backpressure boundary)
PROCESSOR_BATCH = 2000        # max events pulled per processor drain
AVG_EPS = 580                 # ~50M events/day
BURST_EPS = 5800              # 10x spike
EVENT_BYTES = (1024, 2048)    # ~1-2 KB/event (assumed; recorded, not used for latency)


class Event:
    __slots__ = ("tenant", "ingest_ts", "size")

    def __init__(self, tenant, ingest_ts, size):
        self.tenant = tenant
        self.ingest_ts = ingest_ts
        self.size = size


def generator(q, stop_event, eps_schedule, shed, stats):
    """
    Produce events according to a schedule of (duration_seconds, eps) phases.
    Backpressure model:
      * default   -> q.put(block=True) so the producer is throttled by the
                     consumer; no events are lost (mirrors Kinesis retry +
                     ProvisionedThroughputExceeded backoff on the producer).
      * --shed    -> q.put_nowait(); on queue.Full we drop and count the event
                     (mirrors an explicit edge load-shedding policy, e.g. return
                     429 at the API tier under sustained overload).
    """
    for phase_seconds, eps in eps_schedule:
        interval = 1.0 / eps
        phase_end = time.perf_counter() + phase_seconds
        next_emit = time.perf_counter()
        while time.perf_counter() < phase_end and not stop_event.is_set():
            now = time.perf_counter()
            if now < next_emit:
                # tiny spin/sleep to pace emission without busy-burning a core
                time.sleep(min(next_emit - now, 0.0005))
                continue
            ev = Event(
                tenant=random.randrange(NUM_TENANTS),
                ingest_ts=time.perf_counter(),
                size=random.randint(*EVENT_BYTES),
            )
            stats["produced"] += 1
            if shed:
                try:
                    q.put_nowait(ev)
                except queue.Full:
                    stats["dropped"] += 1
            else:
                q.put(ev, block=True)
            next_emit += interval
    stop_event.set()


def processor(q, stop_event, latencies, windows, stats, max_eps=None):
    """
    Stream processor: drains the bounded queue in batches, assigns each event to
    a tumbling window keyed by (window_id, tenant), and records end-to-end
    latency at the moment the event becomes visible in an aggregate.

    max_eps caps sustained consumer throughput. It models a stream processor
    (Flink/KDA) whose provisioned parallelism is finite: when producer load
    exceeds it, the bounded queue fills and the chosen policy (backpressure or
    shed) kicks in. When None, the consumer runs as fast as the host allows.
    """
    min_batch_dt = 0.0
    if max_eps:
        min_batch_dt = PROCESSOR_BATCH / float(max_eps)

    while True:
        drained = 0
        try:
            ev = q.get(timeout=0.05)
        except queue.Empty:
            if stop_event.is_set() and q.empty():
                break
            continue

        batch = [ev]
        while drained < PROCESSOR_BATCH:
            try:
                batch.append(q.get_nowait())
            except queue.Empty:
                break
            drained += 1

        batch_start = time.perf_counter()
        visible_ts = batch_start
        for e in batch:
            window_id = int(e.ingest_ts // WINDOW_SECONDS)
            windows[(window_id, e.tenant)] += 1
            latencies.append(visible_ts - e.ingest_ts)
            stats["processed"] += 1

        # Throttle to model finite processor capacity.
        if min_batch_dt:
            spent = time.perf_counter() - batch_start
            if spent < min_batch_dt:
                time.sleep(min_batch_dt - spent)


def run_scenario(seconds, shed, max_eps=None, seed=42):
    random.seed(seed)
    q = queue.Queue(maxsize=QUEUE_MAXSIZE)
    stop_event = threading.Event()
    latencies = collections.deque()
    windows = collections.defaultdict(int)
    stats = collections.Counter()

    # Schedule: half the run at average load, half at 10x burst.
    half = max(1.0, seconds / 2.0)
    eps_schedule = [(half, AVG_EPS), (half, BURST_EPS)]

    gen_t = threading.Thread(
        target=generator, args=(q, stop_event, eps_schedule, shed, stats), daemon=True
    )
    proc_t = threading.Thread(
        target=processor,
        args=(q, stop_event, latencies, windows, stats, max_eps),
        daemon=True,
    )

    t0 = time.perf_counter()
    proc_t.start()
    gen_t.start()
    gen_t.join()
    proc_t.join(timeout=10)
    wall = time.perf_counter() - t0

    return latencies, windows, stats, wall


def pct(sorted_vals, p):
    if not sorted_vals:
        return 0.0
    k = int(round((p / 100.0) * (len(sorted_vals) - 1)))
    return sorted_vals[k]


def report(name, latencies, windows, stats, wall):
    lat_ms = sorted(v * 1000.0 for v in latencies)
    processed = stats["processed"]
    print(f"\n=== {name} ===")
    print(f"  duration (wall)      : {wall:6.2f} s")
    print(f"  produced             : {stats['produced']:,}")
    print(f"  processed            : {processed:,}")
    print(f"  dropped              : {stats['dropped']:,}")
    print(f"  active windows       : {len(windows):,} (window_id x tenant keys)")
    if processed:
        print(f"  throughput           : {processed / wall:,.0f} events/sec")
        print(f"  latency p50          : {pct(lat_ms, 50):8.2f} ms")
        print(f"  latency p95          : {pct(lat_ms, 95):8.2f} ms")
        print(f"  latency p99          : {pct(lat_ms, 99):8.2f} ms")
        print(f"  latency max          : {lat_ms[-1]:8.2f} ms")
        print(f"  latency mean         : {statistics.fmean(lat_ms):8.2f} ms")
        p99 = pct(lat_ms, 99)
        ok = p99 < 5000.0
        print(f"  <5s p99 target       : {'PASS' if ok else 'FAIL'} "
              f"(p99 = {p99/1000.0:.3f} s)")
    return stats["dropped"]


def main():
    ap = argparse.ArgumentParser(description="Local streaming-pipeline simulator.")
    ap.add_argument("--seconds", type=float, default=6.0,
                    help="total run duration (avg phase + burst phase).")
    ap.add_argument("--shed", action="store_true",
                    help="use load-shedding (drop on full queue) instead of backpressure.")
    ap.add_argument("--max-eps", type=int, default=4000,
                    help="modeled sustained processor capacity (eps). The 5,800 "
                         "eps burst exceeds this so the buffer/backpressure path "
                         "is actually exercised. Set 0 to run consumer unthrottled.")
    args = ap.parse_args()
    max_eps = args.max_eps if args.max_eps and args.max_eps > 0 else None

    print("Real-time analytics pipeline - local simulator")
    print("-" * 52)
    print(f"tenants={NUM_TENANTS}  queue_maxsize={QUEUE_MAXSIZE:,}  "
          f"window={WINDOW_SECONDS:g}s")
    print(f"avg load={AVG_EPS} eps  burst load={BURST_EPS} eps  "
          f"processor_cap={max_eps or 'unthrottled'} eps")
    print(f"policy={'SHED (drop on full)' if args.shed else 'BACKPRESSURE (no drops)'}")

    latencies, windows, stats, wall = run_scenario(args.seconds, args.shed, max_eps)
    drops = report(
        "Scenario: avg (580 eps) then 10x burst (5,800 eps)",
        latencies, windows, stats, wall,
    )

    print("\n" + "-" * 52)
    if not args.shed and drops == 0:
        print("RESULT: bounded buffer + backpressure -> ZERO data loss under 10x burst.")
    elif args.shed:
        print(f"RESULT: shedding policy shed {drops:,} events under overload "
              f"(explicit, counted, back-pressure-free).")
    print("Note: local demonstrator of design mechanics, not the AWS system.")


if __name__ == "__main__":
    main()
