#!/usr/bin/env python3
"""
Unit tests for pipeline_sim.py.

Stdlib `unittest` only, no third-party dependencies. The tests are seeded,
fast (short durations, small event counts), and Windows-safe (they rely on
thread joins and counted invariants, never on wall-clock magnitudes).

Run:
    python -m unittest discover -s artifact -p "test_*.py" -v
or, from inside artifact/:
    python -m unittest test_pipeline_sim -v
"""

import os
import sys
import unittest

# Make the module importable whether tests are run from the repo root
# (discover -s artifact) or from inside the artifact/ directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pipeline_sim as sim  # noqa: E402


class TestWindowedAggregation(unittest.TestCase):
    """Windowing math is deterministic and independent of threads/clocks."""

    def test_window_id_left_closed_right_open(self):
        # window size 1.0: [0,1) -> 0, [1,2) -> 1
        self.assertEqual(sim.window_id_for(0.0, 1.0), 0)
        self.assertEqual(sim.window_id_for(0.999, 1.0), 0)
        self.assertEqual(sim.window_id_for(1.0, 1.0), 1)
        self.assertEqual(sim.window_id_for(2.5, 1.0), 2)

    def test_known_input_expected_counts(self):
        # Hand-built events with explicit timestamps and tenants so the
        # expected per-(window, tenant) counts are known by construction.
        # window_seconds = 1.0
        events = [
            sim.Event(tenant=0, ingest_ts=0.10, size=1),  # win 0, tenant 0
            sim.Event(tenant=0, ingest_ts=0.90, size=1),  # win 0, tenant 0
            sim.Event(tenant=1, ingest_ts=0.50, size=1),  # win 0, tenant 1
            sim.Event(tenant=0, ingest_ts=1.20, size=1),  # win 1, tenant 0
            sim.Event(tenant=1, ingest_ts=1.99, size=1),  # win 1, tenant 1
            sim.Event(tenant=1, ingest_ts=2.00, size=1),  # win 2, tenant 1
        ]
        counts = sim.aggregate_windows(events, window_seconds=1.0)
        expected = {
            (0, 0): 2,
            (0, 1): 1,
            (1, 0): 1,
            (1, 1): 1,
            (2, 1): 1,
        }
        self.assertEqual(dict(counts), expected)

    def test_total_count_conserved(self):
        # Every event lands in exactly one window/tenant bucket.
        events = [
            sim.Event(tenant=i % 3, ingest_ts=i * 0.4, size=1) for i in range(50)
        ]
        counts = sim.aggregate_windows(events, window_seconds=1.0)
        self.assertEqual(sum(counts.values()), len(events))

    def test_window_size_changes_bucketing(self):
        events = [
            sim.Event(tenant=0, ingest_ts=0.5, size=1),
            sim.Event(tenant=0, ingest_ts=1.5, size=1),
            sim.Event(tenant=0, ingest_ts=2.5, size=1),
        ]
        # 1s windows -> three distinct windows for tenant 0
        self.assertEqual(len(sim.aggregate_windows(events, 1.0)), 3)
        # 5s window -> all three collapse into window 0
        counts5 = sim.aggregate_windows(events, 5.0)
        self.assertEqual(len(counts5), 1)
        self.assertEqual(counts5[(0, 0)], 3)


class TestBackpressureNoLoss(unittest.TestCase):
    """Default policy: producer blocks on a full queue, nothing is dropped."""

    def test_zero_drops_and_produced_equals_processed(self):
        latencies, windows, stats, wall = sim.run_scenario(
            seconds=0.6, shed=False, max_eps=2000, seed=7
        )
        self.assertEqual(stats["dropped"], 0)
        self.assertEqual(stats["produced"], stats["processed"])
        self.assertGreater(stats["processed"], 0)
        # Aggregation buckets account for every processed event.
        self.assertEqual(sum(windows.values()), stats["processed"])


class TestSheddingCounted(unittest.TestCase):
    """Shed policy: drops are explicit and counted, never silently lost."""

    def test_no_silent_loss_produced_equals_processed_plus_shed(self):
        # Under-provision the processor hard so the burst overflows the queue
        # and the shed path is actually exercised.
        latencies, windows, stats, wall = sim.run_scenario(
            seconds=0.6, shed=True, max_eps=500, seed=7
        )
        self.assertGreater(stats["dropped"], 0, "burst should force shedding")
        # The accounting identity: nothing vanishes silently.
        self.assertEqual(
            stats["produced"], stats["processed"] + stats["dropped"]
        )
        self.assertEqual(sum(windows.values()), stats["processed"])


class TestLatencyMetrics(unittest.TestCase):
    """Latency is measured; percentiles are ordered and well-formed."""

    def test_pct_ordering_and_positive(self):
        latencies, windows, stats, wall = sim.run_scenario(
            seconds=0.6, shed=False, max_eps=2000, seed=11
        )
        self.assertTrue(latencies)
        lat_ms = sorted(v * 1000.0 for v in latencies)
        p50 = sim.pct(lat_ms, 50)
        p95 = sim.pct(lat_ms, 95)
        p99 = sim.pct(lat_ms, 99)
        # Finite and positive.
        for v in (p50, p95, p99):
            self.assertTrue(v == v, "percentile is NaN")          # not NaN
            self.assertNotEqual(v, float("inf"))
            self.assertGreater(v, 0.0)
        # Ordered.
        self.assertLessEqual(p50, p95)
        self.assertLessEqual(p95, p99)

    def test_pct_empty_is_zero(self):
        self.assertEqual(sim.pct([], 99), 0.0)

    def test_percentiles_helper_matches_pct(self):
        vals = [0.001 * i for i in range(1, 101)]
        helper = sim.percentiles(vals, ps=(50, 95, 99))
        ordered = sorted(vals)
        self.assertEqual(helper[50], sim.pct(ordered, 50))
        self.assertEqual(helper[95], sim.pct(ordered, 95))
        self.assertEqual(helper[99], sim.pct(ordered, 99))


class TestDeterminism(unittest.TestCase):
    """Same seed -> same aggregate results (on the seeded, clock-free path)."""

    def test_same_seed_same_aggregates(self):
        # The threaded generator is clock-paced, so its event *count* varies by
        # host; determinism lives in the seeded RNG. make_events() exposes that
        # reproducible core: same seed must yield byte-identical aggregates.
        e1 = sim.make_events(2000, num_tenants=8, seed=123)
        e2 = sim.make_events(2000, num_tenants=8, seed=123)
        self.assertEqual(
            dict(sim.aggregate_windows(e1, 1.0)),
            dict(sim.aggregate_windows(e2, 1.0)),
        )

    def test_different_seed_diverges(self):
        e1 = sim.make_events(2000, num_tenants=8, seed=1)
        e2 = sim.make_events(2000, num_tenants=8, seed=2)
        self.assertNotEqual(
            dict(sim.aggregate_windows(e1, 1.0)),
            dict(sim.aggregate_windows(e2, 1.0)),
        )

    def test_aggregate_windows_is_pure_and_stable(self):
        events = [
            sim.Event(tenant=(i * 7) % 5, ingest_ts=i * 0.13, size=1)
            for i in range(200)
        ]
        a = sim.aggregate_windows(events, 1.0)
        b = sim.aggregate_windows(events, 1.0)
        self.assertEqual(dict(a), dict(b))


if __name__ == "__main__":
    unittest.main(verbosity=2)
