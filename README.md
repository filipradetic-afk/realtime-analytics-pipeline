# Real-Time Analytics Pipeline

**"Beat Claude" challenge, engineer-004. Candidate: Filip Radetić.**

A design for a real-time analytics pipeline for a Series B martech startup:
50M events/day, sub-5-second end-to-end latency, 10x spike resilience with zero
data loss, GDPR/CCPA/SOC 2 compliant, multi-tenant across 500+ customers, AWS
only, no SDK changes, under $50K/month, delivered by 2 engineers in a 3-month
MVP / 6-month full build.

- Deeper architecture, diagram, schema, tech trade-offs: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- Cost breakdown: [`docs/COST-MODEL.md`](docs/COST-MODEL.md)
- Source-labeled claims: [`docs/EVIDENCE-LOG.md`](docs/EVIDENCE-LOG.md)
- AI usage disclosure: [`docs/AI-DISCLOSURE.md`](docs/AI-DISCLOSURE.md)
- Runnable proof of the mechanics: [`artifact/pipeline_sim.py`](artifact/pipeline_sim.py)
  (unit tests: [`artifact/test_pipeline_sim.py`](artifact/test_pipeline_sim.py))
- Captured benchmark + test output: [`RESULTS.md`](RESULTS.md)

---

## 1. Problem framing

The existing pipeline batches events and serves analytics 15-30 minutes late.
The business needs interactive, near-real-time analytics without asking 500+
customers to touch their integrations. Three forces constrain the design and
they pull against each other:

- **Latency** (< 5s) wants continuous stream processing and a fast OLAP store.
- **Cost** (< $50K/mo) and **team size** (2 engineers) want managed services and
  as few stateful systems to operate as possible.
- **Compliance + no-breaking-changes** wants server-side control of tenancy, PII,
  and residency, so the frozen client contract stays frozen.

The design below is the point where those forces balance for *this* company, not
the maximally scalable design a FAANG team would build.

## 2. Chosen architecture (and why)

```
SDKs (unchanged)  ->  API Gateway/ALB + ingest service  ->  Kinesis Data Streams
   ->  Apache Flink (dedup + windowing)  ->  ClickHouse (hot, sub-second)
                                          \->  S3 + Iceberg (cold, replayable)
```

- **Ingest**: API Gateway/ALB in front of a stateless autoscaled service
  (Lambda or Fargate). The public endpoint keeps the exact same contract the
  SDKs already use. Tenant identity is derived server-side from the write key,
  never from the body, which is what lets us add multi-tenancy with zero client
  changes. PII is tokenized here, at the edge.
- **Backbone**: Kinesis Data Streams in on-demand mode. Managed, so 2 engineers
  do not operate a broker fleet; on-demand auto-scales shards through spikes; the
  retention window is a durable replay buffer.
- **Processing**: Apache Flink on Managed Service for Apache Flink. Real
  event-time windows, watermarks for late data, keyed per-tenant state, and
  exactly-once sinks. Micro-batch engines were rejected because their added
  seconds fight the sub-5s goal.
- **Serving**: ClickHouse for sub-second aggregations, with per-tenant isolation
  via sharding and row policies. S3 + Apache Iceberg is the cheap, durable system
  of record and the source for replay/backfill.
- **Correctness**: at-least-once transport plus DynamoDB-based idempotent dedup
  (`tenant_id:event_id`) gives *effective exactly-once at the aggregate level*
  without paying full end-to-end exactly-once latency.

Full technology table with rejected alternatives (Kinesis vs MSK, Flink vs Spark,
ClickHouse vs Pinot vs Druid) is in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## 3. Sub-5-second latency

Latency is budgeted per hop rather than hoped for. Client to API ~50-150 ms,
API to Kinesis ~20-80 ms, Kinesis to Flink ~200-800 ms (enhanced fan-out),
Flink windowing + sink ~0.5-2 s, ClickHouse insert to queryable ~0.2-1 s. That
sums under 5 s with margin. Levers: enhanced fan-out for low propagation delay,
short Flink emit cadence with incremental window emission, and ClickHouse async
inserts with a sub-second flush. All hop numbers are [estimated]; see the
Evidence Log. The [artifact](artifact/pipeline_sim.py) is the piece that is
actually measured: it holds p99 under 5s even with a saturated processor.

## 4. 10x spikes without data loss

Every stage has a bounded buffer and a policy for when it is full:

- **Ingest** is stateless and autoscales (Lambda concurrency / Fargate target
  tracking). Kinesis on-demand adds shards automatically.
- **Backpressure first**: when a downstream stage saturates, producers slow down
  rather than drop. Kinesis retention (24h+) absorbs the backlog as a durable
  buffer, so a spike becomes latency, not loss.
- **Shedding last, and counted**: only under sustained overload do we return
  per-tenant HTTP 429s, so one noisy tenant cannot starve the other 499. Nothing
  is dropped silently.
- **Failures are captured**: poison/parse failures go to an SQS DLQ plus an
  immutable S3 raw archive.

The [artifact](artifact/pipeline_sim.py) demonstrates both policies against a
580-then-5,800 eps profile: backpressure yields **zero drops** with bounded p99
(latest run: 16,606 produced == 16,606 processed, 0 dropped, p99 1.09 s), and
`--shed` shows an explicit, **counted** shed under a deliberately
under-provisioned processor (18,935 produced == 15,647 processed + 3,288 shed).
These invariants are asserted by [`artifact/test_pipeline_sim.py`](artifact/test_pipeline_sim.py)
(12 passing tests), and the full captured output is in [`RESULTS.md`](RESULTS.md).

## 5. Migration: zero breaking changes

1. **Shadow (dual-write)**: the ingest tier tees a copy of live traffic into the
   new Kinesis pipeline while the legacy batch pipeline stays authoritative. The
   tee is async and fire-and-forget: a failure or added latency in the new path
   can never slow or fail the legacy write, so the live contract cannot regress.
   No customer sees anything change. The endpoint contract is untouched.
2. **Parallel-run + reconcile**: run both pipelines and diff their outputs
   (counts, aggregates) per tenant. This validates the effective-exactly-once
   claim on real traffic and surfaces schema edge cases.
3. **Read cutover, per tenant**: flip dashboards to the new serving layer tenant
   by tenant behind a flag, starting with internal/design-partner tenants.
   Instant rollback is just flipping the flag back.
4. **Decommission** the legacy path only after a full reconciliation window with
   no material drift.

At no point does an SDK, pixel, or server integration change. The whole strategy
depends on the endpoint staying backwards-compatible, which is why tenancy and
PII handling were pushed server-side.

## 6. Multi-tenancy, GDPR/CCPA, SOC 2

- **Isolation**: `tenant_id` is the Kinesis partition-key prefix, the Flink state
  key, and the ClickHouse shard/row-policy key. Per-tenant API rate limits bound
  noisy-neighbour blast radius.
- **PII handling**: identifiers are tokenized at ingest; raw values live only in
  a regional token vault. Analytics run on tokens. IP is used to derive geo, then
  dropped (data minimization).
- **Right-to-erasure**: crypto-shredding first (drop the per-subject key in the
  token vault, so raw values are unrecoverable at once), then a scheduled purge of
  the pseudonymous rows in Iceberg + ClickHouse by `user_key`, because a stable
  token is still personal data under GDPR. Kinesis records age out via retention.
- **Data residency**: a `residency` field pins EU traffic to eu-central-1 streams,
  processing, and storage, and US traffic to us-east-1. No cross-region PII flow.
- **SOC 2**: KMS encryption at rest, TLS in transit, CloudTrail + immutable S3 raw
  log for audit, least-privilege IAM per service.

## 7. Cost model summary

Planning total is **~$43K/month**, under the $50K ceiling with ~14% headroom.
Largest lines: ClickHouse serving (~$7.5K), Kinesis on-demand (~$6.5K), Flink
(~$6K), ingest compute (~$3.5K), DynamoDB dedup (~$2.5K), plus data transfer,
observability, and a ~20% contingency. Every figure is [estimated] from public
AWS list pricing against the volume assumptions; none is benchmarked yet, and the
first MVP milestone is a real cost benchmark on a shadow traffic slice. Full table
and the levers that pull cost down: [`docs/COST-MODEL.md`](docs/COST-MODEL.md).

## 8. Trade-offs and risks

- **Kinesis on-demand vs MSK/Kafka**: chose managed simplicity and spike-safety
  over Kafka's lower unit cost at very high scale. Justified for 2 engineers at
  50M/day; MSK is the documented next step if sustained volume 10x's again.
- **At-least-once + dedup vs strict exactly-once**: chose correct-at-aggregate
  with lower latency and less operational fragility. Strict exactly-once via
  Flink two-phase-commit is reserved for the few billing-grade metrics that need
  it. Risk: dedup correctness depends on `event_id` uniqueness; validated in the
  parallel-run reconcile step.
- **ClickHouse vs Pinot/Druid**: chose the best ops/cost/latency balance for a
  small team. Pinot is the upgrade path if per-tenant concurrent QPS explodes.
- **Biggest budget risk**: the model assumes spikes are *bursty*. Sustained 10x
  load roughly doubles the three biggest lines and breaks $50K. Flagged honestly;
  mitigation is the MSK/provisioned-Kinesis lever.
- **Team risk**: 2 engineers running Kinesis + Flink + ClickHouse + DynamoDB is
  tight. Managed services during MVP, selective self-management later, and a hard
  bias toward fewer stateful systems is the mitigation.

## 9. 3-month MVP vs 6-month full system

**MVP (months 1-3), objective: prove sub-5s on real traffic without risk.**
- Shadow dual-write of live traffic into Kinesis; no read cutover.
- Flink job with tumbling windows + DynamoDB dedup for a handful of core event
  types; ClickHouse serving a small set of dashboards.
- Reconciliation harness (new vs legacy) and the **real cost benchmark**.
- Cut over internal + design-partner tenants only. GDPR tokenization + residency
  pinning in place from day one (not retrofitted).

**Full system (months 4-6), objective: all tenants, full compliance surface.**
- Per-tenant read cutover for all 500+ customers behind flags; decommission
  legacy after a clean reconciliation window.
- Full event-type coverage, session windows, late-data handling, DLQ + replay.
- Right-to-erasure automation, SOC 2 audit trail hardening, per-tenant isolation
  and rate limits at scale.
- Cost tuning against the benchmarked numbers (provisioned Kinesis lever, S3
  tiering, ClickHouse replication tuning).

---

*Artifact, evidence log, and AI disclosure are part of this submission by design:
the measured piece is small and honestly labeled, and the reasoning is meant to
be defensible in an interview rather than merely complete-looking.*
