# Evidence Log

Every quantitative or load-bearing claim in this submission, labeled by source
class with a one-line basis. Label meanings:

- **[observed]** measured directly from a running system in this submission.
- **[benchmarked]** measured from a purpose-built benchmark (none of the AWS
  numbers are benchmarked yet; the local artifact is the only measured piece).
- **[estimated]** derived by arithmetic or from public reference figures.
- **[assumed]** a stated planning assumption, to be validated during the MVP.

## Volume and load

| Claim | Value | Class | Basis |
| ----- | ----- | ----- | ----- |
| Event volume | 50M events/day | [assumed] | Challenge brief. |
| Average throughput | ~580 events/sec | [estimated] | 50,000,000 / 86,400 s = 578.7. |
| 10x spike throughput | ~5,800 events/sec | [estimated] | 10 x 580. |
| Event size | 1-2 KB/event | [assumed] | Typical martech event payload; not measured on real traffic. |
| Monthly events | ~1.5 billion | [estimated] | 50M x 30. |
| Monthly ingress | ~2-3 TB | [estimated] | 1.5B x ~1.5 KB average. |
| Customers / tenants | 500+ | [assumed] | Challenge brief. |

## Latency (design targets)

| Claim | Value | Class | Basis |
| ----- | ----- | ----- | ----- |
| End-to-end target | < 5 s p99 | [assumed] | Challenge requirement. |
| Client -> ingest hop | 50-150 ms | [estimated] | Typical TLS + auth + validate + tokenize at API tier. |
| Ingest -> Kinesis put | 20-80 ms | [estimated] | Batched `PutRecords` round trip. |
| Kinesis -> Flink read | 200-800 ms | [estimated] | Enhanced fan-out propagation range from AWS guidance. |
| Flink window + sink | 0.5-2 s | [estimated] | Dominated by window emit cadence + checkpoint. |
| ClickHouse insert -> queryable | 0.2-1 s | [estimated] | Async insert with short flush interval. |
| Sum of budget | < 5 s with headroom | [estimated] | Sum of hops above; leaves margin. |

## Artifact (measured locally)

| Claim | Value | Class | Basis |
| ----- | ----- | ----- | ----- |
| Simulator p99 latency (backpressure) | ~1.0-1.1 s | [observed] | `python pipeline_sim.py`, single run; varies by host. Captured runs in RESULTS.md. |
| Simulator drops (backpressure) | 0 | [observed] | Bounded queue with blocking `put`; producer throttled. |
| Simulator drops (shed) | ~3,000-3,400 | [observed] | `--shed`; counted drops on full queue under burst. |
| Simulator throughput | ~2,000-2,400 eps | [observed] | Processor capped via `--max-eps 4000`; GIL-bound single process. |
| Run time | < 10 s | [observed] | Wall clock; ~7.5 s per run on Windows / Python 3.13. |
| Unit tests | 12 pass | [observed] | `python -m unittest discover -s artifact -p "test_*.py"`; stdlib only. |
| p99 < 5 s target met | PASS | [observed] | Both policies in the artifact; see RESULTS.md. |

Note: the artifact's absolute latencies are in-process queueing times, **not** a
prediction of AWS production latency. They demonstrate mechanics (bounded buffer
prevents loss; saturated processor raises but bounds latency; shedding is
explicit and counted). This is [observed] behaviour of the simulator, not
[benchmarked] behaviour of the AWS system.

## Cost

| Claim | Value | Class | Basis |
| ----- | ----- | ----- | ----- |
| Total infra | ~$43K/month | [estimated] | Sum of COST-MODEL.md line items from public AWS list pricing + volume assumptions. |
| Under $50K ceiling | yes, ~14% headroom | [estimated] | $42.96K vs $50K. |
| Kinesis on-demand premium | ~20-40% vs provisioned | [assumed] | General AWS pricing relationship; not benchmarked on this workload. |
| Every line item | see COST-MODEL.md | [estimated] | None [benchmarked]; MVP milestone includes a real cost benchmark. |

## Semantics and reliability

| Claim | Class | Basis |
| ----- | ----- | ----- |
| At-least-once + dedup = effective exactly-once at aggregates | [assumed] | Standard streaming pattern; correctness depends on `event_id` uniqueness and DynamoDB conditional writes. To be validated with a correctness test in MVP. |
| Kinesis retention provides replay buffer | [estimated] | AWS retention feature (24h default, up to 365d). |
| DynamoDB single-digit-ms lookups | [estimated] | AWS published latency characteristics; not measured on this workload. |
| ClickHouse sub-second OLAP at this scale | [estimated] | Public ClickHouse benchmarks on comparable event data; not benchmarked here. |
