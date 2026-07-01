# Cost Model

Target: stay under **$50,000/month** for infrastructure at 50M events/day with
headroom for 10x spikes. All figures are US-East on-demand style pricing as a
planning baseline. Every number is labeled by source class. Prices drift, so
treat this as a defensible estimate to be re-benchmarked during the MVP, not a
quote.

## Baseline assumptions

| Assumption | Value | Source |
| ---------- | ----- | ------ |
| Events/day | 50,000,000 | [assumed] challenge brief |
| Events/sec (avg) | ~580 | [estimated] 50M / 86,400 |
| Events/sec (10x spike) | ~5,800 | [estimated] 10x average |
| Bytes/event (wire) | 1-2 KB | [assumed] typical martech event |
| Monthly events | ~1.5 billion | [estimated] 50M x 30 |
| Monthly ingress volume | ~2-3 TB | [estimated] 1.5B x 1.5 KB |
| Spike frequency | short, bursty, not sustained | [assumed] traffic pattern |

## Monthly cost breakdown

| Component | Config | Est. $/mo | Source class | Basis / note |
| --------- | ------ | --------: | ------------ | ------------ |
| API Gateway (HTTP API) or ALB | ~1.5B requests | 1,800 | [estimated] | HTTP API ~$1.00-1.11 / M req; ALB path is cheaper at this volume, used as ceiling here. |
| Ingest compute (Fargate or Lambda) | steady + burst autoscale | 3,500 | [estimated] | ~4-8 vCPU steady, bursts on spikes; Fargate right-sizing assumed. |
| Kinesis Data Streams (on-demand) | ~1.5B records, ~2-3 TB in | 6,500 | [estimated] | On-demand: per-GB ingest + per-payload-unit puts + fan-out; on-demand chosen for spike safety over cheaper provisioned. |
| Managed Service for Apache Flink (KDA) | ~8-16 KPUs | 6,000 | [estimated] | ~$0.11/KPU-hr; sized for burst parallelism + checkpoint overhead. |
| DynamoDB (dedup state) | on-demand, TTL 24h | 2,500 | [estimated] | ~1.5B conditional writes + reads; TTL keeps table small. |
| ClickHouse (hot serving) | 3-node cluster (EC2) + EBS | 7,500 | [estimated] | ~3x r6i.4xlarge + gp3; self-managed to control cost vs ClickHouse Cloud. |
| S3 (raw + Iceberg cold) | growing ~2-3 TB/mo | 1,500 | [estimated] | Storage + PUT/GET + lifecycle to IA/Glacier for old raw. |
| Athena (ad-hoc/batch) | modest scan volume | 800 | [estimated] | $5/TB scanned; Iceberg partition pruning keeps scans small. |
| Data transfer + NAT + inter-AZ | cross-AZ streaming | 2,500 | [estimated] | Inter-AZ and NAT are a real, often underestimated line item. |
| KMS, CloudTrail, GuardDuty, Config (SOC 2) | security/compliance baseline | 1,200 | [estimated] | Encryption, audit, monitoring required for SOC 2. |
| Observability (CloudWatch + Grafana/Prometheus) | metrics, logs, traces | 2,000 | [estimated] | Log volume at this scale is non-trivial. |
| **Subtotal** | | **35,800** | | |
| Contingency / spike buffer (~20%) | | 7,160 | [estimated] | Absorbs sustained spikes, hot-partition growth, price drift. |
| **Total** | | **~42,960** | | **Under the $50K ceiling with ~14% headroom.** |

## Cost trade-off notes

- **Kinesis on-demand vs provisioned**: on-demand is ~20-40% more expensive per
  unit but removes the need to pre-provision for 10x spikes and removes a whole
  class of `ProvisionedThroughputExceeded` incidents. For a 2-engineer team that
  reliability-per-dollar trade is worth it in year one. [assumed] Switching hot
  paths to provisioned + auto-scaling is a documented cost lever once traffic
  patterns are known.
- **ClickHouse self-managed vs ClickHouse Cloud / managed OLAP**: self-managed
  on EC2 is materially cheaper but costs engineering time. With 2 seniors, plan
  for managed initially during MVP, migrate to self-managed for steady-state
  savings once load patterns stabilize. [assumed]
- **Biggest levers if we approach the ceiling**: (1) move steady-state Kinesis to
  provisioned, (2) tier old raw S3 data to Glacier aggressively, (3) reduce
  ClickHouse replication factor for non-critical tenants, (4) shorten DynamoDB
  dedup TTL. Each is reversible.
- **What could blow the budget**: sustained (not bursty) 10x load would roughly
  double Kinesis + Flink + ClickHouse lines and push over $50K. The design
  assumes spikes are short. If sustained high load becomes the norm, MSK/Kafka
  becomes cheaper than Kinesis and is the documented next step. [assumed]

All dollar figures above are **[estimated]** planning numbers derived from public
AWS list pricing and the volume assumptions in this table. They are explicitly
**not [benchmarked]**; the first MVP milestone includes a real cost benchmark on
a shadow slice of production traffic.
