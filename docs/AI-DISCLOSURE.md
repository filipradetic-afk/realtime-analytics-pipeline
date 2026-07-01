# AI Usage Disclosure

This submission was produced **AI-augmented**, and that is stated up front on
purpose. The challenge is about judgment under constraints and about what should
stay human, so hiding the tooling would miss the point.

## Who and how

The candidate, **Filip Radetić**, uses AI coding assistants (Claude Code, Cursor)
as a daily part of how he works. This artifact reflects that real workflow rather
than a sanitized version of it.

## What the AI did

- Drafted prose in the docs and README from a specified outline and constraints.
- Wrote the first pass of the Python simulator from an explicit spec (bounded
  queue, backpressure vs. shedding, tumbling windows, per-event latency).
- Produced the Mermaid diagram, the cost table scaffold, and the field tables.
- Cross-checked arithmetic (events/sec, monthly volume, cost subtotal).

## What was human judgment

- **The architecture decisions.** Kinesis over MSK for a 2-engineer team, Flink
  over Spark for sub-5s event-time semantics, ClickHouse over Pinot/Druid on the
  ops-cost-latency trade, at-least-once + dedup instead of paying for strict
  end-to-end exactly-once. These are opinionated calls with named rejected
  alternatives, not defaults an AI would pick unprompted.
- **The honesty framing.** Deciding that every cost number is [estimated] and
  none [benchmarked], that the artifact is a mechanics demonstrator and not the
  AWS system, and that sustained (vs. bursty) 10x load is the scenario that
  breaks the budget. Calling out weaknesses is a judgment choice.
- **The constraints that shape everything**: the $50K ceiling, 2 engineers, the
  "no SDK change" rule driving server-side tenancy derivation, and residency
  pinning for GDPR. These reframe generic best-practice into this specific brief.
- **Verification.** The simulator was run, its output inspected, and it was
  iterated until the shedding and backpressure paths were both genuinely
  exercised (the first version's processor was too fast to fill the buffer, so
  the demo was strengthened). That loop is human-directed.

## What was deliberately kept human (and why it matters here)

The parts a Series B company should not hand to an AR are exactly the parts
flagged as judgment above: the trade-off calls, the cost-risk honesty, and the
decision about which claims are evidence vs. assumption. An AI is excellent at
producing a confident, complete-looking design. The value a senior engineer adds
is knowing where that confidence is unearned and saying so in writing. This
document, and the [Evidence Log](EVIDENCE-LOG.md), are that.

## Reproducibility

The operating artifact is runnable and stdlib-only, so a reviewer can verify the
one measured claim in this submission independently:

```bash
python artifact/pipeline_sim.py
python artifact/pipeline_sim.py --shed
```
