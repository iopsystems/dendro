---
status: open
opened: 2026-09-12
updated: 2026-09-12
---

# Generations: telling a counter reset from a counter wrap

## Goal

Decide how a consumer of an archive distinguishes a cumulative counter that
was **reset** from one that **wrapped**, and what each layer — producer,
container, query engine — owns of that.

Today nothing distinguishes them, because from the values alone they are
identical: both are a counter that went down. Every consumer in this
ecosystem assumes reset, which is correct for a restart and silently wrong,
by up to the counter's full width, for an overflow.

## Decision Criteria

**GO on a per-counter generation** when a producer in this ecosystem exposes
a counter narrow enough to wrap in practice, or one it deliberately zeroes
without restarting. Both exist: a hardware performance counter is commonly
48 bits, several kernel counters are 32, and "zero this on read" is an
ordinary sampler idiom.

**NO-GO on the container computing any of it.** dendro stores a row as
opaque bytes and cannot see a counter, let alone its width. Anything here
that reads a value is the container learning what a row means, which is the
one thing it must not do — see [the encoder
boundary](2026-09-11-encoder-boundary.md).

**NO-GO on inferring width from observed values.** "It dropped from near
2^32, so it wrapped" is a heuristic that fails exactly where it matters: a
counter reset from a high value looks the same, and a wrap of a counter that
was not near its maximum (because the interval spanned more than one wrap)
looks like neither.

## Scope

In: what a generation is, where it lives, what granularity it needs, and
what each layer does with it.

Out: implementing it. Nothing here is built, in this repo or any other. This
entry exists so the design is recorded once rather than re-derived by whoever
next notices that a rate is too small.

## Evidence

**The two cases are arithmetically different and observationally identical.**
For an observation `prev` followed by `cur` with `cur < prev`:

| cause | true increment |
|---|---|
| reset | `cur` |
| wrap of a `w`-bit counter | `cur + (2^w - prev)` |

Nothing in `(prev, cur)` selects between them.

**Everything here assumes reset.** metriken-query's `RateMode::Grid` is
documented as using "the reset-adjusted cumulative counter", and its test
`test_grid_rate_counter_reset` pins the behaviour: the series
`100, 200, 300, 50, 150` yields increments `100, 100, 50, 100`. The
`300 → 50` step contributes **50**. Had that been a 32-bit wrap, the true
increment was `50 + (2^32 − 300)` ≈ **4.29 × 10⁹**. The engine is not wrong
to choose a default; it is that there is nothing for it to choose *from*.

**The error is systematic, not noise.** Treating a wrap as a reset
undercounts by `2^w − prev` every time it happens, and a counter that wraps
does so regularly. Treating a reset as a wrap overcounts by about `2^w`
once. For a counter sampled often enough to wrap between samples, the
undercount is unbounded — each wrap loses another period.

**There is a third case, and a generation is the only thing that catches
it.** A counter reset to zero that counts past its previous value before the
next observation shows **no drop at all**. The series looks monotonic, the
delta looks plausible, and the interval silently undercounts by `prev`. No
value-based heuristic can see this one, because there is nothing anomalous
in the values. A generation change is visible whether or not the value
dropped, which is what makes it strictly more than a better wrap detector.

**Granularity has to be per counter, not per source.** A process restart
zeroes every counter at once, and the source-scoped
[`keys::PRODUCER_EPOCH`](../../src/lib.rs) already covers that. A single
counter wrapping, or a single counter the producer zeroes on read, does not
restart the process — so a source-scoped epoch says nothing about it. The
two levels nest and both are needed:

| level | what it says | where it lives |
|---|---|---|
| source epoch | every counter in this source restarted | `sources.metadata`, `producer_epoch` |
| counter generation | *this* counter restarted | the row — the encoder's bytes |

**Width has to travel too.** Wrap arithmetic needs `w`. A producer that
wants wraps interpreted correctly must carry the counter's width alongside
its value, for the same reason it must carry the generation: the container
cannot see either.

## Design and Implementation

Nothing built. The shape, by layer:

**Producer.** Each cumulative counter carries, alongside its value, a
generation and a width. The generation changes when and only when the
producer zeroes that counter — deliberately, or by being restarted. It need
not be a UUID: a monotonically increasing integer per counter is smaller and
strictly more useful, since a consumer can then tell "one generation was
missed" from "many were", and a comparison for ordering is meaningful. A
UUID is the right shape only at the source level, where two *different*
producers must not collide.

**Container (dendro).** Carries both opaquely, inside the row, and does
nothing with them. The one change here is documentary: `keys::PRODUCER_EPOCH`
currently says the epoch is regenerated "whenever its cumulative counters
start from zero", which reads as though the source-level key covers the
per-counter case. It does not, and the wrap distinction is not mentioned at
all. Fixed in this entry's commit, along with `FORMAT.md` §6.

**Query engine.** Given a generation and a width, the rule per consecutive
pair:

- generation unchanged, `cur >= prev` → increment `cur − prev`.
- generation unchanged, `cur < prev` → **wrap**: increment
  `cur + (2^w − prev)`.
- generation changed → **reset**: increment `cur`, and the interval is known
  to be a partial observation — whatever the counter reached before it was
  zeroed is lost, which is unavoidable and should be reported rather than
  hidden.

Absent a generation, the current heuristic stays exactly as it is. That is
what makes this additive: an archive whose rows carry no generation reads
today's answer, and one whose rows do reads a better one.

**Why not just widen every counter to 64 bits.** It is the right answer where
the producer owns the counter — a 64-bit counter at 10⁹/s wraps in ~584
years — and it is not available where the producer is *reading* someone
else's: a 48-bit hardware PMU counter is 48 bits no matter what the sampler
stores it in, and the wrap happens below the sampler. Widening also does not
address the deliberate-zeroing case or the invisible reset.

## Outcome

Open. The design is recorded and the container's documentation now states
the distinction correctly; no generation is emitted, carried in any row, or
consumed by any engine.

## Deferred or Reopen Items

- **Reopen in the producer** when a sampler exposes a counter it zeroes, or
  one narrower than 64 bits whose wrap it cannot rule out. That is where the
  work starts; the container and the engine can only use what the producer
  sends.
- **The source-level half is already reserved and still unemitted.**
  `producer_epoch` and `producer_epochs` have keys, documentation and a
  writer primitive in dendro, and nothing writes them. A producer that starts
  here gets restart detection for every counter at once, which is the larger
  half of the problem for a process-scoped producer, at much lower cost than
  per-counter generations.
- Related: [the encoder boundary](2026-09-11-encoder-boundary.md) is the
  entry that owns "what may live in a row and what the container may know".

## Appendix: Skills Invoked

- `engineering-journal` — this entry.
