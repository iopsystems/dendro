---
status: open
opened: 2026-09-11
updated: 2026-09-11
---

# The encoder boundary does not yet earn the crate's generality claim

## Goal

Decide how much of the crate's telemetry origin should be removed from its
schema and its trait, and how much is honestly part of what a segmented archive
is.

The crate claims to store timestamped opaque rows and to know nothing about what
a row means. Adversarial review found that claim true of the dependency graph
and overstated everywhere else.

## Decision Criteria

**GO on removing a field** when a second, non-telemetry caller exists and has to
fake it. Until then the evidence is one test faking it, which argues for the
claim being softened rather than the schema being churned.

**NO-GO on generalising the trait speculatively.** Every option below widens the
surface for a use case nobody has yet. The crate has one consumer; a second
would settle most of this in an afternoon of contact.

## Scope

In: the three telemetry-shaped schema objects, what `SegmentEncoder` cannot
express, and the claims in `README.md`/`DESIGN.md` that outrun the code.

Out: the seal seam and retention, which are the container proper and hold up.

## Evidence

**Three schema objects are telemetry's, not the container's.** `WalRow` spends
five lines of doc insisting `row` is opaque and says nothing about the `i64`
next to it. `wall_offset` is read by the writer, paired with the batch's newest
`last_ts`, and persisted as a `clock_offsets` row inside the seal transaction —
so dendro assigns a *meaning* (a wall-clock drift observation) to caller-supplied
data and derives a series from it. Add `sources.clock_anchor_wall_ns` and that
is a column on every row, a column on every source, and a whole table, all
load-bearing for one domain.

The proof of generality is the thing that shows it: `tests/roundtrip.rs` exists
to demonstrate a non-telemetry caller, and has to set `clock_anchor_wall_ns:
1_000` and `wall_offset: 0` for every row, and gets a `clock_offsets` series
written on its behalf that it never reads.

**"It never looks inside a segment" is false.** `ColumnFilter` takes
`arrow::datatypes::Field`, so arrow is in the public API of a crate that says it
does not know what a column is; `project_segment_columns` opens the BLOB with a
parquet reader and re-encodes it with the archive's own writer properties; and
`segment::writer_props` fixes LZ4_RAW and dictionary-off for every segment in
every archive, a choice the doc admits is wrong for string data and offers no
way to override. An encoder emitting anything but parquet works until someone
calls `CopySpec::keep_columns`, then fails at runtime. The honest statement of
the boundary is "rows → *parquet* segment".

**What the trait cannot express**, all concrete:

- More than one segment per call. `Option<Segment>` is singular, so a batch
  spanning two incompatible schemas must union, null-fill, or drop.
- State across segments. `&self`, and the symmetry rule *bans* it on principle
  — the reader has none of the writer's state — so a dictionary, a schema
  registry or a delta base is forbidden by contract, in a columnar format where
  those are the main wins.
- I/O. Sync, on the writer thread, behind a bound-1 channel: an encode that
  blocks stalls appends for every source in the archive.
- Which source it is encoding. There is no `source_id` parameter and one encoder
  per archive, yet multi-host and A/B are the headline multi-source cases.
  rezolus smuggles a type tag through the stream name (`key.contains('/')`)
  and backs it with a `debug_assert` at each producer, a test pinning that no
  sampler name contains `/`, and a "release-build backstop" resting on two
  msgpack shapes not aliasing. Four mechanisms compensating for one missing
  parameter.

**Symmetry is required and unversioned.** The writer and any reader must produce
identical bytes from identical rows, and nothing checks it. There is no encoder
version marker in the schema — `schema_version` is the *container's* — so two
processes on different encoder builds disagree about the tail with no symptom.
This is live rather than hypothetical: legacy archives hold WAL rows written by
an older producer and hand them to whatever encoder the reader supplies.

Two smaller ones, fixed while surveying rather than deferred: the encoder is
called with an empty slice on every read of a fully-sealed stream, which the
trait never said (now documented), and `read.rs` discarded `tail.rows` and
`tail.first_ts` — the two facts the trait exists to report — instead of checking
them against `live_wal_span`, which was one line away.

## Design and Implementation

Nothing built. The shape of each answer:

- **`wall_offset` / `clock_anchor_wall_ns`** could move into
  `SourceMeta::metadata`, already a free-form map, leaving the container with no
  opinion about clocks. That costs the `clock_offsets` series its home, and that
  series is genuinely useful to the one caller there is.
- **`writer_props`** could be per-archive configuration, which is small and
  obviously right if anyone ever wants zstd or dictionaries.
- **A source parameter on `encode`** is a one-line signature change and would
  retire four compensating mechanisms downstream. The cheapest item here.
- **An encoder version marker** in `sources.metadata`, refused on mismatch, is
  the smallest thing that turns a silent disagreement into an error.

## Outcome

Open. The two fixable-in-passing items are done; the schema and trait questions
are untouched.

Interim: the claims have been narrowed to what is true rather than the code
being changed to match them. That is the right order — a claim is cheap to fix
and a schema is not — but it is worth being explicit that this entry records
the gap rather than closing it.

## Deferred or Reopen Items

- **Two items came off this list.** The encoder version marker landed with
  [container hardening](2026-09-11-container-hardening.md); `writer_props`
  became per-copy configuration rather than a fixed choice. The schema
  objects (`wall_offset`, `clock_anchor_wall_ns`, `clock_offsets`) and the
  trait's expressiveness are untouched.
- **Reopen** when a second non-telemetry caller exists. It will settle which of
  these are real and which were theoretical, which one round of review cannot.
- A source parameter on `encode` does not need to wait for that.
- Related: [writer failure blast
  radius](2026-09-11-writer-failure-blast-radius.md) covers what happens when an
  encoder returns `Err`.

## Appendix: Skills Invoked

- `engineering-journal` — this entry.
