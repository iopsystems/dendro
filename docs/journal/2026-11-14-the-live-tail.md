---
status: open
opened: 2026-11-14
updated: 2026-11-15
---

# The live tail: liveness from a watermark, and rows anchored to seals

## Goal

Decide whether the live tail, the WAL rows a stream has not sealed, should
change shape, and which of four problems with it to fix.

The tail exists so that a process can stop, or be killed, without losing
rows: every append is durable when its tick commits, and a reader on the same
host sees it (DESIGN.md, "Why a WAL at all"). That property holds and this
entry does not question it. What it questions is how the format decides which
WAL rows are live, and what a WAL row has to contain to be read.

## Decision Criteria

**A. Delete a seal's rows inside the seal transaction.** GO when a seal with
the delete inside is no slower, by median of at least 15 interleaved trials,
than today's seal plus its prunes, beyond the difference between the medians
of two arms of today's seal at the same size. Measured on macOS below, for one
stream and for a batch of 12: met, and faster for the batch of small streams.
Not measured on Linux; GO was taken without it (see Outcome). The prediction
there is that IN beats SEP for 12 streams of 100 rows by about 12 commit
times. IN no faster than SEP beyond the floor refutes the mechanism; IN
slower beyond the floor fails the criterion.

**B. Make a WAL row decodable from its own bytes plus state that retention
does not delete.** GO when a design moves the anchors out of the rows with
bytes written to the SQLite `-wal` file per tick (the metric in DESIGN.md,
"What a row costs to write") rising by no more than 5% for metriken-storage's
writer, against metriken 55c3c27 on the same input. The input, a recorded
rezolus capture with its stream count and tick count, is chosen when B is
designed; DESIGN.md's 156,920 bytes per tick is a synthetic workload with no
anchors and is not the baseline. NO-GO if it needs dendro to
interpret a row's payload. Unmeasured; nothing is designed yet.

**C. Whether the encoder version should gate reads of sealed segments.** An
owner's decision, with no cost claim. Today it does, by design: the version
covers "the bytes in a row and the columns in a segment" (`src/segment.rs:198`).
FORMAT.md §6 says the version is enforced, with a reader on a different
version refused, and that it versions the row encoding. Narrowing the check
to the live tail changes `src/segment.rs:198` and §6's refusal sentence; §6's
"row encoding" fits either reading.

**D. Retention never deletes an unsealed row.** Decision only: either dendro
guarantees it, or FORMAT.md keeps it as the caller's rule.

## Scope

In: the `wal` table, the watermark predicate, the seal and its prune,
retention's treatment of live rows, the encoder version check, and the parts
of metriken-storage's encoder that exist because of how its rows are written.

Out: the SQLite container, Parquet segments, the catalog, and the replication
wire, except where B would change what a `Sync` frame does. The flat layout
for finished archives is
[its own entry](2026-09-24-two-forms-live-and-finalized.md), and the trait's
generality is [the encoder boundary](2026-09-11-encoder-boundary.md).

## Evidence

### A. Liveness is computed from the watermark

A WAL row is live when its timestamp is above its stream's newest sealed row
(`LIVE_WAL_PREDICATE`, `src/archive/sqlite.rs:500`; FORMAT.md §3.3). The rule
is what makes a crash between the seal's commit and its prune harmless: rows
a segment covers stay in `wal` and are hidden.

The comment on the prune (`src/writer.rs:3468-3473`) says "a quiet stream
accumulates thousands of rows before it seals, so pruning inside the seal
commit puts a large delete on the tick path", and that the watermark "leaves
the prune a pure background optimization". The delete is large (below). It
is on the tick path either way: `seal_batch` calls `prune_wal` on the writer
thread right after the commit, and the writer takes the next tick only after
`seal_batch` returns. `seal_batch` seals a batch in one transaction so that
"a dozen implicit commits" are not "a dozen fsyncs" (`src/writer.rs:3431`),
then calls `prune_wal` once per stream (`src/writer.rs:3480-3487`), and each
`prune_wal` at the watermark is an autocommit of its own
(`src/archive/sqlite.rs:3028-3033`). So the separate prune adds one commit per
sealed stream and a crash window, and moves no work. What it does buy is that
a reader sees the new segment earlier, by the length of the delete. The
comment's argument is the same as in the initial commit, 61c99ee, where the
prune was also synchronous; later commits changed only its wording.

`sync_stream` prunes the same way, outside its transaction, and only logs a
failure (`src/writer.rs:3270-3277`). `sync_stream`'s own transaction and
`adopt_segment_after` already delete WAL rows inside a transaction
(`src/writer.rs:3243-3245`, `src/writer.rs:3333`). A prune that fails after a
committed seal, on a `Msg::Seal`, ends the writer when the error is not
retryable (`seal_batch` returns it, and the caller returns it at
`src/writer.rs:2450`); in `retry_deferred_seals` before a retention pass it
is logged instead (`src/writer.rs:3033-3036`). A retryable one is retried by
`with_retries` (`src/writer.rs:1633`), which runs the whole `seal_batch`
again (`src/writer.rs:2419`). The retry finds no live rows for the streams
already sealed, returns `Ok`, and leaves the shadowed rows of the stream
whose prune failed, and of the streams after it in the batch, in `wal` until
a later seal or retention deletes them. Only when every attempt fails are
the streams marked deferred.

Measured at b7e94a7, on an Apple M4 Max (16 cores, macOS 26.6.2), APFS on the
internal SSD, not idle. Each run is one session, 15 trials per arm, arm order
rotated each trial, a fresh archive per trial through `ArchiveMut::create`
(the writer's pragmas: `synchronous=FULL`, a 4 MiB `wal_autocheckpoint`).
dendro sets no `fullfsync`, so a commit on macOS is a plain `fsync`, which
does not flush the drive's cache; that is why this host cannot show what a
commit costs on Linux. Rows are 1,925 bytes (DESIGN.md's mixed workload), and
the archive is checkpointed before the seal. A segment is N × 1,925 / 8 bytes,
an assumed 8:1 compression; real segment sizes were not measured. SEP is
today's shape: the segments' transaction, then one `prune_wal` per stream. IN
deletes the rows with `Transaction::delete_wal_range` inside the
transaction. AA is SEP again, for the floor. Each trial checks that the
sealed streams' `wal` rows are gone. Times are writer-thread wall time in
milliseconds from opening the segments' transaction to the end of the last
prune; they exclude the `live_wal` read and the encode that precede it in
`seal_batch`. Medians of 15 (measured); "prune" is SEP's time after its
commit.

One stream of N rows, beside 25 streams of 200 rows (load averages 3.68,
3.21, 3.13 at the start):

| N | SEP | AA | IN | SEP's prune |
| --- | --- | --- | --- | --- |
| 1,000 | 1.01 | 1.02 | 0.93 | 0.59 |
| 4,000 | 3.25 | 3.27 | 3.26 | 2.03 |
| 16,000 | 23.12 | 23.04 | 22.94 | 18.59 |

A batch of 12 streams of N rows each, sealed in one transaction, beside 14
streams of 200 rows (load averages 8.23, 6.11, 4.32 at the start; heavier
than the first run):

| N per stream | SEP | AA | IN | SEP's prune |
| --- | --- | --- | --- | --- |
| 100 | 2.22 | 2.22 | 1.23 | 1.66 |
| 1,000 | 14.06 | 14.03 | 14.05 | 10.29 |

The difference between SEP and AA medians is 0.001 to 0.09 ms across both runs (derived). For
one stream, IN's median is below SEP's at 1,000 rows by 0.08 ms (IN's median,
0.93 ms, is below SEP's fastest trial, 0.944 ms; SEP's slowest was a 7.64 ms
outlier) and at 16,000 by 0.18 ms, and 0.02 ms above it at 4,000, inside that
size's floor of 0.028 ms. For the batch of 12 streams of 100 rows, IN takes
45% less time, 0.99 ms less; SEP's prune phase there is 1.66 ms. At 1,000
rows per stream the arms are equal. Read as: on this host IN is never slower
beyond the floor, and is faster for 12 small streams, where the saved commits
are the larger part of the prune (inferred from the difference).

The delete is large: SEP's prune is 58% to 62% of the one-stream time at
1,000 and 4,000 rows. At 16,000 it is 80%, but about 6 ms of it is the 4 MiB
autocheckpoint, which runs in whichever commit crosses the threshold. A rerun
of the one-stream module at b7e94a7 with two more arms, SEP0 and IN0, which
set `PRAGMA wal_autocheckpoint=0` after the fill (12 trials per arm, no AA
arm), gave SEP's prune as 18.4 ms with the autocheckpoint and 12.1 ms
without, of a 16.6 ms SEP0 total; IN0 was 16.55 ms against SEP0's 16.61 ms,
with no floor measured for that pair. The 12-stream run at 1,000 rows writes
about 2.9 MB of segments and may cross the threshold too; it was not
separated.

What A removes and what it does not:

- A file written only by a writer that deletes inside the transaction at
  both prune sites, `seal_batch` (`src/writer.rs:3480`) and `sync_stream`
  (`src/writer.rs:3274`), never holds a shadowed row, so `wal` rows and live
  rows are the same set.
- The post-commit prune failures above go away: there is no prune after the
  commit to fail.
- A reader still needs the predicate for any file an older writer touched,
  since that writer may have crashed inside the window. Dropping the
  predicate from readers needs a schema version, or a writer that removes
  shadowed rows on open and records that it has, which a reader checks
  before skipping the predicate.
- The rule that drops a row at or below its stream's newest row stays:
  a reader tailing a stream by timestamp has passed it (FORMAT.md §3.3).
- `adopt_segment`'s refusals of a straddling segment, and of a segment over
  an unsealed row, stay: they keep a stream in time order.
- Nothing else found relies on shadowed rows. Every WAL delete site at
  b7e94a7 (`prune_wal`, `delete_wal_range`, `DELETE FROM wal`) is one of
  those above or retention's, and retention deletes segments and WAL rows in
  one transaction. `copy_sources_into` copies `live_wal` only
  (`src/rewrite.rs:920`), a merged segment takes its last input's `last_ts`
  (`src/rewrite.rs:338`), and a resumed source does not prune.
- FORMAT.md §3.3 and the comment at `src/writer.rs:3468-3474` state the
  prune-outside rule, and change with it.

### B. metriken-storage's rows are anchored to seal boundaries

metriken-storage's writer puts a group's schema, a long table's columns and a
sampler cell's metadata only on the first row since the last seal that needs
them (`metriken-storage/src/encoder.rs:201-208`, metriken 55c3c27). It tracks
dendro's seal boundaries to do so (`sealing`, `metriken-storage/src/writer.rs:873`).
A row after the first is decodable only with that first row. What follows
from it, in dendro and in metriken-storage:

- `SegmentEncoder::encode_after` exists so a split seal can give each part
  the rows before it (`src/segment.rs:217`), and metriken-storage carries
  three `anchor_*` functions to re-anchor a part (`encoder.rs:381`, 402, 425).
- `read::stream_range` returned nothing from the live tail for such an
  encoder from 0.1.0, until the fix listed under Unreleased in CHANGELOG.md,
  which ships in 0.8.0 (#82).
- An unsealed run whose first row retention deletes cannot be decoded. The
  encoder claims the skipped rows as encoded (`encoder.rs:289`). Once a later
  anchored row seals with them, the prune deletes them and no segment holds
  them. Until then they stay live and are skipped on every read. A warning is
  logged per materialization (`metriken-storage/src/wal.rs:341-347`) and no
  counter moves. metriken-storage seals a stream before retention can cut its
  first row, so this needs a file a metriken-storage 0.2.2 writer left, or a deferred seal
  whose retry before an eviction failed with an error that is not retryable
  (the comment at `encoder.rs:289-301` lists both).
- A replica's `Sync` seals its live rows at a boundary of its own and deletes
  the rows its encoder refuses (WIRE.md §4.7, `sync_stream`,
  `src/writer.rs:3156`). I do not know how many of those refusals come from
  anchoring rather than from an incomplete trailing record.

The [encoder boundary entry](2026-09-11-encoder-boundary.md) records that
the trait bans state across segments because a reader has none of the
writer's state. A schema stated once per seal is state across rows of the
tail, carried in the rows, and it depends on where the seals fell.

One shape for B, not evaluated: anchors stored once per hash in
`caller_rows` (FORMAT.md §3.5), which compaction does not delete, and each row
naming its anchor's hash. Whether retention of `caller_rows` can then delete
an anchor a live row still names is the first question to answer.

### C. The encoder version check covers sealed data

`check_encoder` runs before anything is read in `read::probe`
(`src/read.rs:229`), `read::read_archive` (`src/read.rs:322`),
`read::stream_range` (256), `read::stream_segments` (360),
`read::stream_indexes` (439) and `rewrite::copy_sources_into`
(`src/rewrite.rs:848`). `tests/encoder_version.rs:66` pins that a finalized
source written with encoder `v1` is refused to a reader reporting `v2`.
dendro's read paths call the encoder only for a live tail
(`materialize_tail` returns early when there are no live rows,
`src/read.rs:375`), so the refusal of a sealed source is a statement about
the segments' columns rather than a need of the read. Narrowing it would let
a reader built for one encoding receive another's columns with no error.

rezolus reads through metriken-storage's reader, which does its own check
(`metriken-storage/src/reader.rs:1996`) against `READABLE_ENCODERS`, which
today holds only `metriken-archive/1` (`encoder.rs:25`), so it refuses a
sealed recording from another version, as dendro does. At rezolus
a58d5ab2, these call `dendro::rewrite::copy_sources_into`: hindsight dumps,
filter, `recording snapshot` (`src/parquet_tools/mod.rs:45`) and
`edit_target` (`src/parquet_tools/legacy.rs:301`), all through
`src/dendro_copy.rs`; combine (`src/parquet_tools/combine.rs`); and
Save-as-Report (`crates/report-save/src/lib.rs`). So dendro's check at
`src/rewrite.rs:848` applies to them, and narrowing C changes them.

### D. Retention deletes unsealed rows

FORMAT.md §3.3: eviction deletes by timestamp, a live row below the cutoff is
deleted though no segment holds it, `Evicted::live_rows` counts it, and the
caller must seal at least as often as it evicts. The writer seals deferred
streams before a retention pass (`retry_deferred_seals`, `src/writer.rs:3001`).
A stream whose retry fails with a retryable error is left out of the pass,
and so is every deferred stream after it, without a retry
(`src/writer.rs:3014-3016`); one whose retry fails with an error that is not
retryable has its live rows evicted (`src/writer.rs:2590-2640`).

## Plan

1. Measure A on Linux with both tests, for one stream and for a batch of 12.
   Skipped; see Outcome.
2. Decide A's reader story: a schema version, or prune-on-open. Decided:
   neither; see Outcome.
3. Decide C.
4. Design B with metriken-storage, starting from the question above.
5. Decide D.

Whether 0.8.0 (#82) waits for any of this is open. A changes nothing on the
wire. B could change what a `Sync` frame has to delete.

## The benchmark

As run, with its comments and one assertion message removed and one `let` inlined. Appended to the end of
`src/archive/sqlite.rs` in a scratch checkout, and run with
`BENCH_DIR=<dir> TRIALS=15 cargo test --release --lib bench_prune::bench_prune_inside -- --ignored --nocapture --test-threads=1`,
and the batch with
`BENCH_DIR=<dir> TRIALS=15 cargo test --release --lib bench_prune_batch -- --ignored --nocapture --test-threads=1`.
The batch run is a second module, `bench_prune_batch`, that differs in four
places: 12 quiet streams `q00` to `q11` and 14 other streams; the
transaction inserts a segment for each quiet stream (and in IN deletes its
rows); SEP calls `prune_wal` once per quiet stream after the commit; and N
is 100 and 1,000.

```rust
#[cfg(test)]
mod bench_prune {
    use super::*;
    use crate::archive::{SegmentMeta, WalRow};
    use std::time::Instant;

    const OTHER_STREAMS: usize = 25;
    const OTHER_ROWS: i64 = 200;
    const PAYLOAD: usize = 1925;

    fn fill(dir: &std::path::Path, n: i64) -> (ArchiveMut, i64) {
        let mut db = ArchiveMut::create(&dir.join("a.db")).unwrap();
        let sid = db
            .insert_source(&SourceMeta {
                labels: [("k".to_string(), "v".to_string())].into(),
                metadata: Default::default(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        let payload = vec![7u8; PAYLOAD];
        db.transaction(|tx| {
            for s in 0..OTHER_STREAMS {
                let rows: Vec<WalRow> = (0..OTHER_ROWS)
                    .map(|t| WalRow { stream: format!("s{s:02}"), ts: t, wall_offset: 0, row: payload.clone() })
                    .collect();
                tx.insert_wal_rows(sid, &rows)?;
            }
            let rows: Vec<WalRow> = (0..n)
                .map(|t| WalRow { stream: "quiet".into(), ts: t, wall_offset: 0, row: payload.clone() })
                .collect();
            tx.insert_wal_rows(sid, &rows)
        })
        .unwrap();
        db.conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
        (db, sid)
    }

    fn seal(db: &mut ArchiveMut, sid: i64, n: i64, inside: bool) -> (f64, f64) {
        let meta = SegmentMeta { rows: n as u64, first_ts: 0, last_ts: n - 1 };
        let bytes = vec![3u8; (n as usize) * PAYLOAD / 8];
        let t0 = Instant::now();
        db.transaction(|tx| {
            tx.insert_segment(sid, "quiet", 0, &meta, &bytes)?;
            if inside {
                tx.delete_wal_range(sid, "quiet", None, Some(n - 1))?;
            }
            Ok(())
        })
        .unwrap();
        let t1 = Instant::now();
        if !inside {
            db.prune_wal(sid, "quiet", n - 1).unwrap();
        }
        let t2 = Instant::now();
        assert_eq!(db.read_wal(sid, "quiet").unwrap().len(), 0);
        let ms = |d: std::time::Duration| d.as_secs_f64() * 1e3;
        (ms(t2 - t0), ms(t2 - t1))
    }

    #[test]
    #[ignore]
    fn bench_prune_inside_versus_after_the_seal() {
        let base = std::path::PathBuf::from(std::env::var("BENCH_DIR").unwrap());
        let trials: usize = std::env::var("TRIALS").map(|s| s.parse().unwrap()).unwrap_or(15);
        for n in [1_000i64, 4_000, 16_000] {
            for trial in 0..trials {
                let mut arms = ["SEP", "IN", "AA"];
                arms.rotate_left(trial % 3);
                for arm in arms {
                    let dir = base.join(format!("n{n}-t{trial}-{arm}"));
                    std::fs::create_dir_all(&dir).unwrap();
                    let (mut db, sid) = fill(&dir, n);
                    let (total, after) = seal(&mut db, sid, n, arm == "IN");
                    println!("RESULT n={n} trial={trial} arm={arm} total_ms={total:.3} after_commit_ms={after:.3}");
                    drop(db);
                    std::fs::remove_dir_all(&dir).unwrap();
                }
            }
        }
    }
}
```

## Outcome

Open. A is done; B, C and D are not started.

A, decided 2026-11-15 and implemented in the change that adds
`tests/seal_atomic.rs`:

- `seal_batch` deletes each sealed stream's WAL rows up to its newest
  segment's `last_ts` inside the seal's transaction, and `sync_stream`
  deletes the stream's whole WAL inside its transaction. Neither calls
  `prune_wal` any more.
- Readers keep `LIVE_WAL_PREDICATE`. Archives written before the change can
  hold covered rows, and so can one assembled through `ArchiveMut`'s
  low-level writes, so no schema version and no prune-on-open.
- Not measured on Linux (plan step 1). The change removes one autocommit per
  sealed stream and adds no statement to the transaction beyond the deletes
  those autocommits ran, so I expect it to cost no more there than on macOS;
  that is inferred, not measured.
- `a_seal_whose_wal_delete_fails_commits_no_segment` makes the delete fail
  with a trigger and checks that no segment lands. Run against the previous
  `src/writer.rs` it fails: the error is `failed to prune WAL for a:
  refused`, from the prune after the commit. Against the change it passes.
- `a_sync_whose_covered_row_delete_fails_changes_nothing_and_a_retry_deletes_them`
  (`tests/replicate_resume.rs`) does the same for a `Sync`: the copy holds
  covered rows, a trigger refuses deleting them, and the sync must fail with
  the copy unchanged. Against the previous `src/writer.rs` the sync succeeds,
  because the failed prune after its commit only logged a warning.
