// Drives the writer and reads the counter it keeps.
#![cfg(all(feature = "write", feature = "test-support"))]

//! An append at or below its stream's newest row is dropped and said out
//! loud. Below the newest sealed row it would be invisible to every read
//! path; below the newest live row a reader tailing the stream by timestamp
//! has already passed it.
//!
//! The bound is per `(source, stream)`, so this is a constraint WITHIN a
//! stream and nowhere else: a different stream, or a different source, has
//! its own bound and takes the same timestamp happily. That is the
//! property these tests pin, because it is what makes the restriction
//! acceptable rather than merely documented.

use std::collections::BTreeMap;

use dendro::archive::{Archive, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        let ts: Vec<String> = rows.iter().map(|r| r.ts.to_string()).collect();
        Ok(Some(Segment {
            bytes: ts.join(",").into_bytes(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

fn meta(name: &str) -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), name.to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn row(stream: &str, ts: i64) -> WalRow {
    WalRow {
        stream: stream.to_string(),
        ts,
        wall_offset: 0,
        row: vec![1],
    }
}

/// Every stream of every source, as the encoder rendered it.
fn read_back(path: &std::path::Path) -> BTreeMap<String, Vec<String>> {
    let db = Archive::open(path).unwrap();
    let mut out = BTreeMap::new();
    for src in read::read_archive(&db, &Tags).unwrap() {
        for (stream, segments) in src.streams {
            out.insert(
                format!("{}/{stream}", src.labels["source"]),
                segments
                    .into_iter()
                    .map(|b| String::from_utf8(b).unwrap())
                    .collect(),
            );
        }
    }
    out
}

#[test]
fn an_append_below_the_sealed_watermark_is_dropped_and_counted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Tags), meta("a")).unwrap();

    w.wal(vec![row("s", 100)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 0, "nothing late yet");

    // At the watermark, and below it: both unreadable, so both dropped.
    w.wal(vec![row("s", 100)]).unwrap();
    w.wal(vec![row("s", 50)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 2);

    // Past it: taken.
    w.wal(vec![row("s", 150)]).unwrap();
    w.sync().unwrap();
    assert_eq!(
        w.dropped_out_of_order(),
        2,
        "an in-order append is not a drop"
    );

    // And the archive holds exactly the two rows it can serve — the dropped
    // ones are not in the WAL either, which is the point: before this they
    // were stored, forever, and readable by nobody.
    let db = Archive::open(&path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    assert_eq!(
        db.read_wal(id, "s")
            .unwrap()
            .into_iter()
            .map(|r| r.ts)
            .collect::<Vec<_>>(),
        vec![150],
        "the raw WAL holds no row that cannot be read"
    );
    drop(db);
    archive.finalize_single(w, (150, 0)).unwrap();
    assert_eq!(read_back(&path)["a/s"], vec!["100", "150"]);
}

/// One late row among good ones costs only itself. The tick still commits,
/// and the rest of it lands.
#[test]
fn a_late_row_does_not_take_its_tick_with_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Tags), meta("a")).unwrap();
    w.wal(vec![row("s", 100)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.sync().unwrap();

    w.wal(vec![row("s", 50), row("s", 200), row("s", 300)])
        .unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 1);
    archive.finalize_single(w, (300, 0)).unwrap();
    assert_eq!(
        read_back(&path)["a/s"],
        vec!["100", "200,300"],
        "the two good rows of that tick are there"
    );
}

/// The restriction is per `(source, stream)` and nowhere else. A sibling
/// stream and a second source take the very timestamp the sealed stream just
/// refused — which is what makes "append in order" a per-stream contract
/// rather than an archive-wide clock.
#[test]
fn a_sibling_stream_and_another_source_are_unaffected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut a = archive.add_source(meta("a")).unwrap();
    let mut b = archive.add_source(meta("b")).unwrap();

    // `a/ahead` runs on and seals at 1000.
    a.wal(vec![row("ahead", 1000)]).unwrap();
    a.seal(vec!["ahead".to_string()]).unwrap();
    a.sync().unwrap();

    // The same timestamp, three ways.
    a.wal(vec![row("ahead", 10)]).unwrap(); // same stream: dropped
    a.wal(vec![row("behind", 10)]).unwrap(); // sibling stream: kept
    b.wal(vec![row("ahead", 10)]).unwrap(); // other source: kept
    a.sync().unwrap();
    b.sync().unwrap();

    assert_eq!(
        a.dropped_out_of_order(),
        1,
        "only the sealed stream's own row"
    );
    assert_eq!(b.dropped_out_of_order(), 0);

    a.finalize((1000, 0)).unwrap();
    b.finalize((10, 0)).unwrap();
    archive.join().unwrap();

    let got = read_back(&path);
    assert_eq!(got["a/ahead"], vec!["1000"]);
    assert_eq!(
        got["a/behind"],
        vec!["10"],
        "a sibling stream has its own watermark"
    );
    assert_eq!(got["b/ahead"], vec!["10"], "and so does another source");
}

/// A RESUMED source gets the stronger guarantee, and gets it at the handle:
/// `resume_source` hands back a floor — the newest row anywhere in the
/// source — and an append at or below it is a synchronous typed error rather
/// than a drop the caller has to go and count.
///
/// That floor is at or above every one of the source's stream watermarks, so
/// for a resumed source it subsumes the check below entirely. The writer
/// still seeds its watermarks from the catalog on reopen (see
/// `Archive::sealed_watermarks`); this records that the floor is what a caller
/// actually meets, so nobody reads the seeding as the thing doing the work.
#[test]
fn a_resumed_source_is_refused_at_the_handle_which_is_stronger() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Tags), meta("a")).unwrap();
    let id = w.source_id();
    w.wal(vec![row("s", 100)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    archive.finalize_single(w, (100, 0)).unwrap();

    let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
    let (mut w, floor) = archive.resume_source(id, 101).unwrap();
    assert_eq!(floor, Some(100));
    match w.wal(vec![row("s", 60)]).unwrap_err() {
        dendro::Error::TimelineBackwards { ts, floor, .. } => {
            assert_eq!(
                (ts, floor),
                (60, 100),
                "refused, by value, before anything is sent"
            );
        }
        other => panic!("expected TimelineBackwards, got {other}"),
    }
    assert_eq!(w.dropped_out_of_order(), 0, "refused, not dropped");
    archive.finalize_single(w, (101, 0)).unwrap();
    assert_eq!(read_back(&path)["a/s"], vec!["100"]);
}

/// The seeded watermark itself: what a reopened writer reads out of the
/// catalog is each stream's newest sealed row, per source.
#[test]
fn sealed_watermarks_are_per_source_and_per_stream() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut a = archive.add_source(meta("a")).unwrap();
    let mut b = archive.add_source(meta("b")).unwrap();
    let (ia, ib) = (a.source_id(), b.source_id());
    a.wal(vec![row("fast", 100), row("slow", 5)]).unwrap();
    a.seal(vec!["fast".to_string(), "slow".to_string()])
        .unwrap();
    b.wal(vec![row("fast", 7)]).unwrap();
    b.seal(vec!["fast".to_string()]).unwrap();
    a.sync().unwrap();
    b.sync().unwrap();
    // An unsealed stream contributes no watermark at all.
    a.wal(vec![row("unsealed", 999)]).unwrap();
    a.sync().unwrap();

    let marks = Archive::open(&path).unwrap().sealed_watermarks().unwrap();
    assert_eq!(marks[&ia]["fast"], 100);
    assert_eq!(
        marks[&ia]["slow"], 5,
        "a slow stream keeps its own, lower watermark"
    );
    assert!(
        !marks[&ia].contains_key("unsealed"),
        "nothing sealed, nothing to compare against"
    );
    assert_eq!(
        marks[&ib]["fast"], 7,
        "another source's stream of the same name is its own"
    );
    drop(a);
    drop(b);
    archive.join().unwrap();
}

/// Rows of one stream that arrive out of order within one tick are kept:
/// the bound is the stream's newest row before the tick, and the encoder
/// orders what it is given.
#[test]
fn rows_out_of_order_within_one_tick_are_kept() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Tags), meta("a")).unwrap();

    w.wal(vec![row("s", 300), row("s", 100)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 0, "one tick, no bound yet");
    archive.finalize_single(w, (300, 0)).unwrap();

    let db = Archive::open(&path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    assert_eq!(db.live_wal(id, "s").unwrap().len(), 2, "both are readable");
}

/// A writer's own retention pass that evicts every segment of a stream
/// leaves its bound where it was: a row at or below the stream's old newest
/// row is dropped and counted, and one above it is taken and readable. The
/// bound used to be what the file held, so after the pass a row below the
/// evicted ones was taken, and a tailing publisher whose cursor was past it
/// never shipped it.
#[test]
fn an_eviction_of_every_segment_keeps_the_stream_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Tags), meta("a")).unwrap();
    for ts in [10, 20] {
        w.wal(vec![row("s", ts), row("t", ts)]).unwrap();
        w.seal(vec!["s".to_string(), "t".to_string()]).unwrap();
    }
    w.sync().unwrap();

    // `s` loses both segments; `t` keeps its newer one.
    let only_s: Box<dyn Fn(&str) -> bool + Send> = Box::new(|s: &str| s == "s");
    w.evict_streams_before(100, only_s).unwrap();
    w.wal(vec![row("s", 15), row("t", 15)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 2, "both are at or below 20");
    w.wal(vec![row("s", 25)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 2);

    let db = Archive::open(&path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    let live: Vec<i64> = db.live_wal(id, "s").unwrap().iter().map(|r| r.ts).collect();
    assert_eq!(live, vec![25]);
    drop(db);
    archive.finalize_single(w, (25, 0)).unwrap();
}

/// A row at or below its stream's newest LIVE row is dropped and counted,
/// the same as one at or below the newest sealed row. It used to be stored
/// and readable, and a tailing replication publisher, which ships rows after
/// the newest it has sent, never shipped it. Within one tick a stream's rows
/// may arrive in any order; only a second row at one timestamp is dropped.
#[test]
fn a_row_at_or_below_the_newest_live_row_is_dropped_and_counted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Writer::single(&path, Box::new(Tags), meta("a")).unwrap();

    w.wal(vec![row("s", 100)]).unwrap();
    w.wal(vec![row("s", 50)]).unwrap();
    w.wal(vec![row("s", 100)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 2, "50 and the second 100");

    // One tick, out of order within it: both taken. A repeat in it: dropped.
    w.wal(vec![row("s", 300), row("s", 200), row("s", 300)])
        .unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 3);

    // Another stream is unaffected.
    w.wal(vec![row("t", 50)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 3);

    let db = Archive::open(&path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    let ts = |stream: &str| -> Vec<i64> {
        db.read_wal(id, stream)
            .unwrap()
            .into_iter()
            .map(|r| r.ts)
            .collect()
    };
    assert_eq!(ts("s"), vec![100, 200, 300]);
    assert_eq!(ts("t"), vec![50]);
    drop(db);
    archive.finalize_single(w, (300, 0)).unwrap();
}

/// A discard keeps its stream's bound: a row at or below the discarded rows
/// is dropped and counted. The bound used to be what the file held, so such
/// a row was taken, and a tailing publisher whose cursor was past it never
/// shipped it. The next session's floor includes the bound, so the resumed
/// handle refuses such a row outright.
#[test]
fn a_discard_keeps_the_newest_row_bound_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta("a")).unwrap();
    w.wal(vec![row("s", 10), row("s", 100)]).unwrap();
    w.sync().unwrap();
    w.discard_live_rows(Box::new(|s: &str| s == "s")).unwrap();
    w.wal(vec![row("s", 10)]).unwrap();
    w.wal(vec![row("s", 100)]).unwrap();
    w.sync().unwrap();
    assert_eq!(
        w.dropped_out_of_order(),
        2,
        "10 and 100 are at or below the discarded 100"
    );
    drop(w);
    archive.join().unwrap();

    let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
    let id = Archive::open(&path).unwrap().read_sources().unwrap()[0].id;
    let (mut w, floor) = archive.resume_source(id, 1_000).unwrap();
    assert_eq!(
        floor,
        Some(100),
        "the floor was read back from the file's bounds"
    );
    assert!(matches!(
        w.wal(vec![row("s", 50)]),
        Err(dendro::Error::TimelineBackwards { floor: 100, .. })
    ));
    w.wal(vec![row("s", 101)]).unwrap();
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();

    let db = Archive::open(&path).unwrap();
    let live: Vec<i64> = db.live_wal(id, "s").unwrap().iter().map(|r| r.ts).collect();
    assert_eq!(live, vec![101]);
}

/// The offline delete paths, which run without a writer, record the bound
/// too: `ArchiveMut::discard_live_rows`, `ArchiveMut::evict_streams_before`
/// and a `prune_wal` past the stream's watermark. Each stream here is the
/// only one holding the 100, so the next session's floor shows each.
#[test]
fn an_offline_delete_records_the_bound_for_the_next_writer() {
    use dendro::archive::ArchiveMut;
    for (stream, delete) in [("d", 0), ("e", 1), ("p", 2)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("o.dendro");
        let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
        let mut w = archive.add_source(meta("a")).unwrap();
        w.wal(vec![row("keep", 10), row(stream, 100)]).unwrap();
        w.sync().unwrap();
        drop(w);
        archive.join().unwrap();

        let mut db = ArchiveMut::open(&path).unwrap();
        let id = db.read_sources().unwrap()[0].id;
        match delete {
            0 => drop(db.discard_live_rows(id, &|s: &str| s == stream).unwrap()),
            1 => drop(
                db.evict_streams_before(id, 1_000, &|s: &str| s == stream)
                    .unwrap(),
            ),
            _ => drop(db.prune_wal(id, stream, 1_000).unwrap()),
        }
        drop(db);

        let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
        let (w, floor) = archive.resume_source(id, 1_000).unwrap();
        assert_eq!(floor, Some(100), "{stream}: the deleted 100 is the floor");
        drop(w);
        archive.join().unwrap();
    }
}

/// `resume_source` refuses an anchor at or below the newest row the source
/// has held, not only below what it holds now. A source evicted to nothing
/// used to have no floor at all, and a writer whose clock went backwards
/// then had every row dropped one at a time instead of being refused once.
#[test]
fn a_resume_below_an_evicted_source_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta("a")).unwrap();
    w.wal(vec![row("s", 1_000)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.sync().unwrap();
    w.evict_before(10_000).unwrap();
    drop(w);
    archive.join().unwrap();

    let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
    let id = Archive::open(&path).unwrap().read_sources().unwrap()[0].id;
    let refused = archive.resume_source(id, 500).unwrap_err();
    assert!(
        matches!(
            refused,
            dendro::Error::TimelineBackwards { floor: 1_000, .. }
        ),
        "{refused:?}"
    );
    archive.resume_source(id, 1_001).unwrap();
    archive.join().unwrap();
}

/// A copy carries each stream's bound: a time-bounded copy that left a
/// stream's newest rows out still has them as its floor, as its original
/// would. A stream the copy filters out carries no bound.
#[test]
fn a_bounded_copy_keeps_the_original_bound() {
    use dendro::archive::ArchiveMut;
    use dendro::rewrite::{copy_sources_into, CopySpec};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta("a")).unwrap();
    w.wal(vec![row("s", 10), row("t", 10)]).unwrap();
    w.seal(vec!["s".to_string(), "t".to_string()]).unwrap();
    // Live tails wholly past the copy's `end`, so the copy leaves them out.
    w.wal(vec![row("s", 100), row("t", 200)]).unwrap();
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();

    let src = Archive::open(&path).unwrap();
    let copy_with = |keep: &dyn Fn(&str) -> bool| -> Option<i64> {
        let copy = dir.path().join("c.dendro");
        let _ = std::fs::remove_file(&copy);
        let mut dst = ArchiveMut::create(&copy).unwrap();
        let spec = CopySpec {
            end: 50,
            keep_streams: Some(keep),
            ..CopySpec::everything()
        };
        dst.transaction(|tx| copy_sources_into(&src, tx, &spec, &Tags))
            .unwrap();
        drop(dst);
        let mut archive = Writer::open(&copy, Box::new(Tags)).unwrap();
        let id = Archive::open(&copy).unwrap().read_sources().unwrap()[0].id;
        let (w, floor) = archive.resume_source(id, 1_000).unwrap();
        drop(w);
        archive.join().unwrap();
        floor
    };
    assert_eq!(
        copy_with(&|s: &str| s == "s"),
        Some(100),
        "`s` carries 100, not 10"
    );
    assert_eq!(
        copy_with(&|s: &str| s != "t"),
        Some(100),
        "`t`'s 200 is not carried"
    );
}

/// A later writer seeds each stream's bound from the file's `bounds` too, so
/// `Writer::wal_tick` on a source it did not resume drops a row at or below
/// a discarded one.
#[test]
fn a_tick_on_a_source_not_resumed_respects_the_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta("a")).unwrap();
    let id = w.source_id();
    w.wal(vec![row("s", 10)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    w.wal(vec![row("s", 100)]).unwrap();
    w.sync().unwrap();
    w.discard_live_rows(Box::new(|s: &str| s == "s")).unwrap();
    drop(w);
    archive.join().unwrap();

    let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
    archive
        .wal_tick(vec![(id, vec![row("s", 50), row("s", 150)])])
        .unwrap();
    archive.join().unwrap();

    let db = Archive::open(&path).unwrap();
    let live: Vec<i64> = db.live_wal(id, "s").unwrap().iter().map(|r| r.ts).collect();
    assert_eq!(live, vec![150]);
}

/// `Transaction::delete_segment` records the bound as well: deleting a
/// stream's newest segment leaves it as the floor.
#[test]
fn deleting_the_newest_segment_keeps_its_bound() {
    use dendro::archive::ArchiveMut;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta("a")).unwrap();
    for ts in [10, 100] {
        w.wal(vec![row("s", ts)]).unwrap();
        w.seal(vec!["s".to_string()]).unwrap();
    }
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();

    let mut db = ArchiveMut::open(&path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    db.transaction(|tx| tx.delete_segment(id, "s", 1)).unwrap();
    drop(db);

    let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
    let (w, floor) = archive.resume_source(id, 1_000).unwrap();
    assert_eq!(floor, Some(100));
    drop(w);
    archive.join().unwrap();
}

/// A stream the original has evicted to nothing exists only in its
/// `bounds`, and a copy carries that bound too.
#[test]
fn a_copy_carries_the_bound_of_a_stream_evicted_to_nothing() {
    use dendro::archive::ArchiveMut;
    use dendro::rewrite::{copy_sources_into, CopySpec};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let copy = dir.path().join("c.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta("a")).unwrap();
    w.wal(vec![row("keep", 10), row("gone", 100)]).unwrap();
    w.seal(vec!["gone".to_string()]).unwrap();
    w.sync().unwrap();
    w.evict_streams_before(1_000, Box::new(|s: &str| s == "gone"))
        .unwrap();
    drop(w);
    archive.join().unwrap();

    let src = Archive::open(&path).unwrap();
    let mut dst = ArchiveMut::create(&copy).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Tags))
        .unwrap();
    drop(dst);
    let mut archive = Writer::open(&copy, Box::new(Tags)).unwrap();
    let id = Archive::open(&copy).unwrap().read_sources().unwrap()[0].id;
    let (w, floor) = archive.resume_source(id, 1_000).unwrap();
    assert_eq!(
        floor,
        Some(100),
        "the evicted stream's 100, not `keep`'s 10"
    );
    drop(w);
    archive.join().unwrap();
}
