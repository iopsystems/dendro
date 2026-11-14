// Drives the writer and reads the counter it keeps.
#![cfg(all(feature = "write", feature = "test-support"))]

//! A caller-row name's rows append in `ts` order. Several may share the
//! newest `ts`, and within one call they may arrive in any order, but a row
//! below the newest is dropped and counted. After retention removes a
//! name's newest rows, the next row must be above them. That order is what
//! gives a caller row a position a replication subscriber can resume from:
//! its `ts` and its ordinal among the rows at that `ts`.

use std::collections::BTreeMap;

use dendro::archive::{Archive, ArchiveMut, CallerRow, SourceMeta, WalRow};
use dendro::rewrite::{copy_sources_into, CopySpec};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        Ok(Some(Segment {
            bytes: b"x".to_vec(),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

fn meta() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn entry(ts: i64) -> CallerRow {
    CallerRow {
        ts,
        blob: ts.to_le_bytes().to_vec(),
    }
}

fn held(path: &std::path::Path, name: &str) -> Vec<i64> {
    let db = Archive::open(path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    db.read_caller_rows(id, name, i64::MIN, i64::MAX)
        .unwrap()
        .iter()
        .map(|r| r.ts)
        .collect()
}

#[test]
fn a_caller_row_below_its_names_newest_is_dropped_and_counted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta()).unwrap();

    w.caller_rows("idx", vec![entry(10), entry(20)]).unwrap();
    w.caller_rows("idx", vec![entry(15)]).unwrap();
    w.caller_rows("idx", vec![entry(20)]).unwrap();
    // One call, out of order within it, both above the floor of 20.
    w.caller_rows("idx", vec![entry(30), entry(25)]).unwrap();
    // Another name has its own floor.
    w.caller_rows("other", vec![entry(5)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_caller_rows_out_of_order(), 1, "only the 15");
    assert_eq!(w.dropped_out_of_order(), 0, "WAL rows are counted apart");
    drop(w);
    archive.join().unwrap();

    assert_eq!(held(&path, "idx"), vec![10, 20, 20, 25, 30]);
    assert_eq!(held(&path, "other"), vec![5]);
}

/// The floor is read from the file, so a later writer applies it too.
#[test]
fn a_later_writer_reads_the_floor_from_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta()).unwrap();
    let id = w.source_id();
    w.caller_rows("idx", vec![entry(10), entry(20)]).unwrap();
    drop(w);
    archive.join().unwrap();

    let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
    let (mut w, _) = archive.resume_source(id, 1_000).unwrap();
    w.caller_rows("idx", vec![entry(15), entry(20)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_caller_rows_out_of_order(), 1);
    drop(w);
    archive.join().unwrap();
    assert_eq!(held(&path, "idx"), vec![10, 20, 20]);
}

/// Retention that removes a name's newest rows records them, so the next
/// row must be above them, in this writer and in a later one. Retention that
/// leaves the newest rows records nothing, and rows at that `ts` are still
/// taken.
#[test]
fn retention_of_a_names_newest_rows_keeps_its_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta()).unwrap();
    let id = w.source_id();
    w.caller_rows("kept", vec![entry(10), entry(20)]).unwrap();
    w.caller_rows("gone", vec![entry(10), entry(50)]).unwrap();
    w.sync().unwrap();

    // `kept` keeps its 20; `gone` loses both rows.
    w.evict_streams_before(15, Box::new(|s: &str| s == "kept"))
        .unwrap();
    w.evict_streams_before(100, Box::new(|s: &str| s == "gone"))
        .unwrap();
    w.caller_rows("kept", vec![entry(20)]).unwrap();
    w.caller_rows("gone", vec![entry(50)]).unwrap();
    w.caller_rows("gone", vec![entry(51)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_caller_rows_out_of_order(), 1, "only `gone`'s 50");
    drop(w);
    archive.join().unwrap();
    assert_eq!(held(&path, "kept"), vec![20, 20]);
    assert_eq!(held(&path, "gone"), vec![51]);

    // A later writer, after the whole source was evicted below 200 offline
    // with a caller floor that cuts `kept` at the cutoff.
    let mut db = ArchiveMut::open(&path).unwrap();
    db.evict_before_with_floor(id, 200, &|_: &str, _: Option<i64>| i64::MAX)
        .unwrap();
    drop(db);
    let mut archive = Writer::open(&path, Box::new(Tags)).unwrap();
    let (mut w, _) = archive.resume_source(id, 1_000).unwrap();
    w.caller_rows("kept", vec![entry(20)]).unwrap();
    w.caller_rows("gone", vec![entry(51)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_caller_rows_out_of_order(), 2);
    drop(w);
    archive.join().unwrap();
}

/// A copy carries each name's floor: a time-bounded copy that left a name's
/// newest rows out drops a row below them, as its original would.
#[test]
fn a_bounded_copy_keeps_each_names_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let copy = dir.path().join("copy.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta()).unwrap();
    w.caller_rows("idx", vec![entry(10), entry(100)]).unwrap();
    w.caller_rows("all", vec![entry(10)]).unwrap();
    drop(w);
    archive.join().unwrap();

    let src = Archive::open(&path).unwrap();
    let mut dst = ArchiveMut::create(&copy).unwrap();
    let spec = CopySpec {
        end: 50,
        ..CopySpec::everything()
    };
    dst.transaction(|tx| copy_sources_into(&src, tx, &spec, &Tags))
        .unwrap();
    drop(dst);

    let mut archive = Writer::open(&copy, Box::new(Tags)).unwrap();
    let id = Archive::open(&copy).unwrap().read_sources().unwrap()[0].id;
    let (mut w, _) = archive.resume_source(id, 1_000).unwrap();
    // The copy does not hold `idx`'s 100, so a row at 100 is not taken
    // either.
    w.caller_rows("idx", vec![entry(60), entry(100)]).unwrap();
    // The copy holds `all`'s newest row, so rows at its `ts` are still taken.
    w.caller_rows("all", vec![entry(10)]).unwrap();
    w.sync().unwrap();
    assert_eq!(
        w.dropped_caller_rows_out_of_order(),
        2,
        "`idx`'s 60 and 100"
    );
    drop(w);
    archive.join().unwrap();
    assert_eq!(held(&copy, "all"), vec![10, 10]);
}

/// The same holds for whole-source retention without a caller floor.
#[test]
fn whole_source_retention_keeps_each_names_floor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let mut archive = Writer::create(&path, Box::new(Tags)).unwrap();
    let mut w = archive.add_source(meta()).unwrap();
    w.caller_rows("idx", vec![entry(10), entry(50)]).unwrap();
    w.sync().unwrap();
    w.evict_before(100).unwrap();
    w.caller_rows("idx", vec![entry(50), entry(60)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_caller_rows_out_of_order(), 1, "the 50");
    drop(w);
    archive.join().unwrap();
    assert_eq!(held(&path, "idx"), vec![60]);
}
