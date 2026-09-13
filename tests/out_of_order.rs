// Drives the writer and reads the counter it keeps.
#![cfg(all(feature = "write", feature = "test-support"))]

//! An append at or below its stream's newest sealed row used to be committed,
//! charged for, and invisible — `read_wal` showed it and no read path did.
//! It is now dropped and said out loud.
//!
//! The watermark is per `(source, stream)`, so this is a constraint WITHIN a
//! stream and nowhere else: a different stream, or a different source, has
//! its own watermark and takes the same timestamp happily. That is the
//! property these tests pin, because it is what makes the restriction
//! acceptable rather than merely documented.

use std::collections::BTreeMap;

use dendro::db::{Db, SourceMeta, WalRow};
use dendro::read;
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Archive;

struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
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
    let db = Db::open_read_only(path).unwrap();
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
    let (archive, mut w) = Archive::single(&path, Box::new(Tags), meta("a")).unwrap();

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
    let db = Db::open_read_only(&path).unwrap();
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
    let (archive, mut w) = Archive::single(&path, Box::new(Tags), meta("a")).unwrap();
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
    let mut archive = Archive::create(&path, Box::new(Tags)).unwrap();
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
/// `Db::sealed_watermarks`); this records that the floor is what a caller
/// actually meets, so nobody reads the seeding as the thing doing the work.
#[test]
fn a_resumed_source_is_refused_at_the_handle_which_is_stronger() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Archive::single(&path, Box::new(Tags), meta("a")).unwrap();
    let id = w.source_id();
    w.wal(vec![row("s", 100)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    archive.finalize_single(w, (100, 0)).unwrap();

    let mut archive = Archive::open(&path, Box::new(Tags)).unwrap();
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
    let mut archive = Archive::create(&path, Box::new(Tags)).unwrap();
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

    let marks = Db::open_read_only(&path)
        .unwrap()
        .sealed_watermarks()
        .unwrap();
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

/// A deferred seal has not raised anything yet: until the segment commits,
/// an append it would eventually shadow is still legal and still readable.
#[test]
fn the_watermark_moves_with_the_commit_not_the_request() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("o.dendro");
    let (archive, mut w) = Archive::single(&path, Box::new(Tags), meta("a")).unwrap();

    // Never sealed, so there is no watermark at all: out-of-order rows
    // within one unsealed span are the encoder's to order, and are kept.
    w.wal(vec![row("s", 300)]).unwrap();
    w.wal(vec![row("s", 100)]).unwrap();
    w.sync().unwrap();
    assert_eq!(w.dropped_out_of_order(), 0, "no segment, no watermark");
    archive.finalize_single(w, (300, 0)).unwrap();

    let db = Db::open_read_only(&path).unwrap();
    let id = db.read_sources().unwrap()[0].id;
    assert_eq!(db.live_wal(id, "s").unwrap().len(), 2, "both are readable");
}
