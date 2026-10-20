// Drives the writer and compaction.
#![cfg(all(feature = "write", feature = "test-support"))]

//! Where a segment's caller index lives (FORMAT.md §3.2): the
//! `segment_indexes` table on schema 5, the `segments.caller_index` column
//! on schema 4. Every path that writes, copies or deletes a segment keeps
//! the two in step.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dendro::archive::{Archive, ArchiveMut, Depth, Problem, SegmentMeta, SourceMeta, WalRow};
use dendro::rewrite::{compact, copy_sources_into, CompactSpec, CopySpec};
use dendro::segment::{encode_batch, EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

/// Real parquet (compaction opens it), and an index naming the rows'
/// first and last timestamps, so each segment's index is its own.
struct Indexed;

impl SegmentEncoder for Indexed {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return Ok(None);
        };
        let ts: Vec<i64> = rows.iter().map(|r| r.ts).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "timestamp",
            DataType::Int64,
            false,
        )]));
        let array: ArrayRef = Arc::new(Int64Array::from(ts));
        let batch = RecordBatch::try_new(schema.clone(), vec![array]).map_err(|e| e.to_string())?;
        Ok(Some(Segment {
            bytes: encode_batch(schema, &batch)?,
            rows: rows.len() as u64,
            first_ts: first.ts,
            last_ts: last.ts,
            index: Some(format!("{}-{}", first.ts, last.ts).into_bytes()),
        }))
    }
}

fn meta() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
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

/// Streams `a` and `b`, each with segments of two rows over ts 1..=8.
fn archive(path: &Path) -> i64 {
    let (archive, mut w) = Writer::single(path, Box::new(Indexed), meta()).unwrap();
    let id = w.source_id();
    for ts in 1..=8 {
        w.wal(vec![row("a", ts), row("b", ts)]).unwrap();
        if ts % 2 == 0 {
            w.seal(vec!["a".to_string(), "b".to_string()]).unwrap();
        }
    }
    archive.finalize_single(w, (8, 0)).unwrap();
    id
}

fn raw(path: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(path).unwrap()
}

fn count(path: &Path, sql: &str) -> i64 {
    raw(path).query_row(sql, [], |r| r.get(0)).unwrap()
}

/// Every segment with an index has exactly one row in `segment_indexes`,
/// and no row there lacks a segment.
fn in_step(path: &Path) {
    assert_eq!(
        count(
            path,
            "SELECT COUNT(*) FROM segment_indexes i LEFT JOIN segments s \
             ON s.source_id = i.source_id AND s.stream = i.stream AND s.seq = i.seq \
             WHERE s.seq IS NULL"
        ),
        0,
        "an index without its segment"
    );
    let report = Archive::open(path).unwrap().verify(Depth::Full).unwrap();
    assert!(report.is_sound(), "{:?}", report.problems);
}

/// A v4 archive: this build's schema without the two v5 tables, with the
/// index as the last column of `segments`, stamped 4.
fn v4_archive(path: &Path, with_column: bool) -> i64 {
    let mut db = ArchiveMut::create(path).unwrap();
    let id = db.insert_source(&meta()).unwrap();
    drop(db);
    let column = if with_column {
        "ALTER TABLE segments ADD COLUMN caller_index BLOB;"
    } else {
        ""
    };
    raw(path)
        .execute_batch(&format!(
            "DROP TABLE documents; DROP TABLE segment_indexes; {column} \
             PRAGMA user_version = 4;"
        ))
        .unwrap();
    id
}

#[test]
fn a_v5_index_lives_in_its_own_table_not_the_segment_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("i.dendro");
    let id = archive(&path);

    // The segment row has no index column at all.
    let columns: Vec<String> = raw(&path)
        .prepare("SELECT name FROM pragma_table_info('segments')")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert!(
        !columns.contains(&"caller_index".to_string()),
        "{columns:?}"
    );
    assert_eq!(count(&path, "SELECT COUNT(*) FROM segments"), 8);
    assert_eq!(count(&path, "SELECT COUNT(*) FROM segment_indexes"), 8);

    let db = Archive::open(&path).unwrap();
    let indexes = db.read_segment_indexes(id, "a").unwrap();
    let want: Vec<(u64, Option<Vec<u8>>)> = (0..4)
        .map(|k| (k, Some(format!("{}-{}", 2 * k + 1, 2 * k + 2).into_bytes())))
        .collect();
    assert_eq!(indexes, want);
    // The segment reads carry the same index.
    let segs = db.read_segments(id, "a").unwrap();
    assert_eq!(
        segs.iter()
            .map(|s| (s.seq, s.caller_index.clone()))
            .collect::<Vec<_>>(),
        want
    );
    let some = db.segments_overlapping(id, "a", 3, 4).unwrap();
    assert_eq!(some.len(), 1);
    assert_eq!(some[0].caller_index, want[1].1);
    in_step(&path);
}

#[test]
fn a_segment_without_an_index_has_no_index_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("n.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&meta()).unwrap();
    let m = SegmentMeta {
        rows: 1,
        first_ts: 1,
        last_ts: 1,
    };
    db.insert_segment(id, "a", 0, &m, b"x").unwrap();
    db.insert_segment_with_index(id, "a", 1, &m, b"x", Some(b"i"))
        .unwrap();
    drop(db);
    assert_eq!(count(&path, "SELECT COUNT(*) FROM segment_indexes"), 1);
    assert_eq!(
        Archive::open(&path)
            .unwrap()
            .read_segment_indexes(id, "a")
            .unwrap(),
        vec![(0, None), (1, Some(b"i".to_vec()))]
    );
}

#[test]
fn every_path_that_deletes_a_segment_deletes_its_index() {
    let dir = tempfile::tempdir().unwrap();

    // Whole-source retention.
    let path = dir.path().join("evict.dendro");
    let id = archive(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    db.evict_before(id, 5).unwrap();
    drop(db);
    assert_eq!(count(&path, "SELECT COUNT(*) FROM segments"), 4);
    assert_eq!(count(&path, "SELECT COUNT(*) FROM segment_indexes"), 4);
    in_step(&path);

    // Per-stream retention.
    let path = dir.path().join("streams.dendro");
    let id = archive(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    db.evict_streams_before(id, 7, &|s| s == "a").unwrap();
    drop(db);
    assert_eq!(
        count(
            &path,
            "SELECT COUNT(*) FROM segment_indexes WHERE stream = 'a'"
        ),
        1
    );
    assert_eq!(
        count(
            &path,
            "SELECT COUNT(*) FROM segment_indexes WHERE stream = 'b'"
        ),
        4
    );
    in_step(&path);

    // Compaction: the merged segment has no index (an index over one input
    // cannot describe two), and the replaced segments' indexes go with them.
    let path = dir.path().join("compact.dendro");
    archive(&path);
    let mut db = ArchiveMut::open(&path).unwrap();
    let done = compact(&mut db, &CompactSpec::to_rows(100)).unwrap();
    assert_eq!((done.before, done.after), (8, 2));
    drop(db);
    assert_eq!(count(&path, "SELECT COUNT(*) FROM segment_indexes"), 0);
    in_step(&path);
}

#[test]
fn a_copy_carries_indexes_and_a_projection_drops_them() {
    let dir = tempfile::tempdir().unwrap();
    let src_path = dir.path().join("src.dendro");
    let id = archive(&src_path);
    let src = Archive::open(&src_path).unwrap();
    let want = src.read_segment_indexes(id, "a").unwrap();

    let dst_path = dir.path().join("dst.dendro");
    let mut dst = ArchiveMut::create(&dst_path).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Indexed))
        .unwrap();
    let copied = dst.read_sources().unwrap()[0].id;
    assert_eq!(dst.read_segment_indexes(copied, "a").unwrap(), want);
    drop(dst);
    assert_eq!(count(&dst_path, "SELECT COUNT(*) FROM segment_indexes"), 8);
    in_step(&dst_path);
}

#[test]
fn a_v4_archive_keeps_its_indexes_in_the_column_and_a_copy_moves_them() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v4.dendro");
    let id = v4_archive(&path, true);

    // A writer resuming the v4 archive writes its index where v4 keeps it.
    let mut archive = Writer::open(&path, Box::new(Indexed)).unwrap();
    let (mut w, _) = archive.resume_source(id, 0).unwrap();
    w.wal(vec![row("a", 1), row("a", 2)]).unwrap();
    w.seal(vec!["a".to_string()]).unwrap();
    w.finalize((2, 0)).unwrap();
    archive.join().unwrap();
    assert_eq!(
        count(
            &path,
            "SELECT COUNT(*) FROM segments WHERE caller_index = CAST('1-2' AS BLOB)"
        ),
        1
    );
    assert_eq!(
        count(
            &path,
            "SELECT COUNT(*) FROM sqlite_master WHERE name = 'segment_indexes'"
        ),
        0,
        "a v4 archive is given no v5 table"
    );
    let v4 = Archive::open(&path).unwrap();
    assert_eq!(v4.schema_version().unwrap(), 4);
    let want = vec![(0, Some(b"1-2".to_vec()))];
    assert_eq!(v4.read_segment_indexes(id, "a").unwrap(), want);
    assert_eq!(
        v4.read_segments(id, "a").unwrap()[0].caller_index,
        want[0].1
    );

    // Copied forward, the index moves to the table.
    let fwd = dir.path().join("v5.dendro");
    let mut dst = ArchiveMut::create(&fwd).unwrap();
    dst.transaction(|tx| copy_sources_into(&v4, tx, &CopySpec::everything(), &Indexed))
        .unwrap();
    let copied = dst.read_sources().unwrap()[0].id;
    assert_eq!(dst.read_segment_indexes(copied, "a").unwrap(), want);
    drop(dst);
    assert_eq!(count(&fwd, "SELECT COUNT(*) FROM segment_indexes"), 1);
    in_step(&fwd);
}

#[test]
fn a_v4_archive_from_before_the_column_reads_no_index_and_drops_one() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v4-old.dendro");
    let id = v4_archive(&path, false);
    let mut db = ArchiveMut::open(&path).unwrap();
    let m = SegmentMeta {
        rows: 1,
        first_ts: 1,
        last_ts: 1,
    };
    db.insert_segment_with_index(id, "a", 0, &m, b"x", Some(b"i"))
        .unwrap();
    assert_eq!(db.read_segment_indexes(id, "a").unwrap(), vec![(0, None)]);
    assert_eq!(db.read_segments(id, "a").unwrap()[0].caller_index, None);
}

#[test]
fn an_index_and_its_segment_land_together_or_not_at_all() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("atomic.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&meta()).unwrap();
    drop(db);
    // Make the index insert fail after the segment insert has succeeded.
    raw(&path)
        .execute_batch(
            "CREATE TRIGGER refuse_index BEFORE INSERT ON segment_indexes \
             BEGIN SELECT RAISE(ABORT, 'refused'); END;",
        )
        .unwrap();

    let m = SegmentMeta {
        rows: 1,
        first_ts: 1,
        last_ts: 1,
    };
    let mut db = ArchiveMut::open(&path).unwrap();
    // On its own commit, and inside a caller's transaction.
    assert!(db
        .insert_segment_with_index(id, "a", 0, &m, b"x", Some(b"i"))
        .is_err());
    assert!(db
        .transaction(|tx| tx.insert_segment_with_index(id, "a", 0, &m, b"x", Some(b"i")))
        .is_err());
    assert!(db.read_segment_meta(id, "a").unwrap().is_empty());
    // A segment with no index does not touch the table, so it lands.
    db.insert_segment(id, "a", 0, &m, b"x").unwrap();
    assert_eq!(db.read_segment_indexes(id, "a").unwrap(), vec![(0, None)]);
}

/// A segment deleted where the cascade did not run (a connection without
/// `foreign_keys`, such as the sqlite3 shell) leaves its index behind. The
/// next segment at that key must neither fail on it nor read it back.
#[test]
fn a_leftover_index_row_is_named_by_verify_and_replaced_by_the_next_segment() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stale.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&meta()).unwrap();
    let m = SegmentMeta {
        rows: 1,
        first_ts: 1,
        last_ts: 1,
    };
    db.insert_segment_with_index(id, "a", 0, &m, b"x", Some(b"old0"))
        .unwrap();
    db.insert_segment_with_index(id, "a", 1, &m, b"x", Some(b"old1"))
        .unwrap();
    drop(db);
    let conn = raw(&path);
    conn.execute_batch("PRAGMA foreign_keys = OFF; DELETE FROM segments;")
        .unwrap();
    drop(conn);
    assert_eq!(count(&path, "SELECT COUNT(*) FROM segment_indexes"), 2);

    let mut db = ArchiveMut::open(&path).unwrap();
    let report = db.verify(Depth::Quick).unwrap();
    let orphans: Vec<&String> = report
        .problems
        .iter()
        .filter_map(|p| match p {
            Problem::ForeignKey(f) => Some(f),
            _ => None,
        })
        .collect();
    assert_eq!(
        orphans,
        vec![
            &format!("segment_indexes (source_id {id}, stream \"a\", seq 0) -> segments"),
            &format!("segment_indexes (source_id {id}, stream \"a\", seq 1) -> segments"),
        ]
    );

    // Without an index: the leftover is not read back as this segment's.
    db.insert_segment(id, "a", 0, &m, b"x").unwrap();
    // With one: the insert does not collide with the leftover.
    db.transaction(|tx| tx.insert_segment_with_index(id, "a", 1, &m, b"x", Some(b"new1")))
        .unwrap();
    assert_eq!(
        db.read_segment_indexes(id, "a").unwrap(),
        vec![(0, None), (1, Some(b"new1".to_vec()))]
    );
    drop(db);
    in_step(&path);
}
