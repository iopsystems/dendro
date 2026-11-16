//! Per-stream retention bounds the clock-offset series the way whole-source
//! retention does: at the oldest row the source still holds anywhere.

// The archive, which the wire-only build (`--no-default-features`) leaves out.
#![cfg(feature = "archive")]

use std::collections::BTreeMap;

use dendro::archive::{Archive, ArchiveMut, SegmentMeta, SourceMeta, WalRow};

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn segment(db: &mut ArchiveMut, id: i64, stream: &str, seq: u64, first_ts: i64, last_ts: i64) {
    db.insert_segment(
        id,
        stream,
        seq,
        &SegmentMeta {
            rows: 1,
            first_ts,
            last_ts,
        },
        b"x",
    )
    .unwrap();
}

fn offsets(db: &Archive, id: i64) -> Vec<i64> {
    db.read_clock_offsets(id)
        .unwrap()
        .into_iter()
        .map(|(ts, _)| ts)
        .collect()
}

#[test]
fn per_stream_eviction_cuts_clock_offsets_at_the_oldest_surviving_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    // Stream `a` is expensive and kept briefly; `b` is kept longer.
    segment(&mut db, id, "a", 0, 100, 100);
    segment(&mut db, id, "a", 1, 200, 200);
    segment(&mut db, id, "b", 0, 150, 150);
    db.insert_wal_rows(
        id,
        &[WalRow {
            stream: "b".to_string(),
            ts: 250,
            wall_offset: 0,
            row: vec![1],
        }],
    )
    .unwrap();
    db.transaction(|tx| {
        for ts in [100, 150, 200, 250] {
            tx.insert_clock_offset(id, ts, 0)?;
        }
        Ok(())
    })
    .unwrap();

    // Evict `a` before 250: both of its segments go. `b` still holds a row
    // at 150, so the series must keep 150 and everything after it — and drop
    // 100, which no surviving row needs.
    let evicted = db.evict_streams_before(id, 250, &|s| s == "a").unwrap();
    assert_eq!(evicted.segments, 2);
    assert_eq!(offsets(&db, id), vec![150, 200, 250]);

    // Evict `b` before 300 as well: its segment goes and its live row at
    // 250 is kept, so the series keeps 250.
    let evicted = db.evict_streams_before(id, 300, &|s| s == "b").unwrap();
    assert_eq!((evicted.segments, evicted.kept_live_rows), (1, 1));
    assert_eq!(offsets(&db, id), vec![250]);

    // Discard that row and evict again: nothing is left, so the cutoff
    // applies.
    db.discard_live_rows(id, &|s| s == "b").unwrap();
    db.evict_streams_before(id, 300, &|s| s == "b").unwrap();
    assert!(db.all_streams(id).unwrap().is_empty());
    assert!(offsets(&db, id).is_empty());
}

/// Retention never deletes a row no segment holds: it keeps the live rows
/// before the cutoff, counts them, and they stay live once the segment
/// below them is evicted.
#[test]
fn eviction_keeps_and_reports_the_unsealed_rows_before_the_cutoff() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.dendro");
    let mut db = ArchiveMut::create(&path).unwrap();
    let id = db.insert_source(&source()).unwrap();
    // `a`: sealed through 100, with a covered row at 100 still in the WAL,
    // and live rows at 150 and 250.
    segment(&mut db, id, "a", 0, 50, 100);
    db.insert_wal_rows(
        id,
        &[
            WalRow {
                stream: "a".to_string(),
                ts: 100,
                wall_offset: 0,
                row: vec![1],
            },
            WalRow {
                stream: "a".to_string(),
                ts: 150,
                wall_offset: 0,
                row: vec![1],
            },
            WalRow {
                stream: "a".to_string(),
                ts: 250,
                wall_offset: 0,
                row: vec![1],
            },
        ],
    )
    .unwrap();
    // `b`: never sealed, live rows at 10 and 300.
    db.insert_wal_rows(
        id,
        &[
            WalRow {
                stream: "b".to_string(),
                ts: 10,
                wall_offset: 0,
                row: vec![1],
            },
            WalRow {
                stream: "b".to_string(),
                ts: 300,
                wall_offset: 0,
                row: vec![1],
            },
        ],
    )
    .unwrap();

    let evicted = db.evict_before(id, 200).unwrap();
    // Deleted: the segment and the covered WAL row 100. Kept: 150 on `a` and
    // 10 on `b`, which no segment holds.
    assert_eq!(evicted.segments, 1);
    assert_eq!(evicted.wal_rows, 1);
    assert_eq!(evicted.kept_live_rows, 2);
    let live = |db: &ArchiveMut, id: i64, stream: &str| -> Vec<i64> {
        db.live_wal(id, stream)
            .unwrap()
            .into_iter()
            .map(|r| r.ts)
            .collect()
    };
    assert_eq!(live(&db, id, "a"), vec![150, 250]);
    assert_eq!(live(&db, id, "b"), vec![10, 300]);

    // Per stream, the same accounting, per stream.
    let mut db2 = ArchiveMut::create(&dir.path().join("r2.dendro")).unwrap();
    let id2 = db2.insert_source(&source()).unwrap();
    db2.insert_wal_rows(
        id2,
        &[
            WalRow {
                stream: "b".to_string(),
                ts: 10,
                wall_offset: 0,
                row: vec![1],
            },
            WalRow {
                stream: "c".to_string(),
                ts: 10,
                wall_offset: 0,
                row: vec![1],
            },
        ],
    )
    .unwrap();
    let evicted = db2.evict_streams_before(id2, 200, &|s| s == "b").unwrap();
    assert_eq!((evicted.wal_rows, evicted.kept_live_rows), (0, 1));
    assert_eq!(live(&db2, id2, "b"), vec![10]);

    // `discard_live_rows` is how live rows are deleted.
    assert_eq!(db2.discard_live_rows(id2, &|s| s == "b").unwrap(), 1);
    assert_eq!(live(&db2, id2, "b"), Vec::<i64>::new());
    assert_eq!(live(&db2, id2, "c"), vec![10]);
}
