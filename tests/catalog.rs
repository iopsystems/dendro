//! The lazy read: what the catalog answers without a BLOB, one segment for
//! a schema probe, a time-bounded read, and bytes fetched on demand.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use dendro::db::{Db, SegmentMeta, SourceMeta, WalRow};
use dendro::read::{self, SegmentBytes};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};

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
        }))
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

/// Stream `a`: two sealed segments (1..=2, 3..=4) and live rows 5 and 6.
/// Stream `b`: live rows only, 7. Stream `c`: nothing live, one segment.
fn fixture(path: &std::path::Path) -> i64 {
    let mut db = Db::create(path).unwrap();
    let id = db
        .insert_source(&SourceMeta {
            labels: BTreeMap::from([("source".to_string(), "x".to_string())]),
            metadata: BTreeMap::new(),
            clock_anchor_wall_ns: 0,
        })
        .unwrap();
    for (seq, (first, last)) in [(1, 2), (3, 4)].into_iter().enumerate() {
        db.insert_segment(
            id,
            "a",
            seq as u64,
            &SegmentMeta {
                rows: 2,
                first_ts: first,
                last_ts: last,
            },
            format!("{first},{last}").as_bytes(),
        )
        .unwrap();
    }
    db.insert_wal_rows(id, &[row("a", 5), row("a", 6), row("b", 7)])
        .unwrap();
    db.insert_segment(
        id,
        "c",
        0,
        &SegmentMeta {
            rows: 1,
            first_ts: 9,
            last_ts: 9,
        },
        b"9",
    )
    .unwrap();
    id
}

#[test]
fn the_catalog_describes_every_stream_without_a_blob() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let id = fixture(&path);
    let db = Db::open_read_only(&path).unwrap();
    let cat = read::catalog(&db).unwrap();
    assert_eq!(cat.len(), 1);
    let src = &cat[0];
    assert_eq!(src.id, id);
    assert!(src.uuid.is_some());
    assert_eq!(src.labels["source"], "x");
    assert_eq!(src.span(), Some((1, 9)));
    let by_name: BTreeMap<&str, _> = src.streams.iter().map(|s| (s.name.as_str(), s)).collect();
    let a = by_name["a"];
    assert_eq!((a.segments, a.sealed.rows, a.live.rows), (2, 4, 2));
    assert_eq!(a.span(), Some((1, 6)));
    assert_eq!(a.rows(), 6);
    let b = by_name["b"];
    assert_eq!((b.segments, b.sealed.rows, b.live.rows), (0, 0, 1));
    assert_eq!(
        b.span(),
        Some((7, 7)),
        "a stream inside its first seal period is present"
    );
    let c = by_name["c"];
    assert_eq!(c.span(), Some((9, 9)));
}

#[test]
fn a_probe_is_the_first_sealed_segment_or_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let id = fixture(&path);
    let db = Db::open_read_only(&path).unwrap();
    assert_eq!(read::probe(&db, id, "a", &Tags).unwrap().unwrap(), b"1,2");
    assert_eq!(read::probe(&db, id, "b", &Tags).unwrap().unwrap(), b"7");
    assert_eq!(read::probe(&db, id, "c", &Tags).unwrap().unwrap(), b"9");
    assert!(read::probe(&db, id, "nope", &Tags).unwrap().is_none());
}

#[test]
fn a_ranged_read_takes_whole_segments_and_trims_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let id = fixture(&path);
    let db = Db::open_read_only(&path).unwrap();
    let got = read::stream_range(&db, id, "a", 3, 5, &Tags).unwrap();
    assert_eq!(got, vec![b"3,4".to_vec(), b"5".to_vec()]);
    // A range past everything sealed reads only the tail, trimmed.
    let got = read::stream_range(&db, id, "a", 6, 100, &Tags).unwrap();
    assert_eq!(got, vec![b"6".to_vec()]);
    // A range before everything: nothing.
    assert!(read::stream_range(&db, id, "a", -10, 0, &Tags)
        .unwrap()
        .is_empty());
}

#[test]
fn segment_bytes_resolve_lazily_by_path_and_by_shared_connection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.dendro");
    let id = fixture(&path);
    let full = vec![b"1,2".to_vec(), b"3,4".to_vec(), b"5,6".to_vec()];
    let by_path = SegmentBytes::at_path(path.clone(), id, "a".to_string());
    assert_eq!(by_path.all(&Tags).unwrap(), full);
    let shared = Arc::new(Mutex::new(
        Db::open_bytes(std::fs::read(&path).unwrap()).unwrap(),
    ));
    let by_conn = SegmentBytes::shared(shared, id, "a".to_string());
    assert_eq!(by_conn.all(&Tags).unwrap(), full);
}
