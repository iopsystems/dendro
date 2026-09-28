// Drives the writer, so it needs the `write` feature.
#![cfg(feature = "write")]

//! `vacuum_into` on the read handle of an archive a writer is still
//! appending to: the one handle a second party can hold on a live archive.

use std::collections::BTreeMap;

use dendro::archive::{Archive, SourceMeta, WalRow};
use dendro::segment::{EncodeResult, SegmentEncoder};
use dendro::writer::Writer;

struct Never;
impl SegmentEncoder for Never {
    fn encode(&self, _stream: &str, _rows: &[WalRow]) -> EncodeResult {
        Ok(None)
    }
}

fn row(ts: i64) -> WalRow {
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: 0,
        row: vec![1],
    }
}

#[test]
fn a_live_archive_dumps_through_its_read_handle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("live.dendro");
    let mut archive = Writer::create(&path, Box::new(Never)).unwrap();
    let mut w = archive
        .add_source(SourceMeta {
            labels: BTreeMap::new(),
            metadata: BTreeMap::new(),
            clock_anchor_wall_ns: 0,
        })
        .unwrap();
    let id = w.source_id();
    w.wal(vec![row(1_000), row(2_000)]).unwrap();
    w.sync().unwrap();

    let reader = Archive::open(&path).unwrap();
    let copy = dir.path().join("copy.dendro");
    reader.vacuum_into(&copy).unwrap();

    // The copy holds the rows committed so far, WAL frames included.
    let copied = Archive::open(&copy).unwrap();
    assert_eq!(copied.live_wal(id, "s").unwrap().len(), 2);

    // And the writer carries on.
    w.wal(vec![row(3_000)]).unwrap();
    w.sync().unwrap();
    assert_eq!(
        Archive::open(&path)
            .unwrap()
            .live_wal(id, "s")
            .unwrap()
            .len(),
        3
    );
}
