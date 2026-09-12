// Drives the writer, so it needs the `write` feature.
#![cfg(feature = "write")]

//! Metadata written DURING a recording, ordered with the ticks and durable
//! without a finalize — what a producer needs to record an epoch change or a
//! discontinuity at the tick it happened, on an archive a kill may end.

use std::collections::BTreeMap;

use dendro::db::{Db, SourceMeta, WalRow};
use dendro::keys;
use dendro::segment::{EncodeResult, SegmentEncoder};
use dendro::writer::Archive;

struct Never;
impl SegmentEncoder for Never {
    fn encode(&self, _stream: &str, _rows: &[WalRow]) -> EncodeResult {
        Ok(None)
    }
}

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::new(),
        metadata: BTreeMap::from([("kept".to_string(), "yes".to_string())]),
        clock_anchor_wall_ns: 0,
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

fn patch(k: &str, v: &str) -> BTreeMap<String, String> {
    BTreeMap::from([(k.to_string(), v.to_string())])
}

#[test]
fn a_patch_lands_in_order_with_the_ticks_and_without_a_finalize() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.dendro");
    let mut archive = Archive::create(&path, Box::new(Never)).unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();

    w.wal(vec![row(1_000)]).unwrap();
    w.update_metadata(patch(keys::PRODUCER_EPOCH, "epoch-a"))
        .unwrap();
    w.wal(vec![row(2_000)]).unwrap();
    // A second key in a second patch: merged, not replaced.
    w.update_metadata(patch(keys::PRODUCER_EPOCHS, "[]"))
        .unwrap();
    // Committed, not finalized — what a kill would leave behind.
    w.sync().unwrap();

    // Seen from another connection while the writer still holds the file.
    let seen = Db::open_read_only(&path)
        .unwrap()
        .source_metadata(id)
        .unwrap();
    assert_eq!(
        seen.get(keys::PRODUCER_EPOCH).map(String::as_str),
        Some("epoch-a")
    );
    assert_eq!(
        seen.get(keys::PRODUCER_EPOCHS).map(String::as_str),
        Some("[]")
    );
    assert_eq!(
        seen.get("kept").map(String::as_str),
        Some("yes"),
        "seed keys survive a patch"
    );

    // A later patch to the same key replaces only that key.
    w.update_metadata(patch(keys::PRODUCER_EPOCH, "epoch-b"))
        .unwrap();
    archive.finalize_single(w, (2_000, 0)).unwrap();
    let after = Db::open_read_only(&path)
        .unwrap()
        .source_metadata(id)
        .unwrap();
    assert_eq!(
        after.get(keys::PRODUCER_EPOCH).map(String::as_str),
        Some("epoch-b")
    );
    assert_eq!(
        after.get(keys::PRODUCER_EPOCHS).map(String::as_str),
        Some("[]")
    );
    assert_eq!(after.len(), 3);
}

/// Metadata is not the recording: a patch that cannot be applied is logged
/// and skipped, and the ticks around it are unaffected.
#[test]
fn a_patch_that_cannot_land_does_not_stop_the_writer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("m.dendro");
    let mut archive = Archive::create(&path, Box::new(Never)).unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    // Delete the source's row out from under the writer, so its patch has no
    // row to land on. (A second writing connection, deliberately: the point
    // is the writer's reaction, not the ordering.)
    w.sync().unwrap();
    {
        let other = Db::open(&path).unwrap();
        other.update_source_metadata(id, &BTreeMap::new()).unwrap();
        rusqlite::Connection::open(&path)
            .unwrap()
            .execute("DELETE FROM sources WHERE id = ?1", [id])
            .unwrap();
    }
    w.update_metadata(patch("k", "v")).unwrap();
    // Still alive: the next hand-off is answered, not refused.
    w.sync().unwrap();
    drop(w);
    archive.join().unwrap();
}
