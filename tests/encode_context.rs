// Drives the writer, so it needs the `write` feature.
#![cfg(feature = "write")]

//! The encoder is told which source a stream belongs to: on a seal, on a
//! read that materializes a tail, and on a copy, where the rows are the
//! source archive's.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use dendro::archive::{Archive, ArchiveMut, SourceMeta, WalRow};
use dendro::read;
use dendro::rewrite::{copy_sources_into, CopySpec};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

/// `(source_id, source_uuid, stream)` for every call that had rows.
type Seen = Arc<Mutex<Vec<(i64, Option<String>, String)>>>;

/// Records its context, and returns a segment whose bytes are opaque (the
/// archive never opens them on these paths).
struct Recording(Seen);

impl SegmentEncoder for Recording {
    fn encode(&self, ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        let (Some(first), Some(last)) = (rows.first(), rows.last()) else {
            return Ok(None);
        };
        self.0.lock().unwrap().push((
            ctx.source_id,
            ctx.source_uuid.map(str::to_string),
            ctx.stream.to_string(),
        ));
        Ok(Some(Segment {
            bytes: vec![0],
            rows: rows.len() as u64,
            first_ts: first.ts,
            last_ts: last.ts,
            index: None,
        }))
    }
}

fn source(host: &str) -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("host".to_string(), host.to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

fn row(ts: i64) -> WalRow {
    WalRow {
        stream: "cpu".to_string(),
        ts,
        wall_offset: 0,
        row: vec![1],
    }
}

fn take(seen: &Seen) -> Vec<(i64, Option<String>, String)> {
    let mut v = std::mem::take(&mut *seen.lock().unwrap());
    v.sort();
    v
}

#[test]
fn every_encode_names_the_source_its_rows_are_in() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two.dendro");
    let seen: Seen = Arc::default();

    // Two sources with the same stream name in one writer: the case a
    // stream name alone cannot tell apart.
    let mut archive = Writer::create(&path, Box::new(Recording(seen.clone()))).unwrap();
    let mut a = archive.add_source(source("a")).unwrap();
    let mut b = archive.add_source(source("b")).unwrap();
    a.wal(vec![row(1)]).unwrap();
    b.wal(vec![row(1)]).unwrap();
    a.seal(vec!["cpu".to_string()]).unwrap();
    b.seal(vec!["cpu".to_string()]).unwrap();
    // A live row each, for the read and the copy to materialize.
    a.wal(vec![row(2)]).unwrap();
    b.wal(vec![row(2)]).unwrap();
    a.sync().unwrap();
    b.sync().unwrap();

    let reader = Archive::open(&path).unwrap();
    let sources = reader.read_sources().unwrap();
    let expect: Vec<(i64, Option<String>, String)> = {
        let mut v: Vec<_> = sources
            .iter()
            .map(|s| (s.id, s.uuid.clone(), "cpu".to_string()))
            .collect();
        v.sort();
        v
    };
    assert_eq!(expect.len(), 2);
    assert!(expect.iter().all(|e| e.1.is_some()));
    assert_ne!(expect[0].0, expect[1].0);
    assert_eq!(take(&seen), expect, "the seals");

    let tails = Recording(seen.clone());
    read::read_archive(&reader, &tails).unwrap();
    assert_eq!(take(&seen), expect, "the read");

    // The copy renumbers the sources, so the destination's ids differ from
    // the source archive's; the encoder is told the latter, whose rows they
    // are.
    let mut dst = ArchiveMut::create_in_memory().unwrap();
    // Two padding sources, so neither copied id is one the source used.
    dst.insert_source(&source("padding")).unwrap();
    dst.insert_source(&source("padding")).unwrap();
    dst.transaction(|tx| copy_sources_into(&reader, tx, &CopySpec::everything(), &tails))
        .unwrap();
    let copied_ids: Vec<i64> = dst.read_sources().unwrap()[2..]
        .iter()
        .map(|s| s.id)
        .collect();
    assert!(copied_ids
        .iter()
        .all(|id| !expect.iter().any(|e| e.0 == *id)));
    assert_eq!(take(&seen), expect, "the copy");

    drop(a);
    drop(b);
    archive.join().unwrap();
}
