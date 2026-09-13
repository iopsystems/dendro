// Drives the writer and its test-only hooks, so it needs both features.
#![cfg(all(feature = "write", feature = "test-support"))]

//! The writer's recovery policy: what is retried, what is isolated to one
//! source, and what still stops the archive.
//!
//! Each of these failed on the writer as it was before the policy existed —
//! a single `?` per message — which is why they are here rather than argued.

use std::collections::BTreeMap;
use std::time::Duration;

use dendro::db::{Db, SourceMeta, WalRow};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Archive;
use dendro::Error;

/// Reports the timestamps it was handed.
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

fn source(name: &str) -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), name.to_string())]),
        metadata: BTreeMap::new(),
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

fn wal_ts(db: &Db, id: i64) -> Vec<i64> {
    db.read_wal(id, "s")
        .unwrap()
        .into_iter()
        .map(|r| r.ts)
        .collect()
}

/// A source repeating a `(stream, ts)` it already committed is that source's
/// bad tick and nobody else's. The batched commit used to turn it into a dead
/// writer for every source in the archive; now the colliding rows are dropped,
/// the other source's land, and the writer keeps going.
#[test]
fn a_sources_colliding_tick_is_dropped_without_taking_the_archive_down() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("two.dendro");
    let mut archive = Archive::create(&path, Box::new(Tags)).unwrap();
    let mut a = archive.add_source(source("a")).unwrap();
    let b = archive.add_source(source("b")).unwrap();
    let (ia, ib) = (a.source_id(), b.source_id());

    archive
        .wal_tick(vec![(ia, vec![row(1_000)]), (ib, vec![row(1_000)])])
        .unwrap();
    // Tick 2: A repeats ts 1000 (collides with its own row), B is fine.
    archive
        .wal_tick(vec![(ia, vec![row(1_000)]), (ib, vec![row(2_000)])])
        .unwrap();
    // Tick 3: a normal tick for both. The writer must still be alive.
    archive
        .wal_tick(vec![(ia, vec![row(3_000)]), (ib, vec![row(3_000)])])
        .unwrap();
    a.sync().unwrap();

    let db = Db::open_read_only(&path).unwrap();
    assert_eq!(
        wal_ts(&db, ia),
        vec![1_000, 3_000],
        "A lost only its colliding tick"
    );
    assert_eq!(wal_ts(&db, ib), vec![1_000, 2_000, 3_000], "B lost nothing");
    drop(b);
    drop(a);
    archive.join().unwrap();
}

/// Another connection holding the write lock is a condition that clears, so
/// the writer retries it rather than dying on it — for a tick and for a seal.
/// The writer's `busy_timeout` is shortened so the wait is observable in
/// milliseconds; the retry schedule is what carries it past the lock. A seal
/// retried this way reuses its sequence number: the stream ends with `seq 0`,
/// not a hole per attempt.
#[test]
fn a_busy_database_is_retried_rather_than_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy.dendro");
    let mut archive = Archive::create_with_busy_timeout(
        &path,
        Box::new(Tags),
        Duration::from_secs(10),
        Duration::from_millis(20),
    )
    .unwrap();
    let mut w = archive.add_source(source("a")).unwrap();
    let id = w.source_id();

    // Hold the write lock from a second connection for longer than the busy
    // timeout plus the first two retries, and release it before the schedule
    // runs out.
    let hold = |ms: u64| {
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let lock_path = path.clone();
        let holder = std::thread::spawn(move || {
            let conn = rusqlite::Connection::open(&lock_path).unwrap();
            conn.execute_batch("BEGIN IMMEDIATE").unwrap();
            locked_tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(ms));
            conn.execute_batch("COMMIT").unwrap();
        });
        locked_rx.recv().unwrap();
        holder
    };

    let holder = hold(150);
    w.wal(vec![row(1_000)]).unwrap();
    // Blocks until the tick is committed — i.e. until a retry got past the
    // lock. A writer that died would make this an `Err`.
    w.sync().unwrap();
    holder.join().unwrap();

    let holder = hold(150);
    w.seal(vec!["s".to_string()]).unwrap();
    w.sync().unwrap();
    holder.join().unwrap();

    let db = Db::open_read_only(&path).unwrap();
    let seqs: Vec<u64> = db
        .read_segment_meta(id, "s")
        .unwrap()
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(seqs, vec![0], "one segment, numbered from zero, no hole");
    assert!(db.live_wal(id, "s").unwrap().is_empty());
    drop(w);
    archive.join().unwrap();
}

/// An encoder that panics on the writer thread used to leave every handle
/// reporting `WriterGone`, as if the archive had been joined. It is the
/// encoder's failure and is reported as one, with the panic's message.
#[test]
fn an_encoder_panic_is_the_encoders_error_not_writer_gone() {
    struct Panics;
    impl SegmentEncoder for Panics {
        fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
            if rows.is_empty() {
                return Ok(None);
            }
            panic!("this encoder cannot count to {}", rows.len());
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("panic.dendro");
    let mut archive = Archive::create(&path, Box::new(Panics)).unwrap();
    let mut w = archive.add_source(source("a")).unwrap();
    w.wal(vec![row(1_000)]).unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    let err = w.sync().unwrap_err();
    match err.root() {
        Error::Encoder { stream, source } => {
            assert_eq!(stream, "s");
            assert!(source.to_string().contains("cannot count to 1"), "{source}");
        }
        other => panic!("expected the encoder's error, got {other:?}"),
    }
    drop(w);
    // The row it could not seal is still there for a reader with a working
    // encoder: nothing was lost, only the seal.
    let _ = archive.join();
    let db = Db::open_read_only(&path).unwrap();
    assert_eq!(wal_ts(&db, 1), vec![1_000]);
}
