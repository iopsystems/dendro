// Drives the writer and its test-only constructor, so it needs both features.
#![cfg(all(feature = "write", feature = "test-support"))]

//! Retention after a deferred seal: the writer seals the deferred streams
//! before it evicts, and leaves out any it still cannot seal, so a pass never
//! deletes live rows whose seal is pending. Those rows can include the one
//! the rest need to decode (a schema stated at a segment's start).

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;
use std::thread::JoinHandle;
use std::time::Duration;

use dendro::archive::{Archive, Evicted, SourceMeta, WalRow};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::{SourceWriter, Writer};

fn source() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 0,
    }
}

/// A row of stream `s`; `anchor` marks the row the rows after it need.
fn row(ts: i64, anchor: bool) -> WalRow {
    WalRow {
        stream: "s".to_string(),
        ts,
        wall_offset: ts * 10,
        row: vec![if anchor { b'A' } else { b'R' }],
    }
}

/// Decodes a row only after an anchor row and drops the rows before the
/// first anchor, as an encoder whose schema is stated once per segment does.
/// Sends the running count of `encode` calls on `calls`, when set.
struct Anchored {
    count: AtomicUsize,
    calls: Option<Mutex<Sender<usize>>>,
}

impl Anchored {
    fn new(calls: Option<Sender<usize>>) -> Self {
        Self {
            count: AtomicUsize::new(0),
            calls: calls.map(Mutex::new),
        }
    }
}

impl SegmentEncoder for Anchored {
    fn encode(&self, _stream: &str, rows: &[WalRow]) -> EncodeResult {
        let n = self.count.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(calls) = &self.calls {
            let _ = calls.lock().unwrap().send(n);
        }
        let mut anchored = false;
        let mut decoded = Vec::new();
        for r in rows {
            anchored |= r.row == b"A";
            if anchored {
                decoded.push(r.ts);
            }
        }
        let (Some(first), Some(last)) = (decoded.first(), decoded.last()) else {
            return Ok(None);
        };
        let ts: Vec<String> = decoded.iter().map(|t| t.to_string()).collect();
        Ok(Some(Segment {
            bytes: ts.join(",").into_bytes(),
            rows: decoded.len() as u64,
            first_ts: *first,
            last_ts: *last,
            index: None,
        }))
    }
}

/// Take the write lock from a second connection, return once it is held,
/// and release it when `release` yields.
fn hold_lock(path: &Path, release: impl FnOnce() + Send + 'static) -> JoinHandle<()> {
    let path: PathBuf = path.to_path_buf();
    let (locked_tx, locked_rx) = channel();
    let holder = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked_tx.send(()).unwrap();
        release();
        conn.execute_batch("COMMIT").unwrap();
    });
    locked_rx.recv().unwrap();
    holder
}

/// Each segment of `s` as `(first_ts, last_ts)`, and its bytes.
fn segments(path: &Path, id: i64) -> (Vec<(i64, i64)>, Vec<String>) {
    let segs = Archive::open(path).unwrap().read_segments(id, "s").unwrap();
    (
        segs.iter()
            .map(|s| (s.meta.first_ts, s.meta.last_ts))
            .collect(),
        segs.iter()
            .map(|s| String::from_utf8(s.bytes.clone()).unwrap())
            .collect(),
    )
}

fn wal_rows(path: &Path) -> i64 {
    rusqlite::Connection::open(path)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM wal WHERE stream = 's'", [], |r| {
            r.get(0)
        })
        .unwrap()
}

/// A seal deferred by another connection's lock, then a retention pass
/// after the lock clears: the pass seals the stream first, so the anchor
/// at 1000 is in a segment and the rows after the cutoff decode. Before,
/// the pass deleted the live rows at 1000 and 2000 and the next seal of
/// 3000 decoded nothing.
#[test]
fn eviction_seals_a_deferred_stream_first() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deferred.dendro");
    let mut archive = Writer::create_with_busy_timeout(
        &path,
        Box::new(Anchored::new(None)),
        Duration::from_secs(10),
        Duration::from_millis(20),
    )
    .unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    for ts in 1..=3 {
        w.wal(vec![row(ts * 1_000, ts == 1)]).unwrap();
    }
    w.sync().unwrap();

    let holder = hold_lock(&path, || std::thread::sleep(Duration::from_millis(1_500)));
    w.seal(vec!["s".to_string()]).unwrap();
    w.sync().unwrap();
    holder.join().unwrap();
    assert_eq!(segments(&path, id).0, [], "the seal was deferred");

    let evicted = w.evict_before(2_500).unwrap();
    assert_eq!(evicted.live_rows, 0, "{evicted:?}");
    assert_eq!(evicted.held_streams, 0, "{evicted:?}");
    assert_eq!(
        segments(&path, id),
        (vec![(1_000, 3_000)], vec!["1000,2000,3000".to_string()])
    );

    drop(w);
    archive.join().unwrap();
}

/// The retry before the pass also fails, and the lock clears only after
/// it: the pass runs and leaves the stream out, so all three rows stay in
/// the WAL and seal whole with the next seal.
///
/// The lock is released between the retry's last attempt and the pass. The
/// encoder runs once per attempt, before the attempt's transaction, so the
/// eighth call (four attempts for the seal, four for the retry) starts the
/// last attempt; it waits out the 400 ms busy timeout and fails, and the
/// pass that follows waits up to another 400 ms. Releasing 600 ms after the
/// eighth call lands in the middle of the pass's wait.
#[test]
fn eviction_leaves_out_a_stream_whose_seal_still_cannot_commit() {
    held(|w, cutoff| w.evict_before(cutoff).unwrap(), 1);
}

/// The same with a caller's filter that names the stream: the hold-back
/// applies on top of it.
#[test]
fn a_filtered_eviction_leaves_out_a_held_stream() {
    held(
        |w, cutoff| {
            w.evict_streams_before(cutoff, Box::new(|s: &str| s == "s"))
                .unwrap()
        },
        1,
    );
}

/// A caller's filter that does not name the stream: nothing was held back
/// that the caller asked to evict, so `held_streams` is 0.
#[test]
fn a_held_stream_the_filter_skips_is_not_counted() {
    held(
        |w, cutoff| {
            w.evict_streams_before(cutoff, Box::new(|s: &str| s != "s"))
                .unwrap()
        },
        0,
    );
}

/// A seal deferred, its retry at the start of `evict` failing too, and the
/// pass that follows; `held_streams` is expected to be `count`.
fn held(evict: fn(&mut SourceWriter, i64) -> Evicted, count: usize) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("held.dendro");
    let (calls_tx, calls_rx): (Sender<usize>, Receiver<usize>) = channel();
    let mut archive = Writer::create_with_busy_timeout(
        &path,
        Box::new(Anchored::new(Some(calls_tx))),
        Duration::from_secs(10),
        Duration::from_millis(400),
    )
    .unwrap();
    let mut w = archive.add_source(source()).unwrap();
    let id = w.source_id();
    for ts in 1..=3 {
        w.wal(vec![row(ts * 1_000, ts == 1)]).unwrap();
    }
    w.sync().unwrap();

    let holder = hold_lock(&path, move || {
        while calls_rx.recv().unwrap() < 8 {}
        std::thread::sleep(Duration::from_millis(600));
    });
    w.seal(vec!["s".to_string()]).unwrap();
    let evicted = evict(&mut w, 2_500);
    holder.join().unwrap();
    assert_eq!(evicted.held_streams, count, "{evicted:?}");
    assert_eq!(evicted.wal_rows, 0, "{evicted:?}");
    assert_eq!(evicted.live_rows, 0, "{evicted:?}");
    assert_eq!(wal_rows(&path), 3, "the held stream kept its rows");

    w.seal(vec!["s".to_string()]).unwrap();
    let evicted = w.evict_before(2_500).unwrap();
    assert_eq!(evicted.held_streams, 0, "the seal committed: {evicted:?}");
    drop(w);
    archive.join().unwrap();
    assert_eq!(
        segments(&path, id),
        (vec![(1_000, 3_000)], vec!["1000,2000,3000".to_string()])
    );
}
