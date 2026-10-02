// Drives the writer and reads back what it stored.
#![cfg(all(feature = "write", feature = "test-support"))]

//! A running writer bumps each open source's heartbeat on a timer, so a
//! reader can tell a source still being written from one whose writer
//! stopped, and from a copy.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use dendro::archive::{
    Archive, ArchiveMut, HeartbeatWatch, SourceMeta, SourceRow, WalRow, WriterState,
};
use dendro::rewrite::{copy_sources_into, CopySpec};
use dendro::segment::{EncodeResult, SegmentEncoder};
use dendro::writer::Writer;

const BEAT: Duration = Duration::from_millis(40);

struct Never;

impl SegmentEncoder for Never {
    fn encode(&self, _stream: &str, _rows: &[WalRow]) -> EncodeResult {
        Ok(None)
    }
}

fn meta() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("host".to_string(), "a".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 1_000,
    }
}

fn source(path: &std::path::Path) -> SourceRow {
    Archive::open(path)
        .unwrap()
        .read_sources()
        .unwrap()
        .remove(0)
}

/// Observe `path`'s source every `BEAT / 2` for `beats` beats.
fn watch_for(path: &std::path::Path, watch: &mut HeartbeatWatch, beats: u32) -> Vec<WriterState> {
    let end = Instant::now() + BEAT * beats;
    let mut seen = Vec::new();
    while Instant::now() < end {
        seen.push(watch.observe(&source(path), Instant::now()));
        std::thread::sleep(BEAT / 2);
    }
    seen
}

/// An idle writer still beats; a reader sees it live throughout, and the
/// row says how often it beats.
#[test]
fn an_idle_writer_beats_and_reads_as_live() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("live.dendro");
    let mut writer = Writer::create_beating_every(&path, Box::new(Never), BEAT).unwrap();
    let handle = writer.add_source(meta()).unwrap();

    let first = source(&path);
    assert_eq!(first.heartbeat_interval_ns, Some(BEAT.as_nanos() as i64));
    let mut watch = HeartbeatWatch::new();
    let states = watch_for(&path, &mut watch, 8);
    assert!(states.iter().all(|s| *s == WriterState::Live), "{states:?}");
    assert!(source(&path).heartbeat.unwrap() > first.heartbeat.unwrap() + 3);

    writer.finalize_single(handle, (2_000, 0)).unwrap();
    assert_eq!(
        watch.observe(&source(&path), Instant::now()),
        WriterState::Complete
    );
}

/// A writer that goes away without finalizing (a killed process, here its
/// handles dropped) stops beating, and a reader sees it stop.
#[test]
fn a_writer_that_stops_without_finalizing_reads_as_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("stopped.dendro");
    let mut writer = Writer::create_beating_every(&path, Box::new(Never), BEAT).unwrap();
    let handle = writer.add_source(meta()).unwrap();
    std::thread::sleep(BEAT * 2);
    drop(handle);
    writer.join().unwrap();
    drop(writer);

    assert!(!source(&path).complete);
    let mut watch = HeartbeatWatch::new();
    let states = watch_for(&path, &mut watch, 5);
    assert_eq!(states.first(), Some(&WriterState::Live), "{states:?}");
    assert_eq!(states.last(), Some(&WriterState::Stopped), "{states:?}");
}

/// A copy of a running source carries its heartbeat as it stood, and reads
/// as stopped once the reader has watched it not change.
#[test]
fn a_copy_of_a_running_source_reads_as_stopped() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("running.dendro");
    let mut writer = Writer::create_beating_every(&path, Box::new(Never), BEAT).unwrap();
    let _handle = writer.add_source(meta()).unwrap();
    std::thread::sleep(BEAT * 2);

    let copied = dir.path().join("copy.dendro");
    let src = Archive::open(&path).unwrap();
    let mut dst = ArchiveMut::create(&copied).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Never))
        .unwrap();
    drop(dst);
    let copy = source(&copied);
    assert!(copy.heartbeat.is_some());
    assert_eq!(copy.heartbeat_interval_ns, Some(BEAT.as_nanos() as i64));

    let mut watch = HeartbeatWatch::new();
    let states = watch_for(&copied, &mut watch, 5);
    assert_eq!(states.last(), Some(&WriterState::Stopped), "{states:?}");
    // The original, still being written, stays live.
    let mut original = HeartbeatWatch::new();
    let states = watch_for(&path, &mut original, 5);
    assert!(states.iter().all(|s| *s == WriterState::Live), "{states:?}");
}

/// A file from before the columns reads as unknown, and a writer that
/// reopens it adds them and beats a resumed source.
#[test]
fn an_archive_without_the_columns_reads_as_unknown_until_a_writer_resumes_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.dendro");
    let mut writer = Writer::create(&path, Box::new(Never)).unwrap();
    let handle = writer.add_source(meta()).unwrap();
    drop(handle);
    writer.join().unwrap();
    drop(writer);
    {
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch(
            "ALTER TABLE sources DROP COLUMN heartbeat; \
             ALTER TABLE sources DROP COLUMN heartbeat_interval_ns;",
        )
        .unwrap();
    }
    let old = source(&path);
    assert_eq!((old.heartbeat, old.heartbeat_interval_ns), (None, None));
    assert_eq!(
        HeartbeatWatch::new().observe(&old, Instant::now()),
        WriterState::Unknown
    );

    let mut writer = Writer::open(&path, Box::new(Never)).unwrap();
    let _resumed = writer.resume_source(old.id, 3_000).unwrap();
    let resumed = source(&path);
    assert!(resumed.heartbeat.is_some());
    assert_eq!(
        resumed.heartbeat_interval_ns,
        Some(dendro::writer::HEARTBEAT_INTERVAL.as_nanos() as i64)
    );
}

/// A resumed source continues the count a previous writer left, so a reader
/// that saw that writer stop sees the resumed one start.
#[test]
fn a_resumed_source_continues_its_count() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("resume.dendro");
    let mut writer = Writer::create_beating_every(&path, Box::new(Never), BEAT).unwrap();
    let handle = writer.add_source(meta()).unwrap();
    std::thread::sleep(BEAT * 3);
    drop(handle);
    writer.join().unwrap();
    drop(writer);
    let before = source(&path).heartbeat.unwrap();
    assert!(before > 1);

    let mut writer = Writer::open(&path, Box::new(Never)).unwrap();
    let _resumed = writer.resume_source(source(&path).id, 3_000).unwrap();
    assert_eq!(source(&path).heartbeat, Some(before + 1));
}

/// A copy into an archive without the columns succeeds, and its sources
/// read as unknown.
#[test]
fn a_copy_into_an_older_archive_reads_as_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("running.dendro");
    let mut writer = Writer::create_beating_every(&path, Box::new(Never), BEAT).unwrap();
    let _handle = writer.add_source(meta()).unwrap();
    std::thread::sleep(BEAT);

    let copied = dir.path().join("older.dendro");
    drop(ArchiveMut::create(&copied).unwrap());
    rusqlite::Connection::open(&copied)
        .unwrap()
        .execute_batch(
            "ALTER TABLE sources DROP COLUMN heartbeat; \
             ALTER TABLE sources DROP COLUMN heartbeat_interval_ns;",
        )
        .unwrap();
    let src = Archive::open(&path).unwrap();
    let mut dst = ArchiveMut::open(&copied).unwrap();
    dst.transaction(|tx| copy_sources_into(&src, tx, &CopySpec::everything(), &Never))
        .unwrap();
    drop(dst);
    assert_eq!(
        HeartbeatWatch::new().observe(&source(&copied), Instant::now()),
        WriterState::Unknown
    );
}

/// A file with one of the two columns reads, and a writer that reopens it
/// adds the other.
#[test]
fn half_of_the_columns_still_reads_and_is_completed_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("half.dendro");
    let mut writer = Writer::create(&path, Box::new(Never)).unwrap();
    drop(writer.add_source(meta()).unwrap());
    writer.join().unwrap();
    drop(writer);
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("ALTER TABLE sources DROP COLUMN heartbeat_interval_ns;")
        .unwrap();
    let half = source(&path);
    assert_eq!(half.heartbeat_interval_ns, None);

    let mut writer = Writer::open(&path, Box::new(Never)).unwrap();
    let _resumed = writer.resume_source(half.id, 3_000).unwrap();
    assert!(source(&path).heartbeat_interval_ns.is_some());
}

/// An interval of zero or less is not a heartbeat a reader can judge.
#[test]
fn a_nonpositive_interval_reads_as_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("zero.dendro");
    let mut writer = Writer::create_beating_every(&path, Box::new(Never), BEAT).unwrap();
    let handle = writer.add_source(meta()).unwrap();
    writer.finalize_single(handle, (2_000, 0)).unwrap();
    rusqlite::Connection::open(&path)
        .unwrap()
        .execute_batch("UPDATE sources SET complete = 0, heartbeat_interval_ns = 0;")
        .unwrap();
    let mut watch = HeartbeatWatch::new();
    let row = source(&path);
    assert_eq!(watch.observe(&row, Instant::now()), WriterState::Unknown);
    assert_eq!(
        watch.observe(&row, Instant::now() + Duration::from_secs(60)),
        WriterState::Unknown
    );
}

/// A writer kept busy with ticks still beats on time.
#[test]
fn a_busy_writer_beats() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("busy.dendro");
    let mut writer = Writer::create_beating_every(&path, Box::new(Never), BEAT).unwrap();
    let mut handle = writer.add_source(meta()).unwrap();
    let first = source(&path).heartbeat.unwrap();
    let end = Instant::now() + BEAT * 5;
    let mut ts = 10_000i64;
    while Instant::now() < end {
        ts += 1;
        handle
            .wal(vec![WalRow {
                stream: "s".to_string(),
                ts,
                wall_offset: 0,
                row: vec![0],
            }])
            .unwrap();
    }
    handle.sync().unwrap();
    assert!(source(&path).heartbeat.unwrap() >= first + 3);
}
