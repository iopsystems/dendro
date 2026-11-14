// Spawns a resumed copy in its own process and kills it, so it needs the
// writer and the fixture binary behind `test-support`.
#![cfg(all(feature = "write", feature = "test-support"))]

//! A replication copy killed partway through a resume.
//!
//! In its own test binary, apart from `replicate_resume.rs`. Spawning a child
//! gives it a copy of this process's descriptors until it execs, with
//! `posix_spawn` as with `fork` (measured on macOS). `flock` locks belong to the open file
//! description, so a writer lock a test running beside the spawn has just
//! dropped stays held for that window, and the test's reopen fails as
//! `InUse` (`src/archive/lock.rs`).

use std::collections::BTreeMap;
use std::path::Path;

use dendro::archive::{Archive, SourceMeta, WalRow};
use dendro::replicate::wire::{FrameReader, FrameWriter, PROTOCOL_VERSION_2};
use dendro::replicate::{Applied, ArchivePublisher, Frame, ResumeRequest, Subscriber};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::{SourceWriter, Writer};

/// Rows as a comma-joined list of timestamps, so a reader can list what a
/// segment holds without parquet. Refuses rows in `[1000, 2000)`, which
/// stand for rows an encoder cannot seal.
struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
        if rows.is_empty() {
            return Ok(None);
        }
        if rows.iter().any(|r| (1000..2000).contains(&r.ts)) {
            return Err("rows in [1000, 2000) do not encode".into());
        }
        Ok(Some(Segment {
            bytes: tags(rows.iter().map(|r| r.ts)),
            rows: rows.len() as u64,
            first_ts: rows[0].ts,
            last_ts: rows[rows.len() - 1].ts,
            index: None,
        }))
    }
}

fn tags(ts: impl Iterator<Item = i64>) -> Vec<u8> {
    ts.map(|t| t.to_string())
        .collect::<Vec<_>>()
        .join(",")
        .into_bytes()
}

fn row(stream: &str, ts: i64) -> WalRow {
    WalRow {
        stream: stream.to_string(),
        ts,
        wall_offset: 0,
        row: ts.to_le_bytes().to_vec(),
    }
}

/// The origin: one source, written and sealed by a test.
struct Origin {
    archive: Writer,
    w: SourceWriter,
}

impl Origin {
    fn create(path: &Path) -> Self {
        let mut archive = Writer::create(path, Box::new(Tags)).unwrap();
        let w = archive
            .add_source(SourceMeta {
                labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
                metadata: BTreeMap::new(),
                clock_anchor_wall_ns: 0,
            })
            .unwrap();
        Origin { archive, w }
    }

    fn rows(&mut self, stream: &str, ts: impl IntoIterator<Item = i64>) {
        self.w
            .wal(ts.into_iter().map(|t| row(stream, t)).collect())
            .unwrap();
        self.w.sync().unwrap();
    }

    fn seal(&mut self, streams: &[&str]) {
        self.w
            .seal(streams.iter().map(|s| s.to_string()).collect())
            .unwrap();
        self.w.sync().unwrap();
    }

    fn close(self) {
        drop(self.w);
        let mut archive = self.archive;
        archive.join().unwrap();
    }
}

/// Every timestamp a file's one source holds of each stream, sealed or live.
fn readable(path: &Path) -> BTreeMap<String, Vec<i64>> {
    let db = Archive::open(path).unwrap();
    let mut out = BTreeMap::new();
    for rec in db.read_sources().unwrap() {
        for stream in db.all_streams(rec.id).unwrap() {
            let mut ts: Vec<i64> = Vec::new();
            for seg in db
                .segments_overlapping(rec.id, &stream, i64::MIN, i64::MAX)
                .unwrap()
            {
                let text = String::from_utf8(seg.bytes).unwrap();
                ts.extend(text.split(',').map(|t| t.parse::<i64>().unwrap()));
            }
            ts.extend(db.live_wal(rec.id, &stream).unwrap().iter().map(|r| r.ts));
            if !ts.is_empty() {
                out.insert(stream, ts);
            }
        }
    }
    out
}

/// Through the version 2 codec and into the subscriber.
fn pump(frames: Vec<Frame>, sub: &mut Subscriber) -> Applied {
    let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
    for f in &frames {
        w.write(f).unwrap();
    }
    let mut reader = FrameReader::new(std::io::Cursor::new(w.into_inner())).unwrap();
    let mut decoded = Vec::new();
    while let Some(f) = reader.next_frame().unwrap() {
        decoded.push(f);
    }
    let applied = sub.apply_all(decoded).unwrap();
    sub.sync().unwrap();
    applied
}

/// Open the copy for a resume, and its request through the codec.
fn resume_copy(copy: &Path) -> (Subscriber, ResumeRequest) {
    let writer = if copy.exists() {
        Writer::open(copy, Box::new(Tags)).unwrap()
    } else {
        Writer::create(copy, Box::new(Tags)).unwrap()
    };
    let (sub, request) = Subscriber::resume(writer).unwrap();
    let request = ResumeRequest::decode(&request.encode().unwrap()).unwrap();
    (sub, request)
}

/// Resume `copy` from `origin`: the opening batch and the first poll, then
/// finish.
fn resume_from(origin: &Path, copy: &Path) {
    let (mut sub, request) = resume_copy(copy);
    let db = Archive::open(origin).unwrap();
    let (mut publisher, mut frames) = ArchivePublisher::resuming(&db, &request).unwrap();
    frames.extend(publisher.next(&db).unwrap());
    pump(frames, &mut sub);
    sub.finish().unwrap();
}

/// A child process killed and reaped when dropped, so a failed assertion
/// does not leave it running.
struct Reaped(std::process::Child);

impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// When to kill the copy in `a_copy_killed_during_a_sync_resumes_to_the_origin`.
#[derive(Clone, Copy, Debug)]
enum Kill {
    /// This long after it says it is about to apply the `Sync`.
    After(u64),
    /// Once the `Sync` is committed, before the frames after it.
    BetweenFrames,
    /// After the last frame.
    Done,
}

/// A copy killed partway through a resume opens, verifies clean, holds no
/// row twice, and resumes to the origin's rows. The `Sync` is one
/// transaction, so a kill leaves the stream as it was or as the `Sync` left
/// it, and a kill between frames leaves the frames before it applied.
#[test]
fn a_copy_killed_during_a_sync_resumes_to_the_origin() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    let mut kills: Vec<Kill> = (0..=44).step_by(4).map(Kill::After).collect();
    kills.extend([Kill::BetweenFrames, Kill::Done]);
    for kill in kills {
        let dir = tempfile::tempdir().unwrap();
        let (o, c) = (dir.path().join("o"), dir.path().join("c"));
        let mut origin = Origin::create(&o);
        origin.rows("s", 10_001..=30_000);
        resume_from(&o, &c);
        // Sealed while the copy is away: its 20,000 live rows go in a `Sync`.
        origin.seal(&["s"]);
        origin.rows("s", 30_001..=30_010);

        let (sub, request) = resume_copy(&c);
        sub.finish().unwrap();
        let db = Archive::open(&o).unwrap();
        let (mut publisher, mut frames) = ArchivePublisher::resuming(&db, &request).unwrap();
        frames.extend(publisher.next(&db).unwrap());
        assert!(frames.iter().any(|f| matches!(f, Frame::Sync { .. })));
        let mut w = FrameWriter::new(Vec::new(), PROTOCOL_VERSION_2).unwrap();
        for f in &frames {
            w.write(f).unwrap();
        }
        let recorded = dir.path().join("stream");
        std::fs::write(&recorded, w.into_inner()).unwrap();

        let mut cmd = Command::new(env!("CARGO_BIN_EXE_resume-until-killed"));
        cmd.arg(&c).arg(&recorded);
        if matches!(kill, Kill::BetweenFrames) {
            cmd.arg("pause");
        }
        let mut child = Reaped(
            cmd.stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .spawn()
                .unwrap(),
        );
        // Held open until after the kill, so a paused child does not see EOF.
        let stdin = child.0.stdin.take().unwrap();
        let mut out = BufReader::new(child.0.stdout.take().unwrap());
        let mut line = String::new();
        out.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "syncing");
        let expect = |out: &mut BufReader<_>, want: &str| {
            let mut line = String::new();
            out.read_line(&mut line).unwrap();
            assert_eq!(line.trim(), want);
        };
        match kill {
            Kill::After(ms) => std::thread::sleep(std::time::Duration::from_millis(ms)),
            Kill::BetweenFrames => expect(&mut out, "synced"),
            Kill::Done => expect(&mut out, "done"),
        }
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        drop(stdin);

        let db = Archive::open(&c).unwrap();
        let report = db.verify(dendro::archive::Depth::Quick).unwrap();
        assert!(
            report.problems.is_empty(),
            "{kill:?}: {:?}",
            report.problems
        );
        drop(db);
        let held = readable(&c).remove("s").unwrap_or_default();
        assert!(
            held.windows(2).all(|p| p[0] < p[1]),
            "{kill:?}: a row twice"
        );
        match kill {
            // The `Sync` deleted the copy's live rows, and the segment that
            // replaces them has not arrived.
            Kill::BetweenFrames => assert!(held.is_empty(), "{kill:?}: {}", held.len()),
            Kill::Done => assert_eq!(held, readable(&o)["s"]),
            // As it was, with the copy's 20,000 rows; as the `Sync` left it,
            // with none; or later, with the origin's rows up to a point.
            Kill::After(_) => {
                let all = &readable(&o)["s"];
                assert!(
                    held.is_empty() || (held.len() >= 20_000 && held[..] == all[..held.len()]),
                    "{kill:?}: {} rows",
                    held.len()
                );
            }
        }

        resume_from(&o, &c);
        assert_eq!(readable(&c), readable(&o), "{kill:?}");
        origin.close();
    }
}
