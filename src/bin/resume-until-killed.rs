//! Resumes a replication copy from a recorded stream, and waits to be killed.
//!
//! A test binary, not a tool. A kill partway through a `Sync` cannot be
//! staged in-process, because dropping the subscriber joins its writer and
//! closes cleanly; so the copy is written in this process and the test kills
//! it.
//!
//! Usage: `resume-until-killed <copy> <stream> [pause]`. Opens `<copy>` with
//! `Subscriber::resume`, reads the version 2 stream in the file `<stream>`
//! (one the test recorded from `ArchivePublisher::resuming`), and applies its
//! frames in order. It prints `syncing` just before applying the first
//! `Sync` frame, and `done` after the last frame, then sleeps without
//! finishing the subscriber. With `pause`, it also prints `synced` once that
//! `Sync` is committed and waits for a line on stdin before going on, so a
//! test can kill it between the `Sync` and the frames after it.

use std::io::Write as _;
use std::path::Path;
use std::time::Duration;

use dendro::archive::WalRow;
use dendro::replicate::wire::FrameReader;
use dendro::replicate::{Frame, Subscriber};
use dendro::segment::{EncodeContext, EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Writer;

/// Rows as a comma-joined list of timestamps, as `tests/resume_kill.rs` reads
/// them; unlike that file's `Tags`, it refuses nothing.
struct Tags;

impl SegmentEncoder for Tags {
    fn encode(&self, _ctx: &EncodeContext<'_>, rows: &[WalRow]) -> EncodeResult {
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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (copy, stream, pause) = match args.as_slice() {
        [copy, stream] => (copy, stream, false),
        [copy, stream, pause] if pause == "pause" => (copy, stream, true),
        _ => {
            eprintln!("usage: resume-until-killed <copy> <stream> [pause]");
            std::process::exit(2);
        }
    };
    let writer = Writer::open(Path::new(copy), Box::new(Tags)).expect("open the copy");
    let (mut sub, _) = Subscriber::resume(writer).expect("resume");
    let bytes = std::fs::read(stream).expect("read the stream");
    let mut reader = FrameReader::new(bytes.as_slice()).expect("preamble");
    let mut said = false;
    while let Some(frame) = reader.next_frame().expect("frame") {
        let first_sync = !said && matches!(frame, Frame::Sync { .. });
        if first_sync {
            println!("syncing");
            std::io::stdout().flush().expect("flush");
            said = true;
        }
        sub.apply(frame).expect("apply");
        if first_sync && pause {
            sub.sync().expect("sync");
            println!("synced");
            std::io::stdout().flush().expect("flush");
            let mut line = String::new();
            std::io::stdin().read_line(&mut line).expect("stdin");
        }
    }
    sub.sync().expect("sync");
    println!("done");
    std::io::stdout().flush().expect("flush");

    // Not finished: `finish` would join the writer and close cleanly.
    std::mem::forget(sub);
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
