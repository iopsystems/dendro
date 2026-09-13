//! Appends to an archive, commits, and then waits to be killed.
//!
//! A test binary, not a tool. An unclean kill cannot be staged in-process:
//! `Archive`'s `Drop` joins the writer, and anything that runs `Drop` is by
//! definition a clean close. So the only honest way to test what a SIGKILL
//! leaves behind is to put the writer in a process and kill that.
//!
//! Usage: `append-until-killed <path> <rows> <seal_after>`. Appends rows at
//! ts `1..=rows`, seals once after `seal_after` (so the archive is killed
//! holding both sealed segments and a live WAL tail), commits everything,
//! prints `ready`, and then sleeps. It never finalizes and never joins.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::Path;
use std::time::Duration;

use dendro::db::{SourceMeta, WalRow};
use dendro::segment::{EncodeResult, Segment, SegmentEncoder};
use dendro::writer::Archive;

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

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [path, rows, seal_after] = args.as_slice() else {
        eprintln!("usage: append-until-killed <path> <rows> <seal_after>");
        std::process::exit(2);
    };
    let rows: i64 = rows.parse().expect("rows");
    let seal_after: i64 = seal_after.parse().expect("seal_after");

    let seed = SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "killed".to_string())]),
        metadata: BTreeMap::new(),
        clock_anchor_wall_ns: 1_000,
    };
    let (archive, mut w) = Archive::single(Path::new(path), Box::new(Tags), seed).expect("create");
    for ts in 1..=rows {
        w.wal(vec![WalRow {
            stream: "s".to_string(),
            ts,
            wall_offset: 0,
            row: vec![1],
        }])
        .expect("append");
        if ts == seal_after {
            w.seal(vec!["s".to_string()]).expect("seal");
        }
    }
    // Committed, not finalized: everything above is durable, and the source's
    // `complete` is still 0 — which is exactly the state a kill should find.
    w.sync().expect("sync");

    println!("ready");
    std::io::stdout().flush().expect("flush");

    // Neither handle is dropped: `Drop` would join the writer and close the
    // connection cleanly, which is the one thing this binary must not do.
    std::mem::forget(w);
    std::mem::forget(archive);
    loop {
        std::thread::sleep(Duration::from_secs(3600));
    }
}
