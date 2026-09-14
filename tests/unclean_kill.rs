// Spawns a writer and kills it, so it needs both the writer and the
// test-support constructors the fixture binary uses.
#![cfg(all(feature = "write", feature = "test-support"))]

//! What a SIGKILL leaves behind.
//!
//! `DESIGN.md` quotes recovery numbers for an unclean kill and, until now,
//! nothing asserted them: every "kill" test in the crate was a `drop`, which
//! joins the writer and is by definition a clean close. These stage the real
//! thing — a writer in its own process, killed with no chance to run `Drop` —
//! and check the three states it can leave: the archive with its sidecars
//! (everything committed is there), the archive alone (stale, not broken),
//! and a truncated sidecar (recovered to its last intact frame).

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use dendro::archive::{Archive, WalRow};
use dendro::read;
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
            index: None,
        }))
    }
}

/// Run the fixture writer until it has committed `rows` appends (sealing once
/// after `seal_after`), then SIGKILL it. Returns once the process is reaped,
/// so the archive on disk is in the state the kill left it.
fn write_then_kill(path: &std::path::Path, rows: i64, seal_after: i64) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_append-until-killed"))
        .arg(path)
        .arg(rows.to_string())
        .arg(seal_after.to_string())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn the fixture writer");
    let mut out = BufReader::new(child.stdout.take().expect("stdout"));
    let mut line = String::new();
    out.read_line(&mut line).expect("read readiness");
    assert_eq!(line.trim(), "ready", "the fixture writer did not commit");

    // SIGKILL on unix: no unwinding, no `Drop`, no clean close.
    child.kill().expect("kill");
    child.wait().expect("reap");
}

/// SQLite appends the suffix to the whole filename, not in place of the
/// extension: `x.dendro` has `x.dendro-wal`.
fn sidecar(path: &std::path::Path) -> std::path::PathBuf {
    let mut p = path.to_path_buf().into_os_string();
    p.push("-wal");
    std::path::PathBuf::from(p)
}

fn rows_of(db: &Archive) -> Vec<String> {
    read::read_archive(db, &Tags)
        .expect("a killed archive still opens")
        .into_iter()
        .flat_map(|src| src.streams)
        .flat_map(|(_, segments)| segments)
        .map(|b| String::from_utf8(b).expect("utf8 fixture bytes"))
        .collect()
}

/// The headline claim: an unclean kill costs nothing that was committed. The
/// archive opens, both halves come back — the sealed segment and the live WAL
/// tail past it — and the source reads as unfinished, which is how a consumer
/// knows it was killed rather than closed.
#[test]
fn an_unclean_kill_keeps_every_committed_append() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("killed.dendro");
    write_then_kill(&path, 5, 3);

    // Sidecars survive a kill; that is where the recent commits are.
    assert!(
        sidecar(&path).exists(),
        "a killed writer leaves its -wal behind"
    );

    let db = Archive::open(&path).expect("a killed archive opens");
    assert_eq!(
        rows_of(&db),
        vec!["1,2,3".to_string(), "4,5".to_string()],
        "the sealed segment and the live tail past it both come back"
    );
    let sources = db.read_sources().unwrap();
    assert_eq!(sources.len(), 1);
    assert!(
        !sources[0].complete,
        "a killed source is not complete, which is how a reader knows"
    );
    assert!(sources[0].uuid.is_some(), "identity survives a kill");
}

/// The archive file ALONE — what a `cp` of a live recording gets you — opens
/// and is stale rather than broken. It cannot hold more than the full set,
/// and the catalog is there because creation checkpoints it.
#[test]
fn the_archive_without_its_sidecar_is_stale_not_broken() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("killed.dendro");
    write_then_kill(&path, 5, 3);

    // Copy BEFORE opening the original: a read-write open checkpoints on
    // close and would fold the sidecar in, which is the thing being measured.
    let alone = dir.path().join("alone.dendro");
    std::fs::copy(&path, &alone).unwrap();

    let copied = rows_of(&Archive::open(&alone).expect("the copy opens"));
    let whole = rows_of(&Archive::open(&path).expect("the original opens"));
    let copied_rows: usize = copied.iter().map(|s| s.split(',').count()).sum();
    let whole_rows: usize = whole.iter().map(|s| s.split(',').count()).sum();
    assert!(
        copied_rows <= whole_rows,
        "the copy cannot hold more than the archive and its sidecar: \
         {copied_rows} vs {whole_rows}"
    );
    assert_eq!(whole_rows, 5, "the full set holds everything committed");
    // And it is a real archive, not a diagnosis: no source it reports is
    // invented, and its own catalog reads.
    for src in read::catalog(&Archive::open(&alone).unwrap()).unwrap() {
        assert!(src.uuid.is_some());
    }
}

/// A sidecar torn mid-frame — a kill during a commit, or a partial copy — is
/// recovered to its last intact frame rather than refused. SQLite's WAL
/// format is what does this; the test is here because the claim is the
/// container's and nothing checked it.
#[test]
fn a_truncated_sidecar_recovers_to_its_last_intact_frame() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("killed.dendro");
    write_then_kill(&path, 5, 3);

    let wal = sidecar(&path);
    let before = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    assert!(
        before > 0,
        "fixture: the kill must leave a non-empty sidecar"
    );

    // Lop off the tail of the log, mid-frame.
    let truncated = std::fs::OpenOptions::new().write(true).open(&wal).unwrap();
    truncated.set_len(before - (before / 4).max(1)).unwrap();
    drop(truncated);

    // Opens, and answers with whatever whole frames survived — never more
    // than the five that were committed, and never an error.
    let db = Archive::open(&path).expect("a torn sidecar is recovered, not refused");
    let rows: usize = rows_of(&db).iter().map(|s| s.split(',').count()).sum();
    assert!(
        rows <= 5,
        "a torn log cannot yield more than was written: {rows}"
    );
    assert_eq!(
        db.read_sources().unwrap().len(),
        1,
        "the catalog survives, because creation checkpointed it into the archive"
    );
}

/// A killed source can be picked up where it stopped: the archive reopens for
/// append, `resume_source` refuses to run backwards over what the kill left,
/// and the sequence continues. The recovery path and the resume path meet
/// here, and nothing had tested them together.
#[test]
fn a_killed_source_can_be_resumed() {
    use dendro::writer::Writer;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("killed.dendro");
    write_then_kill(&path, 5, 3);

    let id = Archive::open(&path).unwrap().read_sources().unwrap()[0].id;
    let mut archive = Writer::open(&path, Box::new(Tags)).expect("reopen after a kill");
    // The newest row the killed writer left is 5, so an anchor at or before
    // it is refused and one after it is taken.
    assert!(archive.resume_source(id, 5).is_err(), "5 is not after 5");
    let (mut w, last) = archive.resume_source(id, 6).expect("resume");
    assert_eq!(last, Some(5), "resumed after the killed writer's last row");

    w.wal(vec![WalRow {
        stream: "s".to_string(),
        ts: 7,
        wall_offset: 0,
        row: vec![1],
    }])
    .unwrap();
    w.seal(vec!["s".to_string()]).unwrap();
    archive.finalize_single(w, (7, 0)).unwrap();

    let db = Archive::open(&path).unwrap();
    assert!(
        db.read_sources().unwrap()[0].complete,
        "finalized this time"
    );
    let seqs: Vec<u64> = db
        .read_segment_meta(id, "s")
        .unwrap()
        .into_iter()
        .map(|(seq, _)| seq)
        .collect();
    assert_eq!(
        seqs,
        vec![0, 1],
        "the killed writer's seq 0 was continued, not reused"
    );
    let sessions: Vec<serde_json::Value> =
        serde_json::from_str(&db.source_metadata(id).unwrap()[dendro::keys::WRITER_SESSIONS])
            .unwrap();
    assert_eq!(
        sessions.len(),
        2,
        "the kill and the resume are both on record"
    );
    assert_eq!(sessions[1]["resumed_after_ts"], 5);
}
