// A writer in another process; its own binary, so the child it spawns
// cannot inherit the lock descriptors of other tests running alongside it.
#![cfg(all(feature = "write", feature = "test-support"))]

//! One writing handle per archive, across two processes.

use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};

use dendro::archive::WalRow;
use dendro::segment::{EncodeContext, EncodeResult, SegmentEncoder};
use dendro::writer::Writer;
use dendro::Error;

struct Never;
impl SegmentEncoder for Never {
    fn encode(&self, _ctx: &EncodeContext<'_>, _rows: &[WalRow]) -> EncodeResult {
        Ok(None)
    }
}

fn in_use<T: std::fmt::Debug>(r: Result<T, Error>) -> bool {
    matches!(r, Err(Error::InUse { .. }))
}

/// Kills the child on drop, so a failed assertion does not leave a writer
/// process running.
struct Reaped(std::process::Child);
impl Drop for Reaped {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// A writer in another process holds the file; the lock goes with that
/// process, so a SIGKILL releases it and the archive can be reopened.
#[test]
fn a_writer_in_another_process_holds_the_file_until_it_dies() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("k.dendro");
    let mut child = Reaped(
        Command::new(env!("CARGO_BIN_EXE_append-until-killed"))
            .arg(&path)
            .arg("3")
            .arg("100")
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn the fixture writer"),
    );
    let mut out = BufReader::new(child.0.stdout.take().unwrap());
    let mut line = String::new();
    out.read_line(&mut line).unwrap();
    assert_eq!(line.trim(), "ready");

    assert!(in_use(Writer::open(&path, Box::new(Never))));
    drop(child);
    let mut w = Writer::open(&path, Box::new(Never)).unwrap();
    w.join().unwrap();
}
