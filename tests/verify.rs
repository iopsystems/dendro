//! "Is this archive sound?" — a question an artifact that travels has to be
//! able to answer without reading all of it and seeing whether anything
//! throws.

use std::collections::BTreeMap;

use dendro::db::{Db, Depth, Problem, SegmentMeta, SourceMeta, WalRow};

fn meta() -> SourceMeta {
    SourceMeta {
        labels: BTreeMap::from([("source".to_string(), "a".to_string())]),
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

/// A healthy archive reports sound, at both depths, with the counts a caller
/// would otherwise assemble by hand.
#[test]
fn a_healthy_archive_is_sound_and_counted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v.dendro");
    let mut db = Db::create(&path).unwrap();
    let id = db.insert_source(&meta()).unwrap();
    db.insert_segment(
        id,
        "s",
        0,
        &SegmentMeta {
            rows: 2,
            first_ts: 1,
            last_ts: 2,
        },
        b"1,2",
    )
    .unwrap();
    db.insert_wal_rows(id, &[row(3), row(4)]).unwrap();

    for depth in [Depth::Quick, Depth::Full] {
        let report = db.verify(depth).unwrap();
        assert!(report.is_sound(), "{:?}", report.problems);
        assert_eq!(
            (
                report.sources,
                report.streams,
                report.segments,
                report.wal_rows
            ),
            (1, 1, 1, 2)
        );
    }
}

/// The finding worth having: rows the watermark shadows. A current writer
/// drops such an append, but an archive written before it did is carrying
/// space spent on rows nothing can read, and nothing said so.
#[test]
fn wal_rows_no_read_path_can_reach_are_reported() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v.dendro");
    let mut db = Db::create(&path).unwrap();
    let id = db.insert_source(&meta()).unwrap();
    // Rows at 1 and 2, then a segment sealing through 2: both are shadowed.
    db.insert_wal_rows(id, &[row(1), row(2), row(9)]).unwrap();
    db.insert_segment(
        id,
        "s",
        0,
        &SegmentMeta {
            rows: 2,
            first_ts: 1,
            last_ts: 2,
        },
        b"1,2",
    )
    .unwrap();

    let report = db.verify(Depth::Quick).unwrap();
    assert_eq!(
        report.problems,
        vec![Problem::UnreadableWalRows {
            source_id: id,
            stream: "s".to_string(),
            rows: 2,
        }]
    );
    assert_eq!(report.wal_rows, 3, "all three are stored; only one is live");
    assert!(report.problems[0]
        .to_string()
        .contains("no read path can reach"));

    // Pruning them is what fixes it, and verify then says so.
    db.prune_wal(id, "s", 2).unwrap();
    assert!(db.verify(Depth::Quick).unwrap().is_sound());
}

/// A segment whose catalog entry contradicts itself. Not reachable through
/// the writer, which validates the encoder; reachable by hand, and by a
/// future bug, which is what the check is for.
#[test]
fn a_self_contradicting_segment_is_reported() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v.dendro");
    let mut db = Db::create(&path).unwrap();
    let id = db.insert_source(&meta()).unwrap();
    db.insert_segment(
        id,
        "backwards",
        0,
        &SegmentMeta {
            rows: 1,
            first_ts: 100,
            last_ts: 1,
        },
        b"x",
    )
    .unwrap();
    db.insert_segment(
        id,
        "empty",
        0,
        &SegmentMeta {
            rows: 0,
            first_ts: 1,
            last_ts: 1,
        },
        b"x",
    )
    .unwrap();

    let report = db.verify(Depth::Quick).unwrap();
    assert_eq!(report.problems.len(), 2, "{:?}", report.problems);
    let text: Vec<String> = report.problems.iter().map(|p| p.to_string()).collect();
    assert!(
        text.iter().any(|t| t.contains("runs backwards")),
        "{text:?}"
    );
    assert!(
        text.iter().any(|t| t.contains("claims no rows")),
        "{text:?}"
    );
}

/// Damage to the file itself is SQLite's to find, and both depths find it.
///
/// Worth pinning at both, because it is easy to assume otherwise — this test
/// was first written asserting the quick pass would MISS a scribbled page,
/// and it failed, which is the useful kind of failure. `quick_check` walks
/// the whole database too; what it skips is cross-checking index entries
/// against table rows, not reading pages. So the depth parameter buys index
/// consistency, and for bit-rot the cheap pass is very nearly as good.
#[test]
fn damage_inside_the_file_is_found_at_either_depth() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v.dendro");
    {
        let mut db = Db::create(&path).unwrap();
        let id = db.insert_source(&meta()).unwrap();
        // Big enough to occupy pages well past the header and the catalog.
        for seq in 0..40u64 {
            db.insert_segment(
                id,
                "s",
                seq,
                &SegmentMeta {
                    rows: 1,
                    first_ts: seq as i64,
                    last_ts: seq as i64,
                },
                &vec![seq as u8; 8192],
            )
            .unwrap();
        }
        db.checkpoint_passive().unwrap();
    }

    // Scribble over the middle of the file, well past page 1.
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    for b in &mut bytes[middle..middle + 4096] {
        *b ^= 0xff;
    }
    std::fs::write(&path, &bytes).unwrap();

    let db = Db::open_read_only(&path).unwrap();
    let report = db.verify(Depth::Full).unwrap();
    assert!(
        report
            .problems
            .iter()
            .any(|p| matches!(p, Problem::Corrupt(_))),
        "the deep check must notice a scribbled page: {:?}",
        report.problems
    );
    let quick = db.verify(Depth::Quick).unwrap();
    assert!(
        quick
            .problems
            .iter()
            .any(|p| matches!(p, Problem::Corrupt(_))),
        "the cheap pass walks the database too, and must not miss this: {:?}",
        quick.problems
    );
}
