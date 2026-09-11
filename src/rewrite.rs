//! Rewriting an archive: combining, trimming and time-bounding, without
//! decoding a segment.
//!
//! Every one of those produces a new archive from existing ones by passing the
//! parquet BLOBs through byte-identical and changing only the catalog around
//! them. A ranged copy is the same operation with a time bound, so they share
//! this one implementation rather than each growing their own — the WAL-tail
//! handling below is subtle enough that a second implementation would be a
//! second set of bugs.
//!
//! Two decisions here are the caller's, because they are about what rows MEAN:
//! which streams to keep ([`CopySpec::keep_streams`](crate::rewrite::CopySpec::keep_streams)) and which columns of a
//! segment to keep ([`ColumnFilter`](crate::rewrite::ColumnFilter)).

use std::collections::BTreeMap;

use crate::db::{Db, SegmentMeta, Tx};
use crate::error::{Error, Result};
use crate::segment::SegmentEncoder;

/// Which columns of a segment survive a [`project_segment_columns`] pass.
///
/// dendro does not know what a column is for, so both halves of the decision
/// are here. They are separate questions: a segment keeps its structural
/// columns (timestamps and whatever sidecars the caller's row shape needs)
/// unconditionally, but a segment left holding ONLY those carries no data and
/// is dropped rather than written empty.
pub trait ColumnFilter {
    /// Keep this column in the projected segment?
    fn keep(&self, field: &arrow::datatypes::Field) -> bool;
    /// Does this column carry data, as opposed to being structural?
    fn is_data(&self, field: &arrow::datatypes::Field) -> bool;
}

/// What one copy pass carries across.
pub struct CopySpec<'a> {
    /// Row-timestamp bound in nanoseconds. The rewrite tools copy everything;
    /// a ranged dump narrows it to the incident window.
    pub start: u64,
    pub end: u64,
    /// Keep only the streams this accepts; `None` keeps every stream.
    ///
    /// A predicate rather than a name set because a caller may group streams
    /// under a coarser unit than the stream key — one an operator names, that
    /// owns several streams — and dropping that unit has to drop all of them.
    pub keep_streams: Option<&'a dyn Fn(&str) -> bool>,
    /// Extra metadata merged into each copied source's own, overwriting on
    /// key collision. `annotate` embeds KPIs this way; the others pass `None`.
    pub metadata_extra: Option<&'a BTreeMap<String, String>>,
    /// When set, project each copied segment's parquet down to the columns
    /// this accepts, decoding and re-encoding it. `None` is the fast path —
    /// segment BLOBs pass through byte-identical. This is the ONE copy that
    /// touches segment bytes; see [`project_segment_columns`]. A stream left
    /// with no data column is dropped.
    pub keep_columns: Option<&'a dyn ColumnFilter>,
}

impl CopySpec<'_> {
    /// Every source, every table, every row, metadata untouched.
    pub fn everything() -> Self {
        CopySpec {
            start: 0,
            end: u64::MAX,
            keep_streams: None,
            metadata_extra: None,
            keep_columns: None,
        }
    }
}

/// Copy every source in `src` into the open destination transaction,
/// returning how many sources were copied.
///
/// The destination transaction is the caller's so that `combine` can fold
/// several sources into one atomic write: either the combined archive has all
/// of its inputs or it does not exist.
///
/// Each copied source keeps its source's `complete` flag. That flag answers
/// "may data after the last row be missing", which is a property of the DATA
/// and survives being copied — a source recovered from a checkpoint rather
/// than cleanly finalized is still truncated after a combine or a filter, and
/// claiming otherwise would hide the loss. Missing beats wrong.
///
/// The one caller that overrides it is a ranged dump, which marks its copy
/// complete afterwards for a specific reason: the buffer it copied is
/// perpetually mid-source and would otherwise never produce a snapshot that
/// did not warn.
pub fn copy_sources_into(
    src: &Db,
    tx: &Tx<'_>,
    spec: &CopySpec<'_>,
    encoder: &dyn SegmentEncoder,
) -> Result<usize> {
    // ONE snapshot over every read of the source. The destination transaction
    // is the caller's and is on another connection, so this only bounds what we
    // read.
    //
    // Without it the source can move underneath a copy that takes four
    // separate reads of it - sources, streams, segments, live WAL. A seal
    // landing mid-copy is visible to some of those and not others, and
    // retention landing mid-copy can evict a segment between the query that
    // selected it and the read that copies its bytes. `read_snapshot`'s own
    // doc names that second one; this is the path it was describing.
    src.read_snapshot(|src| copy_sources_snapshotted(src, tx, spec, encoder))
}

fn copy_sources_snapshotted(
    src: &Db,
    tx: &Tx<'_>,
    spec: &CopySpec<'_>,
    encoder: &dyn SegmentEncoder,
) -> Result<usize> {
    let sources = src.read_sources()?;
    let mut copied = 0usize;
    for rec in &sources {
        let mut meta = rec.meta.clone();
        if let Some(extra) = spec.metadata_extra {
            for (k, v) in extra {
                meta.metadata.insert(k.clone(), v.clone());
            }
        }
        let id = tx.insert_source(&meta)?;
        if rec.complete {
            tx.mark_complete(id)?;
        }
        copied += 1;

        for table in src.all_streams(rec.id)? {
            if let Some(keep) = spec.keep_streams {
                if !keep(table.as_str()) {
                    continue;
                }
            }
            // `seq` is renumbered from 0 per table rather than carried over.
            // A filtered or range-bounded copy leaves holes in the source's
            // numbering, and the reader splices segments in `seq` order, so
            // the copy's own numbering has to be dense and start at zero.
            let mut seq = 0u64;
            for segment in src.segments_overlapping(rec.id, &table, spec.start, spec.end)? {
                match spec.keep_columns {
                    // Column trim re-encodes; a stream with none of the kept
                    // columns projects to no data column and is dropped (its
                    // segments simply never inserted). Row count, timestamps
                    // and windows are unchanged by a projection, so the
                    // segment's own `meta` is reused verbatim.
                    Some(keep) => {
                        if let Some(projected) = project_segment_columns(&segment.bytes, keep)? {
                            tx.insert_segment(id, &table, seq, &segment.meta, &projected)?;
                            seq += 1;
                        }
                    }
                    None => {
                        tx.insert_segment(id, &table, seq, &segment.meta, &segment.bytes)?;
                        seq += 1;
                    }
                }
            }

            // The unsealed tail is the newest data in the archive and the only
            // data a quiet table may have at all, so it is never optional —
            // only out of range. An archive still being written (a rolling buffer
            // buffer, or a source combined mid-flight) keeps real rows here
            // that no segment holds yet.
            let tail = src.live_wal(rec.id, &table)?;
            let (Some(first), Some(last)) = (tail.first(), tail.last()) else {
                continue;
            };
            if last.ts < spec.start || first.ts > spec.end {
                continue;
            }
            // `first`/`tail.len()` served the range check above and nothing
            // else: the catalog's `first_ts`/`rows` come from what actually
            // materializes, because an encoder may drop leading rows it cannot
            // yet decode. Cataloguing the raw tail's span would claim a start
            // the bytes do not contain. `last_ts` stays the raw tail's own last
            // row — a drop is always a leading run, so that one is always
            // right. See [`Segment`](crate::segment::Segment).
            let materialized = encoder
                .encode(&table, &tail)
                .map_err(|e| Error::Message(format!("failed to seal the {table} tail: {e}")))?;
            if let Some(materialized) = materialized {
                let meta = SegmentMeta {
                    rows: materialized.rows,
                    first_ts: materialized.first_ts,
                    last_ts: last.ts,
                };
                match spec.keep_columns {
                    Some(keep) => {
                        if let Some(projected) = project_segment_columns(&materialized.bytes, keep)?
                        {
                            tx.insert_segment(id, &table, seq, &meta, &projected)?;
                        }
                    }
                    None => {
                        tx.insert_segment(id, &table, seq, &meta, &materialized.bytes)?;
                    }
                }
            }
        }

        // Drift observations are part of the source's identity and cost
        // nothing to carry; they are already only a handful of rows per seal.
        for (ts, offset) in src.read_clock_offsets(rec.id)? {
            tx.insert_clock_offset(id, ts, offset)?;
        }
    }
    Ok(copied)
}

/// Re-encode one segment with only the columns `keep` accepts, or `None` when
/// nothing but structural columns survive.
///
/// This is the ONE operation in this module that touches segment bytes; every
/// other copy passes the parquet BLOB through verbatim. Row count, timestamps
/// and column values are unchanged by a projection, so a projected segment
/// reuses its source's catalog entry as-is.
///
/// The filter is the caller's because the structural columns are: dendro knows
/// a segment has columns, not which of them a reader cannot do without. An
/// implementation that drops a column its own reader needs to place rows in
/// time will produce a segment that opens and answers wrongly, so
/// [`ColumnFilter::keep`] should accept those unconditionally.
pub fn project_segment_columns(bytes: &[u8], keep: &dyn ColumnFilter) -> Result<Option<Vec<u8>>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .map_err(|e| Error::Message(format!("failed to open a segment for projection: {e}")))?;
    let schema = builder.schema().clone();

    let mut indices: Vec<usize> = Vec::new();
    let mut has_value = false;
    for (i, f) in schema.fields().iter().enumerate() {
        if keep.keep(f) {
            indices.push(i);
            has_value |= keep.is_data(f);
        }
    }
    if !has_value {
        return Ok(None);
    }

    let projected_schema = std::sync::Arc::new(
        schema
            .project(&indices)
            .map_err(|e| Error::Message(format!("failed to project a segment schema: {e}")))?,
    );
    let reader = builder
        .build()
        .map_err(|e| Error::Message(format!("failed to read a segment for projection: {e}")))?;

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(
            &mut buf,
            projected_schema,
            Some(crate::segment::writer_props()),
        )
        .map_err(|e| Error::Message(format!("failed to open a projected segment writer: {e}")))?;
        for batch in reader {
            let batch = batch
                .map_err(|e| Error::Message(format!("failed to read a segment batch: {e}")))?;
            let projected = batch
                .project(&indices)
                .map_err(|e| Error::Message(format!("failed to project a segment batch: {e}")))?;
            writer.write(&projected).map_err(|e| {
                Error::Message(format!("failed to write a projected segment batch: {e}"))
            })?;
        }
        writer
            .close()
            .map_err(|e| Error::Message(format!("failed to finalize a projected segment: {e}")))?;
    }
    Ok(Some(buf))
}

#[cfg(test)]
mod tests {
    use crate::db::Db;

    /// Every table in the schema is either copied by
    /// [`copy_sources_into`] or deliberately not carried, and this
    /// test is what makes that a decision rather than an oversight.
    ///
    /// The weakness of copying instead of deleting is exactly here: a delete
    /// preserves whatever it does not remove, so schema growth is free, while
    /// a copy only carries what it was told to. Adding a table to the schema
    /// without teaching the copy about it would silently drop that table from
    /// every combined, filtered or dumped archive — a data-loss bug with no
    /// error and no symptom until someone queries for what is missing.
    ///
    /// So: adding a table here fails this test. Either copy it in
    /// [`copy_sources_into`] or add it to `NOT_CARRIED` with the reason.
    #[test]
    fn every_schema_table_is_either_copied_or_deliberately_dropped() {
        /// Carried across by [`copy_sources_into`].
        const COPIED: &[&str] = &["sources", "segments", "wal", "clock_offsets"];
        /// Not carried, and correct not to be.
        const NOT_CARRIED: &[&str] = &[
            // Written by `Db::create` for the destination itself; copying
            // the source's would say nothing new and could disagree.
            "schema_version",
        ];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.dendro");
        let db = Db::create(&path).unwrap();

        let mut actual = db.user_table_names().unwrap();
        actual.sort();
        let mut expected: Vec<String> = COPIED
            .iter()
            .chain(NOT_CARRIED)
            .map(|s| s.to_string())
            .collect();
        expected.sort();

        assert_eq!(
            actual, expected,
            "the schema changed. [`copy_sources_into`] copies a fixed set of tables, so a \
             new one is silently dropped from every combined/filtered/dumped archive until it \
             is handled. Copy it, or list it in NOT_CARRIED with the reason."
        );
    }
}
