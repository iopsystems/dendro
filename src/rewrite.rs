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

use crate::archive::{Archive, ArchiveMut, SegmentMeta, Transaction};
use crate::error::{Error, Result};
use crate::segment::SegmentEncoder;

use parquet::file::metadata::KeyValue;

/// What a merge does when two segments of one stream do not have the same
/// schema — which a stream is allowed to do, and which a caller whose
/// columns come and go does constantly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchemaPolicy {
    /// Stop the run at the change. The default, and the only choice that is
    /// safe without knowing what a column means.
    #[default]
    StopAtChange,
    /// Merge on the **union** of the fields: a column present in only some of
    /// the segments is written, and null for the rows of the segments that
    /// lacked it.
    ///
    /// What this asserts, on the caller's behalf: that a column absent from a
    /// segment means *no reading*, not *a different thing*. That is true of a
    /// population that comes and goes — a cgroup that did not exist yet — and
    /// it is why this is opt-in rather than the default.
    ///
    /// **It does not merge across a changed field.** A name that appears in
    /// two segments with any difference at all — type, nullability, or
    /// metadata — still stops the run, because a column whose metadata
    /// changed can be a different series under the same name, and fusing
    /// two series into one column is silent corruption rather than a policy
    /// choice. A caller whose column identity lives in field metadata and
    /// churns gets no more merging from this than from the default; the fix
    /// there is to take identity out of the column, not to merge harder.
    UnionFields,
}

/// What one compaction pass aims for. See [`compact`].
pub struct CompactSpec {
    /// Merge adjacent segments until the next one would push the total past
    /// this. A run of one is left alone.
    ///
    /// It counts catalog rows ([`SegmentMeta::rows`]), which are WAL rows. A
    /// stream whose encoder writes several parquet rows per WAL row gets a
    /// merged segment larger than this by that factor: a long layout writes
    /// one per occupant, and `to_rows(1_000_000)` over one measured long
    /// stream of 34,678 WAL rows built a single segment of 76,182,021
    /// parquet rows, in memory, with a peak resident set of about 2.1 GB.
    pub target_rows: u64,
    /// Properties for the merged segment. `None` uses the archive's own
    /// ([`segment::writer_props`](crate::segment::writer_props)); a caller
    /// whose encoder writes with other settings must pass them, or its
    /// compacted segments come back encoded differently from its sealed
    /// ones.
    pub writer_props: Option<parquet::file::properties::WriterProperties>,
    /// What to do when adjacent segments disagree about their schema. See
    /// [`SchemaPolicy`].
    pub schema: SchemaPolicy,
}

impl CompactSpec {
    /// Merge toward segments of `target_rows`, with the archive's own writer
    /// properties, stopping a run at any schema change.
    pub fn to_rows(target_rows: u64) -> Self {
        CompactSpec {
            target_rows,
            writer_props: None,
            schema: SchemaPolicy::StopAtChange,
        }
    }

    /// This spec, merging across segments whose column SETS differ. See
    /// [`SchemaPolicy::UnionFields`] for what that asserts.
    pub fn unioning_fields(mut self) -> Self {
        self.schema = SchemaPolicy::UnionFields;
        self
    }

    fn props(&self) -> parquet::file::properties::WriterProperties {
        self.writer_props
            .clone()
            .unwrap_or_else(crate::segment::writer_props)
    }
}

/// One segment of a compaction run, as a [`MetadataMerge`] sees it.
#[derive(Debug, Clone, Copy)]
/// Fields are added without a major version; dendro constructs these during
/// compaction, and a caller can construct one with [`MergeInput::new`].
#[non_exhaustive]
pub struct MergeInput<'a> {
    /// The segment's parquet file key-value metadata, in file order, without
    /// Arrow's schema entry (`ARROW:schema`).
    pub key_value: &'a [KeyValue],
    /// The segment's columns, as Arrow reads them. A caller whose metadata
    /// names columns needs these to tell a column the segment lacks from one
    /// its metadata says nothing about.
    pub fields: &'a arrow::datatypes::Fields,
    /// The segment's parquet row count, from its footer. For a stream whose
    /// encoder writes several parquet rows per WAL row (a long layout, one
    /// per occupant) this is larger than the catalog's
    /// [`SegmentMeta::rows`], which counts WAL rows.
    pub rows: u64,
}

impl<'a> MergeInput<'a> {
    /// A segment as a merge sees it, for testing a [`MetadataMerge`]
    /// without an archive. dendro builds these itself during compaction.
    pub fn new(key_value: &'a [KeyValue], fields: &'a arrow::datatypes::Fields, rows: u64) -> Self {
        MergeInput {
            key_value,
            fields,
            rows,
        }
    }
}

/// How a compaction run's file key-value metadata combines: a fold over the
/// run's segments in order, [`start`](Self::start) with the first,
/// [`extend`](Self::extend) with each one after it, and
/// [`finish`](Self::finish) once, for the segments merged.
///
/// Each segment is given to the merge once, so a merge that keeps what it
/// has decoded in [`Run`](Self::Run) costs time linear in the run. dendro
/// gives the merge a segment only after the [`SchemaPolicy`] has admitted
/// it, so the run `finish` describes is the run that is written.
///
/// [`IdenticalMetadata`] is the merge [`compact`] and [`compact_stream`] use.
pub trait MetadataMerge {
    /// What the merge keeps about the run so far.
    type Run;

    /// Begin a run at `first`, or `None` when no run can begin there; the
    /// segment is then left as it is.
    fn start(&self, first: &MergeInput<'_>) -> Option<Self::Run>;

    /// Add `next` to the run and return `true`, or return `false` to end the
    /// run before `next`. After `false`, `run` must still describe the run
    /// without `next`, because that run is merged and finished: decide
    /// before changing it.
    fn extend(&self, run: &mut Self::Run, next: &MergeInput<'_>) -> bool;

    /// The merged segment's file key-value metadata. An `ARROW:schema`
    /// entry in it is ignored; the writer writes the merged schema's own.
    fn finish(&self, run: Self::Run) -> Vec<KeyValue>;
}

/// The [`MetadataMerge`] [`compact`] and [`compact_stream`] use: the run's
/// segments merge only while every one has the first's key-value metadata,
/// the same entries in any order, and the merged segment carries the first's,
/// in its order.
///
/// It is the only rule that is safe without knowing what the metadata means.
/// Order is not part of it because parquet gives it no meaning, and a writer
/// that lists the same entries in another order is describing the same
/// segment.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdenticalMetadata;

/// An [`IdenticalMetadata`] run: the first segment's entries.
#[derive(Debug, Clone)]
pub struct IdenticalMetadataRun {
    /// In file order, which the merged segment is written with.
    first: Vec<KeyValue>,
    /// Sorted, for comparing.
    sorted: Vec<KeyValue>,
}

impl MetadataMerge for IdenticalMetadata {
    type Run = IdenticalMetadataRun;

    fn start(&self, first: &MergeInput<'_>) -> Option<Self::Run> {
        Some(IdenticalMetadataRun {
            first: first.key_value.to_vec(),
            sorted: sorted(first.key_value),
        })
    }

    fn extend(&self, run: &mut Self::Run, next: &MergeInput<'_>) -> bool {
        next.key_value.len() == run.sorted.len() && sorted(next.key_value) == run.sorted
    }

    fn finish(&self, run: Self::Run) -> Vec<KeyValue> {
        run.first
    }
}

/// `kv` sorted by key, then value, so two lists of the same entries compare
/// equal whatever their order.
fn sorted(kv: &[KeyValue]) -> Vec<KeyValue> {
    let mut kv = kv.to_vec();
    kv.sort_by(|a, b| (&a.key, &a.value).cmp(&(&b.key, &b.value)));
    kv
}

/// What one compaction pass did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
/// Fields are added without a major version; construct one only by
/// asking dendro for it, and match with a wildcard arm.
#[non_exhaustive]
pub struct Compacted {
    /// Sealed segments before the pass, across everything it looked at.
    pub before: usize,
    /// And after. The difference is what a reader stops paying for.
    pub after: usize,
    /// How many merges were performed. Zero is a no-op, and the normal
    /// answer for an archive that is already coarse.
    pub merges: usize,
}

/// Merge a stream's small adjacent segments into larger ones, in place.
///
/// Read cost is linear in segment count, measured at 674 µs plus
/// 29 µs per segment on a 50-column stream, an 18.2× difference between 400
/// segments and one, with the archive 2.38× larger as well (see
/// `docs/journal/2026-09-11-segment-compaction.md`). Segments are sized when
/// they are sealed, by a policy that is trading against finalize latency and
/// kill-loss, and an archive that is kept rather than rolled has no way to
/// revisit that trade. This is where it is revisited.
///
/// **What it does not touch.** The live WAL tail, which has no segment yet.
/// Segments whose schemas differ: a run stops at a schema change, because
/// dendro concatenates parquet rather than reconciling it, and a stream's
/// schema may drift. Segments whose file key-value metadata differs, for the
/// same reason: the merged segment carries its inputs' metadata, which is
/// possible only when they agree on all of it, so a run also stops where any
/// key's value, or the set of keys, changes ([`IdenticalMetadata`]; a caller
/// that can combine its metadata says how with [`compact_stream_with`]). And
/// any run of one.
/// The caller's time-keyed store ([`CallerRow`](crate::archive::CallerRow))
/// is keyed by time, not by segment, so a merge leaves it exactly as it was;
/// that is the property it exists for.
///
/// **The caller's index is dropped** on a merged segment, for the same
/// reason a column projection drops it: the index described one of the
/// inputs, dendro cannot combine two of them without knowing what they mean,
/// and a wrong index is worse than none.
///
/// **This does not shrink the file.** The segments it replaces are deleted,
/// and SQLite keeps their pages on the free list rather than returning them,
/// so the archive reads faster and occupies exactly what it did. [`compact`]
/// reclaims at the end; a caller driving streams individually must finish
/// with [`ArchiveMut::incremental_vacuum`](crate::archive::ArchiveMut::incremental_vacuum).
///
/// **Concurrency.** In place, on this connection, so the archive must have no
/// other writer — the same single-writer rule everything else here obeys. A
/// reader on another connection is unaffected: each merge is one transaction,
/// and until it commits a reader sees the segments exactly as they were.
pub fn compact_stream(
    db: &mut ArchiveMut,
    source_id: i64,
    stream: &str,
    spec: &CompactSpec,
) -> Result<Compacted> {
    compact_stream_with(db, source_id, stream, spec, &IdenticalMetadata)
}

/// [`compact_stream`], with the caller deciding what the merged segment's
/// file key-value metadata is, and so where a run must stop.
///
/// `merge` is given the run's segments in order, each once, as a fold
/// ([`MetadataMerge`]). The run ends before the segment the merge refuses,
/// or before the first segment the [`SchemaPolicy`] refuses, whichever comes
/// first, and the merged segment carries what [`MetadataMerge::finish`]
/// returns for the segments merged. [`IdenticalMetadata`] is the merge
/// [`compact_stream`] uses.
///
/// Use it when the metadata can be combined, such as a list of what the
/// rows refer to, which unions across segments. Under [`IdenticalMetadata`]
/// a run stops wherever such a list changes.
pub fn compact_stream_with<M: MetadataMerge + ?Sized>(
    db: &mut ArchiveMut,
    source_id: i64,
    stream: &str,
    spec: &CompactSpec,
    merge: &M,
) -> Result<Compacted> {
    let metas = db.read_segment_meta(source_id, stream)?;
    let mut done = Compacted {
        before: metas.len(),
        after: metas.len(),
        merges: 0,
    };

    let mut at = 0usize;
    while at < metas.len() {
        // Plan a run: adjacent segments whose rows fit the target.
        let mut end = at;
        let mut rows = 0u64;
        while end < metas.len() && rows + metas[end].1.rows <= spec.target_rows.max(1) {
            rows += metas[end].1.rows;
            end += 1;
        }
        if end - at < 2 {
            // Nothing to gain here — one segment already at or over target.
            at = (at + 1).max(end);
            continue;
        }

        // Read and re-encode BEFORE the transaction opens, as `seal_batch`
        // does: both are proportional to segment size and would otherwise
        // hold the write lock for their whole duration.
        let seqs: Vec<u64> = metas[at..end].iter().map(|(seq, _)| *seq).collect();
        let fetch = |seq: u64| db.read_segment_bytes(source_id, stream, seq);
        let (merged, consumed) =
            concat_parquet(&seqs, &fetch, spec.props(), spec.schema, merge, stream)?;
        if consumed < 2 {
            // A schema or metadata change at the very front of the run.
            at += 1;
            continue;
        }
        let run = &metas[at..at + consumed];
        // The catalog counts the WAL rows a segment covers, which is not
        // always its parquet row count: a caller's encoder may write several
        // parquet rows per WAL row (a long layout writes one per occupant).
        let meta = SegmentMeta {
            rows: run.iter().map(|(_, m)| m.rows).sum(),
            first_ts: run[0].1.first_ts,
            last_ts: run[consumed - 1].1.last_ts,
        };
        // ONE transaction. Delete then insert is safe only in here: between
        // them the stream's watermark dips, and a reader that saw the gap
        // would be handed sealed rows again as a live tail. Other
        // connections see the whole thing or none of it.
        let seq = run[0].0;
        db.transaction(|tx| {
            for (s, _) in run {
                tx.delete_segment(source_id, stream, *s)?;
            }
            tx.insert_segment(source_id, stream, seq, &meta, &merged)
        })?;

        done.merges += 1;
        done.after -= consumed - 1;
        at += consumed;
    }
    Ok(done)
}

/// [`compact_stream`] over every stream of every source, then hand the freed
/// pages back to the filesystem.
///
/// **The reclaim is the difference between this and calling
/// `compact_stream` in a loop, and it is not a detail.** Merging deletes the
/// segments it replaced, and SQLite does not return deleted pages to the
/// filesystem — they go on the free list, to be reused. So compaction on its
/// own makes an archive faster to read and exactly as large as it was:
/// measured at 400 segments merged to one, read 12.10 ms to 643 µs, file
/// 9.92 MB to 9.92 MB. Since half of what compaction is for is size, the
/// whole-archive entry point finishes the job.
///
/// Uncapped, unlike the writer's reclaim, which is bounded because it runs on
/// the append path. There is no append path here: compaction already
/// requires that nothing else is writing.
pub fn compact(db: &mut ArchiveMut, spec: &CompactSpec) -> Result<Compacted> {
    compact_with(db, spec, &IdenticalMetadata)
}

/// [`compact`], with `merge` deciding the merged segments' file key-value
/// metadata; see [`compact_stream_with`]. It is used for every stream of
/// every source.
pub fn compact_with<M: MetadataMerge + ?Sized>(
    db: &mut ArchiveMut,
    spec: &CompactSpec,
    merge: &M,
) -> Result<Compacted> {
    let mut total = Compacted::default();
    let sources = db.read_sources()?;
    for src in sources {
        for stream in db.all_streams(src.id)? {
            let one = compact_stream_with(db, src.id, &stream, spec, merge)?;
            total.before += one.before;
            total.after += one.after;
            total.merges += one.merges;
        }
    }
    if total.merges > 0 {
        db.incremental_vacuum(u32::MAX)?;
    }
    Ok(total)
}

/// Concatenate the leading run of the segments `seqs` names that the schema
/// policy and `merge` both admit into one parquet file, which carries the
/// metadata `merge` finishes for that run. `fetch` reads a segment's bytes,
/// `None` if it is gone. Returns the bytes and how many segments were
/// consumed — fewer than all of them when the schema or the
/// metadata changes partway, which a stream is allowed to do. A consumed
/// count under two means nothing was written.
///
/// This is dendro looking inside a segment, which it otherwise does only for
/// a column projection. The compaction entry weighed the alternative — a
/// second method on the encoder trait, implemented by every caller — and
/// chose this: concatenation is a property of the container, not of what a
/// row means. The cost is that compaction, like projection, requires
/// segments to actually be parquet.
fn concat_parquet<M: MetadataMerge + ?Sized>(
    seqs: &[u64],
    fetch: &dyn Fn(u64) -> Result<Option<Vec<u8>>>,
    props: parquet::file::properties::WriterProperties,
    policy: SchemaPolicy,
    merge: &M,
    stream: &str,
) -> Result<(Vec<u8>, usize)> {
    use arrow::array::new_null_array;
    use arrow::datatypes::{Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    // How far the run reaches, decided one segment at a time: first the
    // schema policy, then `merge`, so `merge` is only ever given segments
    // that will be written and its run is exactly the merged one.
    //
    // Under either policy a field that appears twice must be IDENTICAL —
    // same type, same nullability, same metadata. The policies differ only
    // on a field that appears in one segment and not another: stop there, or
    // carry it and null-fill.
    //
    // The file key-value metadata is the caller's description of the
    // segment (a layout marker, a list of what its rows refer to), so the
    // merged segment carries it, and only `merge` knows whether two
    // segments' descriptions combine and into what. Arrow's schema entry is
    // not the caller's and is left out; the writer writes the merged
    // schema's own.
    //
    // The schema Arrow reads carries the file metadata as well, merged into
    // what the writer embedded under `ARROW:schema`, with a file entry
    // winning for a key both carry (parquet's
    // `parquet_to_arrow_schema_and_fields`). Those entries are `merge`'s to
    // judge, so they come out of the schema here; what is left is the
    // embedded entries whose keys the file metadata does not also carry.
    //
    // A segment is read and opened only when it is about to be judged, so a
    // run refused at its second segment reads two segments, not the whole
    // planned run, and each one is opened once: the reader that judged it
    // is the reader that copies its rows.
    let mut opened: Vec<ParquetRecordBatchReaderBuilder<bytes::Bytes>> = Vec::new();
    let mut run: Option<M::Run> = None;
    // `StopAtChange`: the run's schema, the first segment's.
    let mut first: Option<SchemaRef> = None;
    // `UnionFields`: the fields in the order they first appear, where each
    // is in that order, and how many segments carry it.
    let mut order: Vec<Field> = Vec::new();
    let mut seen: BTreeMap<String, usize> = BTreeMap::new();
    let mut carried_by: Vec<usize> = Vec::new();
    for seq in seqs {
        // Gone, which the single-writer rule says cannot happen; end the run
        // here rather than guess.
        let Some(blob) = fetch(*seq)? else {
            break;
        };
        #[cfg(feature = "test-support")]
        OPENS.with(|n| n.set(n.get() + 1));
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(blob)).map_err(|e| {
                Error::Message(format!("failed to open a {stream} segment to merge: {e}"))
            })?;
        let kv = file_kv(builder.metadata());
        let rows = builder.metadata().file_metadata().num_rows().max(0) as u64;
        let read = builder.schema();
        let mut embedded = read.metadata().clone();
        embedded.retain(|k, _| !kv.iter().any(|e| &e.key == k));
        let schema = Arc::new(Schema::new_with_metadata(read.fields().clone(), embedded));

        let admitted = match policy {
            SchemaPolicy::StopAtChange => first.as_ref().is_none_or(|f| *f == schema),
            // Check the whole segment before taking any of it, so a
            // conflict late in its field list does not leave the union
            // half-extended.
            SchemaPolicy::UnionFields => schema
                .fields()
                .iter()
                .all(|f| seen.get(f.name()).is_none_or(|i| &order[*i] == f.as_ref())),
        };
        if !admitted {
            break;
        }
        let input = MergeInput {
            key_value: &kv,
            fields: schema.fields(),
            rows,
        };
        match run.as_mut() {
            None => match merge.start(&input) {
                Some(r) => run = Some(r),
                None => break,
            },
            Some(r) => {
                if !merge.extend(r, &input) {
                    break;
                }
            }
        }

        match policy {
            SchemaPolicy::StopAtChange => {
                first.get_or_insert(schema);
            }
            SchemaPolicy::UnionFields => {
                for f in schema.fields() {
                    match seen.get(f.name()) {
                        Some(i) => carried_by[*i] += 1,
                        None => {
                            seen.insert(f.name().clone(), order.len());
                            order.push(f.as_ref().clone());
                            carried_by.push(1);
                        }
                    }
                }
            }
        }
        opened.push(builder);
    }
    let consumed = opened.len();
    let Some(run) = run.filter(|_| consumed >= 2) else {
        return Ok((Vec::new(), 1));
    };

    let schema = match first {
        Some(schema) => schema,
        None => {
            // A field the whole run carries keeps its nullability; one only
            // some segments have must become nullable, because the rest
            // contribute nulls for it.
            let fields: Vec<Field> = order
                .into_iter()
                .zip(carried_by)
                .map(|(f, n)| {
                    if n == consumed {
                        f
                    } else {
                        let md = f.metadata().clone();
                        Field::new(f.name(), f.data_type().clone(), true).with_metadata(md)
                    }
                })
                .collect();
            Arc::new(Schema::new(fields))
        }
    };
    let props = with_file_metadata(props, merge.finish(run));

    let mut buf: Vec<u8> = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props))
            .map_err(|e| Error::Message(format!("failed to open a merged {stream} writer: {e}")))?;
        for builder in opened {
            for batch in builder.build().map_err(|e| {
                Error::Message(format!("failed to read a {stream} segment to merge: {e}"))
            })? {
                let batch = batch.map_err(|e| {
                    Error::Message(format!("failed to read a {stream} batch to merge: {e}"))
                })?;
                // Under `StopAtChange` every batch already has the merged
                // schema and this is a move; under `UnionFields` a segment
                // that lacked a column contributes nulls for it.
                let batch = if batch.schema().fields() == schema.fields() {
                    batch
                } else {
                    let n = batch.num_rows();
                    let columns = schema
                        .fields()
                        .iter()
                        .map(|f| match batch.schema().index_of(f.name()) {
                            Ok(i) => batch.column(i).clone(),
                            Err(_) => new_null_array(f.data_type(), n),
                        })
                        .collect();
                    RecordBatch::try_new(schema.clone(), columns).map_err(|e| {
                        Error::Message(format!("failed to widen a {stream} batch: {e}"))
                    })?
                };
                writer.write(&batch).map_err(|e| {
                    Error::Message(format!("failed to write a merged {stream} batch: {e}"))
                })?;
            }
        }
        writer.close().map_err(|e| {
            Error::Message(format!("failed to finish a merged {stream} segment: {e}"))
        })?;
    }
    Ok((buf, consumed))
}

#[cfg(feature = "test-support")]
thread_local! {
    static OPENS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many segments compaction has opened on this thread, for a test that
/// bounds how much of a stream a compaction reads. Per thread, so tests
/// running in parallel do not count each other's opens.
#[cfg(feature = "test-support")]
pub fn compaction_opens() -> usize {
    OPENS.with(|n| n.get())
}

/// `props` with `carried` as file key-value metadata. A carried entry
/// replaces the properties' entry for the same key: `carried` describes the
/// rows being written, the properties only how they are written. Arrow's
/// schema entry is dropped from `carried`; the writer writes the new
/// schema's own.
fn with_file_metadata(
    props: parquet::file::properties::WriterProperties,
    carried: Vec<KeyValue>,
) -> parquet::file::properties::WriterProperties {
    let carried: Vec<KeyValue> = carried
        .into_iter()
        .filter(|e| e.key != "ARROW:schema")
        .collect();
    if carried.is_empty() {
        return props;
    }
    let mut kv = props.key_value_metadata().cloned().unwrap_or_default();
    kv.retain(|e| !carried.iter().any(|c| c.key == e.key));
    kv.extend(carried);
    props
        .into_builder()
        .set_key_value_metadata(Some(kv))
        .build()
}

/// A segment's file key-value metadata, without Arrow's schema entry, in
/// file order.
fn file_kv(meta: &parquet::file::metadata::ParquetMetaData) -> Vec<KeyValue> {
    meta.file_metadata()
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .filter(|e| e.key != "ARROW:schema")
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Which columns of a segment survive a [`project_segment_columns`] pass.
///
/// dendro does not know what a column is for, so both halves of the decision
/// are here. They are separate questions: a segment keeps its structural
/// columns (timestamps and whatever sidecars the caller's row shape needs)
/// unconditionally, but a segment left holding only those carries no data and
/// is dropped rather than written empty.
pub trait ColumnFilter {
    /// Keep this column in the projected segment?
    fn keep(&self, field: &arrow::datatypes::Field) -> bool;
    /// Does this column carry data, as opposed to being structural?
    fn is_data(&self, field: &arrow::datatypes::Field) -> bool;
    /// Project this stream's segments at all? A stream this returns `false`
    /// for is copied as it is, byte for byte, like a copy with no
    /// `keep_columns`.
    ///
    /// For a stream whose columns mean something other than the data
    /// streams' do, which one field-by-field filter cannot tell apart: a
    /// caller whose table has a companion stream (a list of what the table's
    /// rows refer to, say) keeps that stream whole while trimming the table.
    /// Every stream is projected by default.
    fn projects(&self, stream: &str) -> bool {
        let _ = stream;
        true
    }
}

/// What one copy pass carries across.
pub struct CopySpec<'a> {
    /// Row-timestamp bound, inclusive, in the unit of the row timestamps.
    /// [`everything`](Self::everything) sets it to `i64::MIN`; a ranged copy
    /// narrows it to the window of interest.
    pub start: i64,
    /// The other end of that bound, inclusive. A segment is carried whole
    /// when it overlaps `[start, end]` at all, so a copy holds a little more
    /// than it asked for at each edge.
    pub end: i64,
    /// Keep only the streams this accepts; `None` keeps every stream.
    ///
    /// A predicate rather than a name set because a caller can group streams
    /// under a coarser unit than the stream key — one an operator names, that
    /// owns several streams — and dropping that unit has to drop all of them.
    pub keep_streams: Option<&'a dyn Fn(&str) -> bool>,
    /// Extra metadata merged into each copied source's own, overwriting on
    /// key collision. A caller that stamps results of its own into a copy
    /// uses this; `None` leaves the metadata as it was.
    pub metadata_extra: Option<&'a BTreeMap<String, String>>,
    /// When set, project each copied segment's parquet down to the columns
    /// this accepts, decoding and re-encoding it. `None` is the fast path —
    /// segment BLOBs pass through byte-identical. This is the only copy that
    /// touches segment bytes; see [`project_segment_columns`]. A stream left
    /// with no data column is dropped.
    pub keep_columns: Option<&'a dyn ColumnFilter>,
    /// Writer properties for a projected segment (`keep_columns`), which is
    /// re-encoded. `None` uses the archive's own
    /// ([`segment::writer_props`](crate::segment::writer_props): LZ4, no
    /// dictionary); a caller whose encoder writes with other settings must
    /// pass them, or its projected segments come back encoded differently
    /// from its sealed ones.
    pub writer_props: Option<parquet::file::properties::WriterProperties>,
}

impl CopySpec<'_> {
    /// The writer properties a projection re-encodes with.
    fn props(&self) -> parquet::file::properties::WriterProperties {
        self.writer_props
            .clone()
            .unwrap_or_else(crate::segment::writer_props)
    }

    /// Every source, every table, every row, metadata untouched.
    ///
    /// `i64::MIN`, not `0`: a timestamp is signed, so zero is the epoch rather
    /// than the bottom of the range. `start: 0` meant "everything since 1970"
    /// while claiming to mean everything, and silently produced an empty
    /// destination — reporting success — for any archive holding pre-epoch
    /// rows.
    pub fn everything() -> Self {
        CopySpec {
            start: i64::MIN,
            end: i64::MAX,
            keep_streams: None,
            metadata_extra: None,
            keep_columns: None,
            writer_props: None,
        }
    }
}

/// The UUIDs of sources present in both archives — the same source, not two
/// sources with the same labels. A caller assembling several archives into
/// one asks this before copying: the same file given twice, or a copy
/// alongside its original, would otherwise land twice and double every
/// value it holds. Sources without a uuid (archives from before the column)
/// are never reported; they are not known to be the same.
pub fn shared_sources(a: &Archive, b: &Archive) -> Result<Vec<String>> {
    let in_a: std::collections::BTreeSet<String> = a
        .read_sources()?
        .into_iter()
        .filter_map(|s| s.uuid)
        .collect();
    Ok(b.read_sources()?
        .into_iter()
        .filter_map(|s| s.uuid)
        .filter(|u| in_a.contains(u))
        .collect())
}

/// Copy every source in `src` into the open destination transaction,
/// returning how many sources were copied.
///
/// The destination transaction is the caller's so that `combine` can fold
/// several sources into one atomic write: either the combined archive has all
/// of its inputs or it does not exist.
///
/// Each copied source keeps its source's `complete` flag. That flag answers
/// whether data after the last row can be missing, which is a property of the DATA
/// and survives being copied — a source recovered from a checkpoint rather
/// than cleanly finalized is still truncated after a combine or a filter, and
/// claiming otherwise would hide the loss. Missing beats wrong.
///
/// The one caller that overrides it is a ranged dump, which marks its copy
/// complete afterwards for a specific reason: the buffer it copied is
/// perpetually mid-source and would otherwise never produce a snapshot that
/// did not warn.
///
/// The archive's documents come too, every revision of every one, whatever
/// the spec's time bound and stream filter: a document belongs to the
/// archive rather than to a source or a time, so no part of the spec
/// selects it. See [`copy_documents_into`], which this calls in the same
/// snapshot, for how revisions already in the destination are handled.
pub fn copy_sources_into(
    src: &Archive,
    tx: &Transaction<'_>,
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
    src: &Archive,
    tx: &Transaction<'_>,
    spec: &CopySpec<'_>,
    encoder: &dyn SegmentEncoder,
) -> Result<usize> {
    let sources = src.read_sources()?;
    let mut copied = 0usize;
    for rec in &sources {
        // The tail is re-encoded on the way across; a different encoder
        // would write a segment the source's earlier ones do not match.
        crate::segment::check_encoder(rec.id, &rec.meta.metadata, encoder)?;
        let mut meta = rec.meta.clone();
        if let Some(extra) = spec.metadata_extra {
            for (k, v) in extra {
                meta.metadata.insert(k.clone(), v.clone());
            }
        }
        // The copy IS the source — same identity, so a later assembly can
        // tell "this again" from "another one with the same labels".
        let id = tx.insert_source_with_uuid(&meta, rec.uuid.as_deref())?;
        if rec.complete {
            tx.mark_complete(id)?;
        }
        // Carried as it stands: no writer bumps the copy's heartbeat, so a
        // reader of the copy sees it stop changing and reads the source as
        // stopped, which a copy of a running source is.
        if rec.heartbeat.is_some() || rec.heartbeat_interval_ns.is_some() {
            tx.set_heartbeat(id, rec.heartbeat, rec.heartbeat_interval_ns)?;
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
            let keep_columns = spec.keep_columns.filter(|k| k.projects(&table));
            for segment in src.segments_overlapping(rec.id, &table, spec.start, spec.end)? {
                match keep_columns {
                    // Column trim re-encodes; a stream with none of the kept
                    // columns projects to no data column and is dropped (its
                    // segments never inserted). Row count, timestamps
                    // and windows are unchanged by a projection, so the
                    // segment's own `meta` is reused verbatim.
                    Some(keep) => {
                        if let Some(projected) =
                            project_segment_columns(&segment.bytes, keep, spec.props())?
                        {
                            // A projection drops columns, so an index built
                            // over the originals may describe columns the
                            // copy no longer has. Dropped rather than
                            // carried: a wrong index is worse than none, and
                            // only the caller can rebuild it.
                            tx.insert_segment(id, &table, seq, &segment.meta, &projected)?;
                            seq += 1;
                        }
                    }
                    None => {
                        tx.insert_segment_with_index(
                            id,
                            &table,
                            seq,
                            &segment.meta,
                            &segment.bytes,
                            segment.caller_index.as_deref(),
                        )?;
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
            // else: every catalog fact below comes from what actually
            // materializes. Cataloging the raw tail's span would claim rows
            // the bytes do not contain — at either end.
            let materialized = crate::segment::materialize(encoder, &table, &tail)?;
            if let Some(materialized) = materialized {
                let meta = SegmentMeta {
                    rows: materialized.rows,
                    first_ts: materialized.first_ts,
                    // From the SEGMENT, like the other two. Taking it from the
                    // input catalogs a row the bytes do not contain.
                    last_ts: materialized.last_ts,
                };
                match keep_columns {
                    Some(keep) => {
                        if let Some(projected) =
                            project_segment_columns(&materialized.bytes, keep, spec.props())?
                        {
                            tx.insert_segment(id, &table, seq, &meta, &projected)?;
                        }
                    }
                    None => {
                        tx.insert_segment_with_index(
                            id,
                            &table,
                            seq,
                            &meta,
                            &materialized.bytes,
                            materialized.index.as_deref(),
                        )?;
                    }
                }
            }
        }

        // Drift observations are part of the source's identity and cost
        // nothing to carry; they are already only a handful of rows per seal.
        for (ts, offset) in src.read_clock_offsets(rec.id)? {
            tx.insert_clock_offset(id, ts, offset)?;
        }

        // The caller's time-keyed store, verbatim and within the range. By
        // its own names rather than the streams', because a series kept
        // under a name no stream uses is still the caller's to keep; the
        // stream filter applies to those names the same way.
        for name in src.caller_row_streams(rec.id)? {
            if let Some(keep) = spec.keep_streams {
                if !keep(name.as_str()) {
                    continue;
                }
            }
            let rows = src.read_caller_rows(rec.id, &name, spec.start, spec.end)?;
            tx.insert_caller_rows(id, &name, &rows)?;
        }
    }
    tx.copy_documents_from(src)?;
    Ok(copied)
}

/// Copy every document revision in `src` into the open destination
/// transaction, returning how many were inserted. FORMAT.md §3.6.
///
/// [`copy_sources_into`] calls this, so a caller copying sources need not.
/// It is public for an assembly that builds its sources some other way and
/// still wants the documents.
///
/// A revision is identified by its uuid, and one the destination already
/// holds is skipped, so copying the same archive twice, or a copy beside its
/// original, adds each revision once. `supersedes` is renumbered to the
/// destination's ids. Inserted revisions take ids after everything the
/// destination holds, in `src`'s order, so where two inputs both hold
/// revisions of one document, the input copied last supplies the current
/// one and the other's stay in its history.
pub fn copy_documents_into(src: &Archive, tx: &Transaction<'_>) -> Result<usize> {
    src.read_snapshot(|src| tx.copy_documents_from(src))
}

/// Re-encode one segment with only the columns `keep` accepts, or `None` when
/// nothing but structural columns survive.
///
/// This is the only operation in this module that touches segment bytes; every
/// other copy passes the parquet BLOB through verbatim. Row count, timestamps
/// and column values are unchanged by a projection, so a projected segment
/// reuses its source's catalog entry as-is.
///
/// The filter is the caller's because the structural columns are: dendro knows
/// a segment has columns, not which of them a reader cannot do without. An
/// implementation that drops a column its own reader needs to place rows in
/// time will produce a segment that opens and answers wrongly, so
/// [`ColumnFilter::keep`] must accept those unconditionally.
pub fn project_segment_columns(
    bytes: &[u8],
    keep: &dyn ColumnFilter,
    props: parquet::file::properties::WriterProperties,
) -> Result<Option<Vec<u8>>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::arrow::ArrowWriter;

    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::copy_from_slice(bytes))
        .map_err(|e| Error::Message(format!("failed to open a segment for projection: {e}")))?;
    let schema = builder.schema().clone();
    // The file's own key-value metadata travels with the projection: it is
    // the caller's description of the segment (a layout marker, a list of
    // what its rows refer to), and dropping it can make the copy read as a
    // different kind of segment. Arrow's schema entry is left out; the
    // writer writes the projected one.
    let carried: Vec<parquet::file::metadata::KeyValue> = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .map(|kv| {
            kv.iter()
                .filter(|e| e.key != "ARROW:schema")
                .cloned()
                .collect()
        })
        .unwrap_or_default();
    let props = with_file_metadata(props, carried);

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
        let mut writer =
            ArrowWriter::try_new(&mut buf, projected_schema, Some(props)).map_err(|e| {
                Error::Message(format!("failed to open a projected segment writer: {e}"))
            })?;
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

    /// A projection carries the source file's own key-value metadata, so a
    /// segment the caller marked (a layout, a list of what its rows refer
    /// to) is still marked after columns are dropped.
    #[test]
    fn a_projection_keeps_the_file_metadata() {
        use arrow::array::{ArrayRef, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        use parquet::arrow::ArrowWriter;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        struct KeepA;
        impl super::ColumnFilter for KeepA {
            fn keep(&self, f: &Field) -> bool {
                f.name() != "b"
            }
            fn is_data(&self, f: &Field) -> bool {
                f.name() == "a"
            }
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
                Arc::new(Int64Array::from(vec![3, 4])) as ArrayRef,
                Arc::new(Int64Array::from(vec![5, 6])) as ArrayRef,
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new(
                "layout".to_string(),
                "long".to_string(),
            )]))
            .build();
        let mut bytes = Vec::new();
        let mut w = ArrowWriter::try_new(&mut bytes, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let projected =
            super::project_segment_columns(&bytes, &KeepA, crate::segment::writer_props())
                .unwrap()
                .unwrap();
        let builder =
            ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(projected)).unwrap();
        let kv = builder
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .unwrap();
        assert!(kv
            .iter()
            .any(|e| e.key == "layout" && e.value.as_deref() == Some("long")));
        assert_eq!(
            kv.iter().filter(|e| e.key == "ARROW:schema").count(),
            1,
            "one arrow schema, the projected one"
        );
        let fields: Vec<String> = builder
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, vec!["timestamp", "a"]);
    }

    use crate::archive::ArchiveMut;

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
        const COPIED: &[&str] = &[
            "sources",
            "segments",
            "wal",
            "clock_offsets",
            "caller_rows",
            "documents",
        ];
        /// Not carried, and correct not to be. Empty today: the schema version
        /// lives in the header stamp, not in a table.
        const NOT_CARRIED: &[&str] = &[];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("schema.dendro");
        let db = ArchiveMut::create(&path).unwrap();

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
