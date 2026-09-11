# Engineering Journal

One durable, repository-local record per non-trivial effort. Entry frontmatter
is authoritative for lifecycle status.

| Opened | Effort | Status |
| --- | --- | --- |
| 2026-09-11 | [Segment compaction](2026-09-11-segment-compaction.md) | open |
| 2026-09-11 | [Out-of-order appends are accepted and never read](2026-09-11-out-of-order-appends.md) | open |
| 2026-09-11 | [One source's bad tick kills the writer for the whole archive](2026-09-11-writer-failure-blast-radius.md) | open |
| 2026-09-11 | [The encoder boundary does not yet earn the crate's generality claim](2026-09-11-encoder-boundary.md) | open |

Every entry records a gap rather than work in progress: nothing is being built
for any of them. They exist so each limitation is a known one with a stated
reopen condition, instead of something a reader rediscovers — and because the
reason each is hard is a property of the design worth writing down once.

The last three came out of an adversarial review on 2026-09-11 that also found
six defects, which were fixed rather than filed: a read path with no snapshot,
`u64 as i64` at six of seven binding sites, a prune that deleted rows the
encoder had not encoded, `Db::open` silently rewriting the archive, unbounded
`clock_offsets`, and retention that was unreachable through the supported API.
What is left here is what changes a contract rather than a line.

`README.md` names the reader-facing ones under "Known gaps".
