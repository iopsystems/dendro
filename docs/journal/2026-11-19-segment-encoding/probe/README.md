# Probe for the segment-encoding entry

The probe for
[2026-11-19-segment-encoding.md](../../2026-11-19-segment-encoding.md).
It is not part of the crate or its workspace, and CI does not build it.

It depends on metriken-storage and metriken-types at metriken 9320a63,
through the path `../metriken` relative to the dendro checkout. From the
dendro checkout's root, with a metriken clone somewhere, add a worktree
there rather than moving the clone's checkout:

```sh
git -C <your metriken clone> worktree add --detach "$PWD/../metriken" 9320a63
cd docs/journal/2026-11-19-segment-encoding/probe
cargo build --release
```

If `../metriken` is already your metriken clone, use a worktree elsewhere
and change the two `path` entries in `Cargo.toml` to it.

Subcommands:

- `reshape IN OUT TABLE...` rebuilds a wide recording's tables as long
  segments. IN is a `.dendro` or a v3 `.rez`, read through SQLite. It uses
  metriken-storage's `LongTableBuilder` with the archive writer's seal
  properties (`segment_props(default_compression())`, zstd 3), in arrival
  order and sorted. The occupant streams use `occupants::encode_segment`.
- `extract IN OUT TABLE...` takes a recording that is already long.
  Arrival order is the recorded bytes; sorted is the same rows re-encoded
  with the same properties.
- `bench DIR TASK_TABLE [RUNS]` runs the entry's measures over arms P, P2,
  A, Az and Pl, interleaved, for both row orders.

`validate.py WIDE_ARCHIVE TABLE OUT/<table> SEQ...` checks a reshape: every
non-null cell of each named wide segment appears with the same value under
the same occupant labels and timestamp in the long segment, and nothing
else does. It needs pyarrow.

`rack/` holds the load generator (`loadgen.c`) and the systemslab spec that
recorded the two fresh recordings on a rack VM. The spec embeds
`loadgen.c` as base64.
