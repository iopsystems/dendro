# Probe for the segment-encoding entry

The probe for
[2026-11-19-segment-encoding.md](../../2026-11-19-segment-encoding.md).
It is not part of the crate or its workspace, and CI does not build it.

It depends on metriken-storage and metriken-types from a metriken checkout
at 9320a63, beside the dendro checkout:

```sh
git -C .. clone https://github.com/iopsystems/metriken   # if not there
git -C ../metriken checkout 9320a63
cd docs/journal/2026-11-19-segment-encoding/probe
cargo build --release
```

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

`rack/` holds the load generator (`loadgen.c`) and the systemslab spec that
recorded the two fresh recordings on a rack VM. The spec embeds
`loadgen.c` as base64.
