# Probe for the single-file log container entry

The GO/NO-GO probe for
[2026-11-18-single-file-log-container.md](../2026-11-18-single-file-log-container.md).
It is not part of the crate or its workspace, and CI does not build it.

It needs two things dendro does not export: the writer thread's own open,
`ArchiveMut::open_for_write`, and a way to set `fullfsync`. `dendro.patch`
adds both. Apply it to a scratch checkout at a7000b5, not to a branch:

```sh
git worktree add --detach /tmp/dendro-probe a7000b5
cd /tmp/dendro-probe
git apply docs/journal/2026-11-18-single-file-log-container/dendro.patch
cd docs/journal/2026-11-18-single-file-log-container
cargo build --release
```

Run one of the two criteria at a time, in a directory on the filesystem
under test:

- **G1, per-tick commit:**
  `target/release/logprobe g1 --dir D [--trials 15] [--warm 200] [--ticks 1000] [--arms sqlite_a,sqlite_b,log_a,log_b,log_prealloc,log_extsize] [--full]`
- **G2, rolling buffer:**
  `target/release/logprobe g2 --dir D [--ws-mib 256] [--mult 20] [--seal-every 1000] [--seal-mib 4] [--prealloc-mib N] [--extsize-kib N] [--no-sync]`

Environment variables for G2:

- `PROBE_KEEP=1` keeps the file at the end.
- `PROBE_CORRUPT=1` overwrites 16 bytes of a live record. This is the
  mutation check that the reader's violation count can go red.

`--extsize-kib` works on xfs and Linux only.
