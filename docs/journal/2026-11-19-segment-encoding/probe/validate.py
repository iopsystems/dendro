"""Check a reshape: every non-null cell of a wide segment appears, with the
same value, in the long segment the probe built from it, under the same
occupant labels and timestamp, and the long segment holds nothing else.

usage: python3 validate.py WIDE_ARCHIVE TABLE RESHAPED_TABLE_DIR SEQ...

WIDE_ARCHIVE is the `.dendro` or v3 `.rez` the probe's `reshape` read;
RESHAPED_TABLE_DIR is OUT/<table with / as __>. Needs pyarrow.
"""

import glob
import io
import sqlite3
import sys

import pyarrow.parquet as pq

FIXED = {"timestamp", ":wall_offset", ":window_begin", ":window_width", "occupant"}


def wide_segment(path, table, seq):
    db = sqlite3.connect(f"file:{path}?immutable=1", uri=True)
    rez = db.execute("select count(*) from sqlite_master where name='recordings'").fetchone()[0]
    key = "sampler" if rez else "stream"
    row = db.execute(f"select bytes from segments where {key}=? and seq=?", (table, seq)).fetchone()
    return pq.read_table(io.BytesIO(row[0]))


def main():
    archive, table, outdir, seqs = sys.argv[1], sys.argv[2], sys.argv[3], [int(s) for s in sys.argv[4:]]
    labels = {}
    occ_keys = set()
    for f in sorted(glob.glob(f"{outdir}/occ/*.parquet")):
        table_ = pq.read_table(f)
        t = table_.to_pydict()
        keys = [k for k in t if k not in ("timestamp", "occupant")]
        occ_keys.update(keys)
        # A label stored as an integer says how it was written:
        # `label_encoding` is "decimal" or "hex16" (16 lowercase hex digits).
        enc = {}
        for fld in table_.schema:
            md = {k.decode(): v.decode() for k, v in (fld.metadata or {}).items()}
            enc[fld.name] = md.get("label_encoding")

        def text(k, v):
            return format(v, "016x") if enc.get(k) == "hex16" else str(v)

        for i, o in enumerate(t["occupant"]):
            labels[o] = tuple(sorted((k, text(k, t[k][i])) for k in keys if t[k][i] is not None))
    ok = True
    for seq in seqs:
        w = wide_segment(archive, table, seq)
        l = pq.read_table(f"{outdir}/arr/{seq:06d}.parquet")
        ld = l.to_pydict()
        metrics = [c for c in l.column_names if c not in FIXED]
        long_cells = {}
        for m in metrics:
            for o, ts, v in zip(ld["occupant"], ld["timestamp"], ld[m]):
                if v is not None:
                    long_cells[(m, labels[o], ts)] = v
        ts = w.column("timestamp").to_pylist()
        compared = mismatched = 0
        for f in w.schema:
            if f.name in FIXED:
                continue
            md = {k.decode(): v.decode() for k, v in f.metadata.items()}
            occ = tuple(sorted((k, md[k]) for k in occ_keys if k in md))
            extra = sorted(
                (k, v)
                for k, v in md.items()
                if k not in occ_keys and k not in ("metric", "metric_type", "unit", "sampler", "source", "endpoint")
            )
            name = md.get("metric", f.name)
            if extra:
                name += "{" + ",".join(f"{k}={v}" for k, v in extra) + "}"
            for t, v in zip(ts, w.column(f.name).to_pylist()):
                if v is None:
                    continue
                compared += 1
                if long_cells.get((name, occ, t)) != v:
                    mismatched += 1
        extra_cells = len(long_cells) - compared
        good = mismatched == 0 and extra_cells == 0
        ok &= good
        print(f"{table} seq {seq}: wide cells {compared}, mismatched {mismatched}, "
              f"long cells not in wide {extra_cells}: {'OK' if good else 'FAIL'}")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
