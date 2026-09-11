# Diagrams

Three charts, one visual language.

| chart | the claim it makes |
|---|---|
| [the model](model.svg) | what nests, and what is only a shared key |
| [the write path](write-path.svg) | one row, from a caller's append to a sealed segment |
| [the read path](read-path.svg) | why an archive reads correctly while it is being written |

## Regenerating

```
docs/regen.sh          # needs graphviz; nothing else
```

That emits the `.dot` sources, renders them to SVG, and checks the rendering.
CI runs the same script and fails on any diff, so the committed SVGs cannot
drift from the code.

## How to read them

**Encoding is stated on every chart, in its own key.** A dashed box or an
orange edge means nothing on its own, and a reader should not have to infer it
from context. Where a chart carries a claim its key cannot hold — a negative
claim, like "a stream has no table" — the claim is in the node's own label.

Set-wide, across all three:

- **Shape** is what kind of thing it is. Rounded runs; square holds; a
  segmented glyph is a history you can look back over, and is never used for
  something written once.
- **Dashed outline** means exactly one thing: outside dendro. It is not spent
  on "in memory", or on "has no catalog row" — a channel carrying two meanings
  is a channel carrying none.
- **Edge color** separates sealed bytes from unsealed. Both are Okabe–Ito
  colors, chosen so the two stay distinguishable to a colorblind reader.

## Why these are generated

Every node, edge, and column name is derived from the crate at generation time:
the catalog from `SCHEMA_SQL`, the writer's message kinds from its `Msg` enum,
the watermark from `LIVE_WAL_PREDICATE`. A hand-drawn diagram is a second
source of truth — right the day it is drawn, wrong on the first refactor, and
silent about the divergence, because the picture keeps rendering.

Two things fail the build rather than rendering something stale:

- A **claim that stopped being true** aborts `gen_diagrams`, naming the claim
  and the string that is gone.
- A **catalog table that is neither drawn nor deliberately omitted** aborts
  too. Silent omission is the worst failure available here: a chart missing a
  table still renders as a complete-looking chart.

And two check the rendering rather than the source, because a layout engine
accepts attributes it then ignores:

- Everything drawn stays inside the `viewBox`.
- The key clears every edge it does not own by at least 8pt. Measured, not
  boolean: a key that clears an edge by two pixels is one layout change away
  from crossing it, and a containment check would call that a pass right up
  until it silently became a defect.
