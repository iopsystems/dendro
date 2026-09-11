#!/usr/bin/env bash
# Regenerate dendro's diagrams.
#
#   docs/regen.sh
#
# Emits the `.dot` sources from the crate's real structures, renders them to
# SVG, and checks the rendering. CI runs this and fails on any diff, so a
# rename the diagrams claim either shows up in the output or breaks the
# generator.
#
# Needs graphviz (`brew install graphviz`, `apt install graphviz`). Nothing
# else: the generator and the checks are plain `cargo run`.
set -euo pipefail

cd "$(dirname "$0")/.."

command -v dot >/dev/null || {
  echo "graphviz is not installed; 'dot' is needed to render the diagrams" >&2
  exit 1
}

cargo run --quiet --example gen_diagrams

for f in model write-path read-path; do
  dot -Tsvg "docs/$f.dot" -o "docs/$f.svg"
done

# Verify the RENDERING, not the source: a layout engine accepts attributes it
# then ignores, so a `.dot` that looks right can still lay out wrong.
cargo run --quiet --example check_diagrams
