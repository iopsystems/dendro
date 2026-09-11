//! Check the rendered diagrams, not their source.
//!
//! A layout engine accepts attributes it then ignores, and a `.dot` file that
//! looks right can render into a picture that is not. These checks run against
//! the SVG graphviz actually produced.
//!
//! Three properties. Two of them a reader notices immediately and no assertion
//! on the `.dot` would catch:
//!
//! * **Bounds.** Every drawn point sits inside the declared `viewBox`.
//! * **The key does not collide.** The legend is placed by the layout engine
//!   into whatever space it finds, and the free region moves whenever an
//!   element is added. A box-only check is not enough here: an edge routed
//!   through otherwise-empty space is invisible to it, and edges are what the
//!   key tends to land on.
//!
//! And one that exists because the geometry is NOT reproducible:
//!
//! * **The committed SVG makes the same claims as its `.dot`.** graphviz 2.43
//!   and 16.0 lay the same graph out differently - different sizes, different
//!   coordinates, even a different SVG preamble - so CI cannot diff the
//!   rendered bytes against a locally rendered copy. It diffs the `.dot`, which
//!   is deterministic because it is just this crate's string output, and then
//!   checks here that the SVG beside it still draws exactly that set of nodes
//!   and edges. That catches the failure a byte-diff was there to catch - a
//!   regenerated `.dot` with a stale SVG next to it - without pinning a
//!   graphviz version nobody else will have.
//!
//! Run by `docs/regen.sh`, which fails the build rather than warning - a
//! warning about a diagram nobody is currently looking at is a warning nobody
//! reads.

use std::collections::BTreeSet;
use std::path::Path;

#[derive(Debug, Clone, Copy)]
struct Rect {
    x0: f64,
    y0: f64,
    x1: f64,
    y1: f64,
}

impl Rect {
    fn of(points: &[(f64, f64)]) -> Option<Rect> {
        let first = *points.first()?;
        let mut r = Rect {
            x0: first.0,
            y0: first.1,
            x1: first.0,
            y1: first.1,
        };
        for &(x, y) in points {
            r.x0 = r.x0.min(x);
            r.y0 = r.y0.min(y);
            r.x1 = r.x1.max(x);
            r.y1 = r.y1.max(y);
        }
        Some(r)
    }

    /// How far outside this rect a point lies; 0 when it is inside.
    ///
    /// Reported rather than thresholded to a boolean, because a key that
    /// clears an edge by two pixels has not really cleared it - it is one
    /// layout change away from crossing, and a binary check would call that
    /// a pass right up until it silently became a defect.
    fn clearance(&self, (x, y): (f64, f64)) -> f64 {
        let dx = (self.x0 - x).max(x - self.x1).max(0.0);
        let dy = (self.y0 - y).max(y - self.y1).max(0.0);
        dx + dy
    }
}

/// Every `x,y` pair in an attribute graphviz writes as a point list.
fn points(attr: &str) -> Vec<(f64, f64)> {
    let mut out = Vec::new();
    for tok in attr.split([' ', ',', 'C', 'M', 'L', '\n']) {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        if let Ok(v) = tok.parse::<f64>() {
            out.push(v);
        }
    }
    out.as_chunks::<2>()
        .0
        .iter()
        .map(|c| (c[0], c[1]))
        .collect()
}

/// Every value of `attr=` on elements of the given tag, within `hay`.
fn attrs<'a>(hay: &'a str, tag: &str, attr: &str) -> Vec<&'a str> {
    let open = format!("<{tag} ");
    let needle = format!("{attr}=\"");
    let mut out = Vec::new();
    let mut rest = hay;
    while let Some(i) = rest.find(&open) {
        rest = &rest[i + open.len()..];
        let end = rest.find('>').unwrap_or(rest.len());
        let el = &rest[..end];
        if let Some(j) = el.find(&needle) {
            let v = &el[j + needle.len()..];
            if let Some(k) = v.find('"') {
                out.push(&v[..k]);
            }
        }
    }
    out
}

/// The `<g>` block for the group whose `<title>` is `title`.
fn group<'a>(svg: &'a str, title: &str) -> Option<&'a str> {
    let marker = format!("<title>{title}</title>");
    let at = svg.find(&marker)?;
    let start = svg[..at].rfind("<g ")?;
    let end = svg[start..].find("</g>")? + start;
    Some(&svg[start..end])
}

/// Node ids and edges declared by a `.dot`, as graphviz will title them.
fn declared(dot: &str) -> (BTreeSet<String>, BTreeSet<String>) {
    let mut nodes = BTreeSet::new();
    let mut edges = BTreeSet::new();
    for line in dot.lines() {
        let line = line.trim();
        if line.starts_with("//") || line.starts_with("--") {
            continue;
        }
        if let Some((lhs, rhs)) = line.split_once("->") {
            let a = lhs.split_whitespace().next_back().unwrap_or("").trim();
            let b = rhs
                .trim_start()
                .split([' ', '[', ';'])
                .next()
                .unwrap_or("")
                .trim();
            if !a.is_empty() && !b.is_empty() {
                edges.insert(format!("{a}->{b}"));
                nodes.insert(a.to_string());
                nodes.insert(b.to_string());
            }
        } else if let Some((id, _)) = line.split_once(" [") {
            let id = id.trim();
            // Attribute defaults (`node [...]`, `edge [...]`) are not nodes.
            if !id.is_empty()
                && id != "node"
                && id != "edge"
                && id != "graph"
                && id.chars().all(|c| c.is_alphanumeric() || c == '_')
            {
                nodes.insert(id.to_string());
            }
        }
    }
    (nodes, edges)
}

/// Node ids and edges the SVG actually draws.
fn rendered(svg: &str) -> (BTreeSet<String>, BTreeSet<String>) {
    let unescape = |s: &str| s.replace("&#45;", "-").replace("&gt;", ">");
    let mut nodes = BTreeSet::new();
    let mut edges = BTreeSet::new();
    for (class, set) in [("node", &mut nodes), ("edge", &mut edges)] {
        let marker = format!("class=\"{class}\"");
        let mut rest = svg;
        while let Some(i) = rest.find(&marker) {
            rest = &rest[i + marker.len()..];
            if let Some(t) = rest
                .split("<title>")
                .nth(1)
                .and_then(|s| s.split("</title>").next())
            {
                set.insert(unescape(t));
            }
        }
    }
    (nodes, edges)
}

fn check(path: &Path) -> Result<(), String> {
    let svg = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let name = path.file_name().unwrap().to_string_lossy();

    // -- bounds ------------------------------------------------------------
    let vb = svg
        .split("viewBox=\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .ok_or("no viewBox")?;
    let v: Vec<f64> = vb
        .split_whitespace()
        .filter_map(|t| t.parse().ok())
        .collect();
    let view = Rect {
        x0: v[0],
        y0: v[1],
        x1: v[0] + v[2],
        y1: v[1] + v[3],
    };
    // graphviz emits a translate on the root group; drawn coordinates are in
    // that translated space, so compare against the viewBox extent alone.
    let drawn: Vec<(f64, f64)> = attrs(&svg, "polygon", "points")
        .iter()
        .chain(attrs(&svg, "path", "d").iter())
        .flat_map(|a| points(a))
        .collect();
    let span = Rect::of(&drawn).ok_or("nothing drawn")?;
    if span.x1 - span.x0 > view.x1 - view.x0 + 1.0 || span.y1 - span.y0 > view.y1 - view.y0 + 1.0 {
        return Err(format!(
            "{name}: drawn extent {span:?} is wider than the viewBox {view:?}"
        ));
    }

    // -- the SVG draws what the .dot declares ------------------------------
    let dot_path = path.with_extension("dot");
    let dot =
        std::fs::read_to_string(&dot_path).map_err(|e| format!("{}: {e}", dot_path.display()))?;
    let (want_nodes, want_edges) = declared(&dot);
    let (got_nodes, got_edges) = rendered(&svg);
    for (what, want, got) in [
        ("node", &want_nodes, &got_nodes),
        ("edge", &want_edges, &got_edges),
    ] {
        let missing: Vec<_> = want.difference(got).cloned().collect();
        let extra: Vec<_> = got.difference(want).cloned().collect();
        if !missing.is_empty() || !extra.is_empty() {
            return Err(format!(
                "{name}: the SVG does not draw what {} declares.\n\
                 missing {what}s: {:?}\n  extra {what}s: {:?}\n\
                 The SVG is stale - run docs/regen.sh and commit the result.",
                dot_path.file_name().unwrap().to_string_lossy(),
                missing,
                extra
            ));
        }
    }

    // -- the key does not collide -----------------------------------------
    let Some(key) = group(&svg, "cluster_key") else {
        return Err(format!("{name}: no cluster_key - the legend is not drawn"));
    };
    let key_poly = attrs(key, "polygon", "points")
        .first()
        .map(|a| points(a))
        .ok_or(format!("{name}: cluster_key has no outline"))?;
    let key_box = Rect::of(&key_poly).ok_or("empty key")?;

    let key_titles: Vec<String> = key
        .split("<title>")
        .skip(1)
        .filter_map(|s| s.split("</title>").next())
        .map(str::to_string)
        .collect();

    /// Minimum gap, in points, between the key and any edge it does not own.
    const MIN_CLEARANCE: f64 = 8.0;

    let mut offenders = Vec::new();
    let mut closest = f64::INFINITY;
    let mut rest = svg.as_str();
    while let Some(i) = rest.find("class=\"edge\"") {
        rest = &rest[i..];
        let end = rest.find("</g>").unwrap_or(rest.len());
        let el = &rest[..end];
        rest = &rest[end..];
        let title = el
            .split("<title>")
            .nth(1)
            .and_then(|s| s.split("</title>").next())
            .unwrap_or("?");
        // The key's own sample edges are allowed inside the key.
        if key_titles.iter().any(|t| t == title) {
            continue;
        }
        let pts: Vec<(f64, f64)> = attrs(el, "path", "d")
            .iter()
            .flat_map(|a| points(a))
            .collect();
        let gap = pts
            .iter()
            .map(|p| key_box.clearance(*p))
            .fold(f64::INFINITY, f64::min);
        if gap < MIN_CLEARANCE {
            offenders.push(format!("{title} ({gap:.0}pt)"));
        }
        closest = closest.min(gap);
    }
    if !offenders.is_empty() {
        return Err(format!(
            "{name}: the key comes within {MIN_CLEARANCE}pt of {} edge(s): {}.\n\
             The legend is placed by the layout engine, so fix this by moving \
             the key in the generator - not by nudging a coordinate.",
            offenders.len(),
            offenders.join(", ")
        ));
    }

    let mut edges = 0;
    let mut r = svg.as_str();
    while let Some(i) = r.find("class=\"edge\"") {
        edges += 1;
        r = &r[i + 12..];
    }
    println!(
        "{name}: {} nodes / {} edges match the .dot; bounds ok; \
         key clears {edges} edges by {closest:.0}pt",
        got_nodes.len(),
        got_edges.len()
    );
    Ok(())
}

fn main() {
    let docs = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs");
    let mut failed = false;
    for name in ["model.svg", "write-path.svg", "read-path.svg"] {
        if let Err(e) = check(&docs.join(name)) {
            eprintln!("FAIL {e}");
            failed = true;
        }
    }
    if failed {
        std::process::exit(1);
    }
}
