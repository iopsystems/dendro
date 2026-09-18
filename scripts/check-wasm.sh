#!/usr/bin/env bash
#
# The wasm32 check, runnable locally and in CI.
#
# The reader half must build for `wasm32-unknown-unknown`, because
# `std::thread::spawn` compiles for wasm32 and then panics at runtime and
# nothing else catches a thread creeping onto the read path. CI has always run
# this; a macOS checkout could not, because Apple's clang has no WebAssembly
# backend at all:
#
#     error: unable to create target: 'No available targets are compatible
#     with triple "wasm32-unknown-unknown"'
#
# and the failure surfaces from inside cc-rs as a bare exit status, several
# hundred lines into a build of zstd and SQLite, which reads like a broken
# dependency rather than a missing toolchain. So the check was one nobody ran
# before pushing.
#
# Only the C dependencies need anything: zstd and SQLite are compiled from
# source, and both ship their own wasm shims (`wasm-shim/`, `shim/musl/`), so
# no sysroot is involved — the one requirement is a clang that has the
# WebAssembly target registered. Setting `CC_wasm32_unknown_unknown` is enough;
# cc-rs finds the archiver beside the compiler.

set -euo pipefail

cd "$(dirname "$0")/.."

# The compiler to export, or empty for "the one on PATH is already fine".
# A variable rather than a return value because the caller needs to tell those
# two apart from a failure, and a function can hand back only one of the three.
wasm_cc=""

# Does this compiler actually have the WebAssembly backend? Asked by compiling,
# not by parsing `-print-targets`, which Apple's clang does not implement — so
# the cheap probe cannot tell "no wasm" from "no such flag".
has_wasm_target() {
    printf 'int f(void){return 1;}\n' |
        "$1" --target=wasm32-unknown-unknown -c -x c - -o /dev/null 2>/dev/null
}

find_wasm_clang() {
    # An explicit choice wins, but is still probed. Taking it on trust puts a
    # typo back where this script found it: several hundred lines down, inside
    # cc-rs, as `failed to find tool`.
    if [ -n "${CC_wasm32_unknown_unknown:-}" ]; then
        if has_wasm_target "$CC_wasm32_unknown_unknown"; then
            wasm_cc="$CC_wasm32_unknown_unknown"
            return 0
        fi
        echo "CC_wasm32_unknown_unknown is set to '$CC_wasm32_unknown_unknown'," >&2
        echo "which cannot compile for wasm32-unknown-unknown." >&2
        echo "Unset it to search for one, or point it at a clang that can." >&2
        return 1
    fi

    # The Linux and CI case: the clang on PATH can do it, and no override is
    # wanted — setting one would pin CI to a path that happens to exist today.
    if command -v clang >/dev/null 2>&1 && has_wasm_target clang; then
        wasm_cc=""
        return 0
    fi

    # macOS: Homebrew's LLVM has the backend Apple's clang lacks. Both prefixes,
    # because Apple Silicon and Intel put it in different places.
    local candidates=(/opt/homebrew/opt/llvm/bin/clang /usr/local/opt/llvm/bin/clang)
    if command -v brew >/dev/null 2>&1; then
        local prefix
        if prefix=$(brew --prefix llvm 2>/dev/null); then
            candidates=("$prefix/bin/clang" "${candidates[@]}")
        fi
    fi
    local candidate
    for candidate in "${candidates[@]}"; do
        if [ -x "$candidate" ] && has_wasm_target "$candidate"; then
            wasm_cc="$candidate"
            return 0
        fi
    done

    echo "no clang with the WebAssembly target was found." >&2
    echo >&2
    echo "  macOS:  brew install llvm     (Apple's clang cannot target wasm32)" >&2
    echo "  Debian: apt install clang     (a stock clang has the backend)" >&2
    echo >&2
    echo "Or set CC_wasm32_unknown_unknown to one yourself." >&2
    return 1
}

find_wasm_clang

if [ -n "$wasm_cc" ]; then
    export CC_wasm32_unknown_unknown="$wasm_cc"
    echo "wasm32 C compiler: $wasm_cc"
else
    echo "wasm32 C compiler: clang (on PATH, no override needed)"
fi

if ! rustup target list --installed 2>/dev/null | grep -qx wasm32-unknown-unknown; then
    echo "adding the wasm32-unknown-unknown target"
    rustup target add wasm32-unknown-unknown
fi

# The reader build, which is what has to reach wasm32. Replication is part of
# it rather than a feature: publishing is a read, so `ArchivePublisher` and the
# codec build here, and only `Subscriber` is gated on `write` for the thread.
set -- cargo check --no-default-features --target wasm32-unknown-unknown
echo "+ $*"
"$@"
