//! What can go wrong, as something a caller can branch on.
//!
//! Every fallible operation used to return `Result<_, String>`. That is fine
//! inside one program and costs a library consumer real things: the errors
//! cannot be the `source()` of anything, they cannot be boxed without a
//! wrapper, and — the expensive one — a caller who wants to *behave*
//! differently has to match on message text. "This archive is a legacy schema,
//! offer to upgrade it" and "the disk is full" were the same type and
//! distinguishable only by substring.
//!
//! The enum is `#[non_exhaustive]`: new variants are not a breaking change, and
//! a caller that matches must keep a `_` arm.

use std::fmt;
use std::path::PathBuf;

/// This crate's result type.
pub type Result<T> = std::result::Result<T, Error>;

/// An error from an archive operation.
#[non_exhaustive]
#[derive(Debug)]
pub enum Error {
    /// The archive exists already. `create` refuses rather than appending or
    /// truncating, so a caller that wants the next free name can act on this
    /// without parsing the OS's wording.
    AlreadyExists(PathBuf),

    /// The handle cannot write, and this says which kind of cannot.
    ReadOnly(ReadOnly),

    /// The file's schema is one this build does not know. Carries the version
    /// found, so a caller can tell "too new, upgrade dendro" from "too old".
    UnsupportedSchema { found: i64, writes: i64, reads: i64 },

    /// The caller's [`SegmentEncoder`](crate::segment::SegmentEncoder) failed.
    /// Its own error is preserved rather than stringified, so a caller can
    /// downcast back to it.
    Encoder {
        stream: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The encoder returned a segment that does not describe the rows it was
    /// given. An encoder may drop rows; it may not invent coverage.
    EncoderContract { stream: String, detail: String },

    /// The writer thread failed, and this is what it failed with.
    ///
    /// Shared rather than owned because every handle on the archive reports the
    /// same failure, and the error is not `Clone` — a caller that wants the
    /// cause can match through it or use [`Error::root`].
    Writer(std::sync::Arc<Error>),

    /// The writer thread is gone and left no error. Usually a handle used after
    /// the archive was joined.
    WriterGone,

    /// SQLite said no.
    Sqlite(rusqlite::Error),

    /// Anything else, with the sentence that described it.
    ///
    /// The long tail is deliberately not enumerated: a variant nobody matches
    /// on is a maintenance cost with no benefit, and the ones above are the
    /// ones a caller was observed to need.
    Message(String),
}

/// Why a handle will not write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOnly {
    /// Opened with [`Db::open_read_only`](crate::db::Db::open_read_only).
    Handle,
    /// A legacy-schema archive, which is read through compatibility views.
    /// Writing would mean migrating it in place, which this crate does not do.
    LegacySchema,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::AlreadyExists(p) => {
                write!(f, "{} already exists", p.display())
            }
            Error::ReadOnly(ReadOnly::Handle) => write!(
                f,
                "this archive was opened read-only; reopen it with `Db::open` to modify it"
            ),
            Error::ReadOnly(ReadOnly::LegacySchema) => write!(
                f,
                "this archive uses a legacy schema, which is readable but not \
                 writable by this build; copy it forward first"
            ),
            Error::UnsupportedSchema {
                found,
                writes,
                reads,
            } => write!(
                f,
                "unsupported archive schema version {found}: this build writes \
                 v{writes} and reads v{reads}"
            ),
            Error::Encoder { stream, source } => {
                write!(f, "failed to encode a {stream} segment: {source}")
            }
            Error::EncoderContract { stream, detail } => write!(
                f,
                "the encoder returned a segment for {stream} that does not \
                 describe the rows it was given: {detail}"
            ),
            Error::Writer(e) => write!(f, "{e}"),
            Error::WriterGone => write!(
                f,
                "the archive writer thread exited before the source finished"
            ),
            Error::Sqlite(e) => write!(f, "{e}"),
            Error::Message(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Encoder { source, .. } => Some(&**source),
            Error::Writer(e) => Some(&**e),
            Error::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

/// So the long tail of `format!`-built messages keeps working: `?` applies
/// `From`, so an existing `.map_err(|e| format!(...))?` converts on its own.
impl From<String> for Error {
    fn from(m: String) -> Self {
        Error::Message(m)
    }
}

impl From<&str> for Error {
    fn from(m: &str) -> Self {
        Error::Message(m.to_string())
    }
}

impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Sqlite(e)
    }
}

impl Error {
    /// The underlying failure, looking through [`Error::Writer`].
    ///
    /// A failure on the writer thread reaches every handle wrapped, so matching
    /// on the variant a caller cares about would otherwise mean unwrapping by
    /// hand at each site.
    pub fn root(&self) -> &Error {
        match self {
            Error::Writer(inner) => inner.root(),
            other => other,
        }
    }
}

/// Kept so a caller that was matching on text, or storing errors as strings,
/// is not broken by the change.
impl From<Error> for String {
    fn from(e: Error) -> String {
        e.to_string()
    }
}
