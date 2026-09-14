//! Errors returned by archive operations.
//!
//! Every fallible operation used to return `Result<_, String>`. That is fine
//! inside one program and costs a library consumer real things: the errors
//! cannot be the `source()` of anything, they cannot be boxed without a
//! wrapper, and — the expensive one — a caller who wants to *behave*
//! differently has to match on message text. "This archive is a legacy schema,
//! offer to upgrade it" and "the disk is full" were the same type and
//! distinguishable only by substring.
//!
//! The enum is `#[non_exhaustive]`: adding variants is not a breaking change, and
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
    UnsupportedSchema {
        /// The `user_version` the file carries.
        found: i64,
        /// The schema version this build writes.
        writes: i64,
        /// The oldest schema version this build reads.
        reads: i64,
    },

    /// The archive says its rows were written by one encoder version and the
    /// caller is reading with another. The bytes are the encoder's, so
    /// nothing else can say whether the two agree; refusing is the only
    /// answer that is never silently wrong.
    EncoderMismatch {
        /// The `sources` row the mismatch was found on.
        source_id: i64,
        /// The encoder version recorded when the source was written.
        wrote: String,
        /// The version the reading encoder reports now.
        reading: String,
    },

    /// A resumed source was handed rows at or before the newest row its
    /// previous writer session left, or a resume anchor at or before it: the
    /// wall clock went backwards across the restart, and writing would make
    /// a timeline that runs backwards or collides with itself.
    TimelineBackwards {
        /// The resumed source.
        source_id: i64,
        /// The timestamp that was refused.
        ts: i64,
        /// The newest row the previous writer session left. Every row this
        /// session commits must be stamped after it.
        floor: i64,
    },

    /// The file is not a dendro archive: not SQLite, another application's
    /// database, or a copy taken from under a writer that carries no catalog.
    /// `what` names the file (or `<bytes>`), `reason` says which.
    NotAnArchive {
        /// The file, or `<bytes>` for an archive opened from memory.
        what: String,
        /// Which of the three it is.
        reason: String,
    },

    /// The caller's [`SegmentEncoder`](crate::segment::SegmentEncoder) failed.
    /// Its own error is preserved rather than stringified, so a caller can
    /// downcast back to it.
    Encoder {
        /// The stream whose rows it was encoding.
        stream: String,
        /// The encoder's own error, downcastable back to its concrete type.
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    /// The encoder returned a segment that does not describe the rows it was
    /// given. An encoder may drop rows; it may not invent coverage.
    EncoderContract {
        /// The stream whose rows it was encoding.
        stream: String,
        /// What the segment claimed against what the rows hold.
        detail: String,
    },

    /// The writer thread failed, and this is what it failed with.
    ///
    /// Shared rather than owned because every handle on the archive reports the
    /// same failure, and the error is not `Clone` — a caller that wants the
    /// cause can match through it or use [`Error::root`].
    Writer(std::sync::Arc<Error>),

    /// The writer thread is gone and left no error. Usually a handle used after
    /// the archive was joined.
    WriterGone,

    /// SQLite returned an error. `context` names the statement that failed; `source`
    /// keeps SQLite's own error, and with it the result code — which is what
    /// lets a caller (the writer thread, mostly) tell a lock that will clear
    /// from a constraint that will not from a corrupt file that is fatal. See
    /// [`Error::is_retryable`] and [`Error::is_constraint`].
    Sqlite {
        /// What was being done, for the message. Empty for a bare conversion.
        context: String,
        /// SQLite's own error, and with it the result code.
        source: rusqlite::Error,
    },

    /// Anything else, with the sentence that described it.
    ///
    /// The long tail is deliberately not enumerated: a variant nobody matches
    /// on is a maintenance cost with no benefit, and the ones above are the
    /// ones a caller was observed to need.
    Message(String),
}

/// Why a handle will not write.
///
/// Variants are added without a major version; match with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
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
            Error::EncoderMismatch {
                source_id,
                wrote,
                reading,
            } => write!(
                f,
                "source {source_id} was written with encoder version {wrote:?} and is being \
                 read with {reading:?}; the rows are the encoder's bytes, so a different \
                 version is not guaranteed to decode them the same way"
            ),
            Error::TimelineBackwards {
                source_id,
                ts,
                floor,
            } => write!(
                f,
                "source {source_id}: timestamp {ts} is not after the newest row its previous \
                 writer session left ({floor}); the clock went backwards across the restart, \
                 and rows would collide or run backwards"
            ),
            Error::NotAnArchive { what, reason } => {
                write!(f, "{what}: not a dendro archive: {reason}")
            }
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
            Error::Sqlite { context, source } if context.is_empty() => write!(f, "{source}"),
            Error::Sqlite { context, source } => write!(f, "{context}: {source}"),
            Error::Message(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Encoder { source, .. } => Some(&**source),
            Error::Writer(e) => Some(&**e),
            Error::Sqlite { source, .. } => Some(source),
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
    fn from(source: rusqlite::Error) -> Self {
        Error::Sqlite {
            context: String::new(),
            source,
        }
    }
}

impl Error {
    /// `map_err` adapter for a SQLite call: `context` names what was being
    /// done, and the result code travels with it.
    pub fn sqlite(context: impl Into<String>) -> impl FnOnce(rusqlite::Error) -> Error {
        let context = context.into();
        move |source| Error::Sqlite { context, source }
    }

    /// SQLite's primary result code, when this error (or the writer failure
    /// it wraps) is SQLite's. `None` for everything else, including a SQLite
    /// error that rusqlite raised without a code.
    pub fn sqlite_code(&self) -> Option<rusqlite::ErrorCode> {
        match self.root() {
            Error::Sqlite {
                source: rusqlite::Error::SqliteFailure(e, _),
                ..
            } => Some(e.code),
            _ => None,
        }
    }

    /// A condition that can clear on its own — another connection's lock, a
    /// full disk, an interrupted call, memory pressure, a schema change under
    /// a prepared statement — so a retry before giving up on the work can
    /// succeed.
    pub fn is_retryable(&self) -> bool {
        use rusqlite::ErrorCode::*;
        matches!(
            self.sqlite_code(),
            Some(
                DatabaseBusy
                    | DatabaseLocked
                    | DiskFull
                    | SystemIoFailure
                    | OutOfMemory
                    | OperationInterrupted
                    | SchemaChanged
            )
        )
    }

    /// A uniqueness or other constraint violation: the row is wrong, the
    /// database is fine. For a tick batched across sources, that means one
    /// source's rows are bad and the others' are not.
    pub fn is_constraint(&self) -> bool {
        matches!(
            self.sqlite_code(),
            Some(rusqlite::ErrorCode::ConstraintViolation)
        )
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    fn sqlite(code: i32) -> Error {
        Error::from(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(code),
            None,
        ))
    }

    /// The writer's whole recovery policy rests on this classification: what
    /// to retry, what to isolate to one source, what to stop on.
    #[test]
    fn classifies_sqlite_result_codes() {
        for code in [
            rusqlite::ffi::SQLITE_BUSY,
            rusqlite::ffi::SQLITE_LOCKED,
            rusqlite::ffi::SQLITE_FULL,
            rusqlite::ffi::SQLITE_IOERR,
            rusqlite::ffi::SQLITE_NOMEM,
            rusqlite::ffi::SQLITE_INTERRUPT,
            rusqlite::ffi::SQLITE_SCHEMA,
        ] {
            let e = sqlite(code);
            assert!(e.is_retryable(), "{code}: {e}");
            assert!(!e.is_constraint(), "{code}: {e}");
        }
        let constraint = sqlite(rusqlite::ffi::SQLITE_CONSTRAINT);
        assert!(constraint.is_constraint() && !constraint.is_retryable());
        // The extended code narrows it and the primary code still classifies.
        let pk = sqlite(rusqlite::ffi::SQLITE_CONSTRAINT_PRIMARYKEY);
        assert!(pk.is_constraint());
        for code in [
            rusqlite::ffi::SQLITE_CORRUPT,
            rusqlite::ffi::SQLITE_READONLY,
            rusqlite::ffi::SQLITE_MISUSE,
            rusqlite::ffi::SQLITE_NOTADB,
        ] {
            let e = sqlite(code);
            assert!(!e.is_retryable() && !e.is_constraint(), "{code}: {e}");
        }
        // Not SQLite's at all: never retried.
        assert!(!Error::Message("bad json".into()).is_retryable());
        assert_eq!(Error::Message("x".into()).sqlite_code(), None);
    }

    /// Context wraps keep the code, and the writer's shared wrapper is looked
    /// through — every handle sees the thread's failure wrapped, and must be
    /// able to classify it without unwrapping by hand.
    #[test]
    fn context_and_writer_wrapping_keep_the_code() {
        let wrapped = Error::sqlite("inserting a row")(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            None,
        ));
        assert!(wrapped.is_retryable());
        assert!(
            wrapped.to_string().starts_with("inserting a row: "),
            "{wrapped}"
        );
        assert!(wrapped.source().is_some(), "the SQLite error is the source");

        let via_writer = Error::Writer(std::sync::Arc::new(wrapped));
        assert!(via_writer.is_retryable());
        assert_eq!(
            via_writer.sqlite_code(),
            Some(rusqlite::ErrorCode::DatabaseBusy)
        );

        // A bare conversion has no context and prints SQLite's message alone.
        let bare = sqlite(rusqlite::ffi::SQLITE_FULL);
        assert!(!bare.to_string().starts_with(": "), "{bare}");
    }
}
