#[cfg(feature = "ssl")]
use rustls::pki_types::InvalidDnsNameError;
use std::fmt::{Display, Formatter};
use std::io::Error as IoError;
use std::{convert, error};

pub type IgniteResult<T> = Result<T, IgniteError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    Other,
    Connection,
    Handshake,
    Authentication,
    Server,
    Tls,
    /// Server response `SECURITY_VIOLATION (1012)`. Java maps this to
    /// `ClientAuthorizationException`; see `TcpClientChannel.java:579-581@2.17.0`.
    Authorization,
    /// Server response `ENTRY_PROCESSOR_EXCEPTION (1040)` from `CACHE_INVOKE` /
    /// `CACHE_INVOKE_ALL`. Java throws `javax.cache.processor.EntryProcessorException`;
    /// see `TcpClientCache.java:922-923, 953-954@2.17.0`.
    EntryProcessorException,
}

#[derive(Debug, Clone)]
pub struct IgniteError {
    pub(crate) desc: String,
    pub(crate) kind: ErrorKind,
    /// Server status code when `kind` came from a failed response frame
    /// (FLAG_ERROR bit set). See Java `ClientStatus.java@2.17.0`. `None`
    /// for errors that did not originate as a server response (I/O, TLS,
    /// client-side logic).
    pub(crate) server_status: Option<i32>,
}

impl IgniteError {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Server status code if this error was produced from a server response
    /// `FLAG_ERROR` frame. `None` for I/O, TLS, handshake, or client-side
    /// errors. Mirrors Java `ClientServerError.getCode()`.
    pub fn server_status(&self) -> Option<i32> {
        self.server_status
    }

    pub(crate) fn new(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Other,
            server_status: None,
        }
    }

    pub(crate) fn connection(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Connection,
            server_status: None,
        }
    }

    pub(crate) fn handshake(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Handshake,
            server_status: None,
        }
    }

    pub(crate) fn authentication(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Authentication,
            server_status: None,
        }
    }

    #[allow(dead_code)]
    pub(crate) fn server(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Server,
            server_status: None,
        }
    }

    /// Construct an error from a server response status code plus message.
    /// Maps special-cased codes to dedicated `ErrorKind` variants per
    /// Java §11:
    /// - `SECURITY_VIOLATION (1012)` → `ErrorKind::Authorization`
    /// - `ENTRY_PROCESSOR_EXCEPTION (1040)` → `ErrorKind::EntryProcessorException`
    /// - all other non-zero statuses → `ErrorKind::Server`
    pub(crate) fn from_server_status(status: i32, desc: impl Into<String>) -> Self {
        const SECURITY_VIOLATION: i32 = 1012;
        const ENTRY_PROCESSOR_EXCEPTION: i32 = 1040;
        let kind = match status {
            SECURITY_VIOLATION => ErrorKind::Authorization,
            ENTRY_PROCESSOR_EXCEPTION => ErrorKind::EntryProcessorException,
            _ => ErrorKind::Server,
        };
        Self {
            desc: desc.into(),
            kind,
            server_status: Some(status),
        }
    }

    pub(crate) fn tls(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Tls,
            server_status: None,
        }
    }

    pub(crate) fn is_connection_related(&self) -> bool {
        if matches!(self.kind, ErrorKind::Connection | ErrorKind::Tls) {
            return true;
        }

        // Errors carrying a server response status code are terminal by
        // definition (Java maps them to `ClientServerError` which is never
        // retried — see `ReliableChannelImpl.java:974-993@2.17.0`).
        if self.server_status.is_some() {
            return false;
        }

        let desc = self.desc.to_ascii_lowercase();
        [
            "connection",
            "channel is closed",
            "closed",
            "broken pipe",
            "reset",
            "timed out",
            "timeout",
            "refused",
            "unavailable",
            "transport",
            "network",
            "eof",
        ]
        .iter()
        .any(|needle| desc.contains(needle))
    }
}

impl error::Error for IgniteError {}

impl Display for IgniteError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.desc)
    }
}

impl convert::From<IoError> for IgniteError {
    fn from(e: IoError) -> Self {
        IgniteError::new(e.to_string())
    }
}

impl convert::From<&str> for IgniteError {
    fn from(desc: &str) -> Self {
        IgniteError::new(String::from(desc))
    }
}

impl convert::From<Option<String>> for IgniteError {
    fn from(desc: Option<String>) -> Self {
        match desc {
            Some(desc) => IgniteError::new(desc),
            None => IgniteError::new("Ignite client error! No description provided"),
        }
    }
}

#[cfg(feature = "ssl")]
impl convert::From<InvalidDnsNameError> for IgniteError {
    fn from(err: InvalidDnsNameError) -> Self {
        IgniteError::tls(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{ErrorKind, IgniteError};

    /// FND-058: server status `SECURITY_VIOLATION (1012)` must surface as a
    /// distinct `ErrorKind::Authorization`, matching Java's
    /// `ClientAuthorizationException` mapping at
    /// `TcpClientChannel.java:579-581@2.17.0`.
    #[test]
    fn server_status_security_violation_maps_to_authorization() {
        let err = IgniteError::from_server_status(1012, "access denied");
        assert_eq!(err.kind(), ErrorKind::Authorization);
        assert_eq!(err.server_status(), Some(1012));
    }

    /// FND-058 / FND-060: `ENTRY_PROCESSOR_EXCEPTION (1040)` must surface
    /// distinctly so invoke callers can rethrow as
    /// `javax.cache.processor.EntryProcessorException` equivalent.
    /// See `TcpClientCache.java:922-923@2.17.0`.
    #[test]
    fn server_status_entry_processor_exception_maps_to_distinct_kind() {
        let err = IgniteError::from_server_status(1040, "processor blew up");
        assert_eq!(err.kind(), ErrorKind::EntryProcessorException);
        assert_eq!(err.server_status(), Some(1040));
    }

    /// FND-058: all other non-zero status codes map to `ErrorKind::Server`
    /// and carry the status through `server_status()`. The caller can still
    /// discriminate e.g. `CACHE_DOES_NOT_EXIST (1000)`, `TX_NOT_FOUND (1021)`,
    /// `RESOURCE_DOES_NOT_EXIST (1011)` via the code.
    #[test]
    fn server_status_other_codes_preserved_on_server_kind() {
        for code in [1, 2, 10, 1000, 1001, 1011, 1020, 1021] {
            let err = IgniteError::from_server_status(code, "boom");
            assert_eq!(err.kind(), ErrorKind::Server, "status {code}");
            assert_eq!(err.server_status(), Some(code), "status {code}");
        }
    }

    /// FND-061: any error carrying a server status must NOT classify as
    /// connection-related. Java only retries `ClientConnectionException`;
    /// `ClientServerError` is terminal (`ReliableChannelImpl.java:974-993@2.17.0`).
    #[test]
    fn server_status_errors_are_not_connection_related() {
        // Include a code whose message would otherwise match the substring
        // heuristic (e.g. message containing "reset" or "connection").
        for code in [1, 1000, 1011, 1012, 1040] {
            let err = IgniteError::from_server_status(code, "connection reset by peer");
            assert!(
                !err.is_connection_related(),
                "server status {code} must not be retried like a connection error"
            );
        }
    }

    /// Non-server errors are unaffected: an `ErrorKind::Connection` with no
    /// status is still connection-related (retryable by `RetryPolicy::Default`).
    #[test]
    fn connection_kind_remains_retryable() {
        let err = IgniteError::connection("connection refused");
        assert!(err.is_connection_related());
        assert_eq!(err.server_status(), None);
    }
}
