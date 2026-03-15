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
}

#[derive(Debug, Clone)]
pub struct IgniteError {
    pub(crate) desc: String,
    pub(crate) kind: ErrorKind,
}

impl IgniteError {
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub(crate) fn new(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Other,
        }
    }

    pub(crate) fn connection(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Connection,
        }
    }

    pub(crate) fn handshake(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Handshake,
        }
    }

    pub(crate) fn authentication(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Authentication,
        }
    }

    pub(crate) fn server(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Server,
        }
    }

    pub(crate) fn tls(desc: impl Into<String>) -> Self {
        Self {
            desc: desc.into(),
            kind: ErrorKind::Tls,
        }
    }

    pub(crate) fn is_connection_related(&self) -> bool {
        if matches!(self.kind, ErrorKind::Connection | ErrorKind::Tls) {
            return true;
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
