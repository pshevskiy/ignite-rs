use crate::api::OpCode;
use crate::error::{IgniteError, IgniteResult};
use crate::exec::TokioExec;
use crate::protocol::{read_i64, write_i32, write_i64, write_null, write_u8};
use crate::{ReadableReq, ReadableType, WritableType, WriteableReq};
use std::io::{self, Read, Write};

/// Flags for data streamer operations, matching `ClientDataStreamerFlags.java`.
const FLAG_ALLOW_OVERWRITE: u8 = 0x01;
const FLAG_SKIP_STORE: u8 = 0x02;
const FLAG_KEEP_BINARY: u8 = 0x04;
const FLAG_FLUSH: u8 = 0x08;
const FLAG_CLOSE: u8 = 0x10;

/// Configuration for a data streamer.
#[derive(Clone, Debug)]
pub struct DataStreamerConfig {
    pub allow_overwrite: bool,
    pub skip_store: bool,
    pub per_node_buffer_size: i32,
    pub per_thread_buffer_size: i32,
}

impl Default for DataStreamerConfig {
    fn default() -> Self {
        Self {
            allow_overwrite: true,
            skip_store: false,
            per_node_buffer_size: -1,
            per_thread_buffer_size: -1,
        }
    }
}

impl DataStreamerConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_allow_overwrite(mut self, allow: bool) -> Self {
        self.allow_overwrite = allow;
        self
    }

    pub fn with_skip_store(mut self, skip: bool) -> Self {
        self.skip_store = skip;
        self
    }

    pub fn with_per_node_buffer_size(mut self, size: i32) -> Self {
        self.per_node_buffer_size = size;
        self
    }

    pub fn with_per_thread_buffer_size(mut self, size: i32) -> Self {
        self.per_thread_buffer_size = size;
        self
    }

    fn flags(&self, flush: bool, close: bool) -> u8 {
        let mut flags = FLAG_KEEP_BINARY;
        if self.allow_overwrite {
            flags |= FLAG_ALLOW_OVERWRITE;
        }
        if self.skip_store {
            flags |= FLAG_SKIP_STORE;
        }
        if flush {
            flags |= FLAG_FLUSH;
        }
        if close {
            flags |= FLAG_CLOSE;
        }
        flags
    }
}

/// A high-throughput data streamer for bulk loading data into an Ignite cache.
///
/// The streamer opens a server-side resource and can send multiple batches of
/// key-value entries before closing.
pub struct DataStreamer<K: WritableType + ReadableType, V: WritableType + ReadableType> {
    exec: TokioExec,
    cache_id: i32,
    config: DataStreamerConfig,
    resource_id: Option<i64>,
    closed: bool,
    _k: std::marker::PhantomData<K>,
    _v: std::marker::PhantomData<V>,
}

impl<K: WritableType + ReadableType, V: WritableType + ReadableType> DataStreamer<K, V> {
    pub(crate) fn new(exec: TokioExec, cache_id: i32, config: DataStreamerConfig) -> Self {
        Self {
            exec,
            cache_id,
            config,
            resource_id: None,
            closed: false,
            _k: std::marker::PhantomData,
            _v: std::marker::PhantomData,
        }
    }

    /// Stream a batch of key-value entries. If this is the first call, the streamer
    /// resource is created on the server. Subsequent calls add data to the open streamer.
    pub async fn add_data(&mut self, entries: &[(K, V)]) -> IgniteResult<()> {
        if self.closed {
            return Err(IgniteError::from("Data streamer is already closed"));
        }

        match self.resource_id {
            None => {
                // First batch — use START operation
                let resp: StreamerStartResponse = self
                    .exec
                    .send_and_read(
                        OpCode::DataStreamerStart,
                        StreamerStartRequest {
                            cache_id: self.cache_id,
                            flags: self.config.flags(false, false),
                            per_node_buffer_size: self.config.per_node_buffer_size,
                            per_thread_buffer_size: self.config.per_thread_buffer_size,
                            entries,
                        },
                    )
                    .await?;
                self.resource_id = Some(resp.resource_id);
                Ok(())
            }
            Some(resource_id) => {
                // Subsequent batches — use ADD_DATA operation
                self.exec
                    .send(
                        OpCode::DataStreamerAddData,
                        StreamerAddDataRequest {
                            resource_id,
                            flags: self.config.flags(false, false)
                                & !(FLAG_ALLOW_OVERWRITE | FLAG_SKIP_STORE | FLAG_KEEP_BINARY),
                            entries,
                        },
                    )
                    .await
            }
        }
    }

    /// Flush buffered data on the server without closing the streamer.
    pub async fn flush(&mut self) -> IgniteResult<()> {
        if self.closed {
            return Err(IgniteError::from("Data streamer is already closed"));
        }

        match self.resource_id {
            None => Ok(()), // Nothing to flush
            Some(resource_id) => {
                let empty: &[(K, V)] = &[];
                self.exec
                    .send(
                        OpCode::DataStreamerAddData,
                        StreamerAddDataRequest {
                            resource_id,
                            flags: FLAG_FLUSH,
                            entries: empty,
                        },
                    )
                    .await
            }
        }
    }

    /// Close the streamer, flushing any remaining data.
    pub async fn close(&mut self) -> IgniteResult<()> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;

        match self.resource_id.take() {
            None => Ok(()),
            Some(resource_id) => {
                let empty: &[(K, V)] = &[];
                self.exec
                    .send(
                        OpCode::DataStreamerAddData,
                        StreamerAddDataRequest {
                            resource_id,
                            flags: FLAG_CLOSE,
                            entries: empty,
                        },
                    )
                    .await
            }
        }
    }

    /// One-shot: stream entries and immediately close. This avoids keeping a
    /// server-side resource open — all data is processed in a single request.
    pub async fn add_data_and_close(mut self, entries: &[(K, V)]) -> IgniteResult<()> {
        if self.closed {
            return Err(IgniteError::from("Data streamer is already closed"));
        }
        self.closed = true;

        match self.resource_id.take() {
            None => {
                // One-shot: START with CLOSE flag
                let _resp: StreamerStartResponse = self
                    .exec
                    .send_and_read(
                        OpCode::DataStreamerStart,
                        StreamerStartRequest {
                            cache_id: self.cache_id,
                            flags: self.config.flags(false, true),
                            per_node_buffer_size: self.config.per_node_buffer_size,
                            per_thread_buffer_size: self.config.per_thread_buffer_size,
                            entries,
                        },
                    )
                    .await?;
                Ok(())
            }
            Some(resource_id) => {
                self.exec
                    .send(
                        OpCode::DataStreamerAddData,
                        StreamerAddDataRequest {
                            resource_id,
                            flags: FLAG_CLOSE,
                            entries,
                        },
                    )
                    .await
            }
        }
    }
}

impl<K: WritableType + ReadableType, V: WritableType + ReadableType> Drop for DataStreamer<K, V> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        if let Some(resource_id) = self.resource_id.take() {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let exec = self.exec.clone();
                handle.spawn(async move {
                    let empty: &[(i32, i32)] = &[];
                    let _ = exec
                        .send(
                            OpCode::DataStreamerAddData,
                            StreamerAddDataRequest {
                                resource_id,
                                flags: FLAG_CLOSE,
                                entries: empty,
                            },
                        )
                        .await;
                });
            }
        }
    }
}

// --- Wire format structs ---

struct StreamerStartRequest<'a, K: WritableType, V: WritableType> {
    cache_id: i32,
    flags: u8,
    per_node_buffer_size: i32,
    per_thread_buffer_size: i32,
    entries: &'a [(K, V)],
}

impl<'a, K: WritableType, V: WritableType> WriteableReq for StreamerStartRequest<'a, K, V> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i32(writer, self.cache_id)?;
        write_u8(writer, self.flags)?;
        write_i32(writer, self.per_node_buffer_size)?;
        write_i32(writer, self.per_thread_buffer_size)?;
        write_null(writer)?; // receiver object (null — no custom receiver)
        write_i32(writer, self.entries.len() as i32)?;
        for (key, value) in self.entries {
            key.write(writer)?;
            value.write(writer)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        4 // cache_id
        + 1 // flags
        + 4 // per_node_buffer_size
        + 4 // per_thread_buffer_size
        + 1 // null receiver
        + 4 // entry count
        + self.entries.iter().map(|(k, v)| k.size() + v.size()).sum::<usize>()
    }
}

struct StreamerAddDataRequest<'a, K: WritableType, V: WritableType> {
    resource_id: i64,
    flags: u8,
    entries: &'a [(K, V)],
}

impl<'a, K: WritableType, V: WritableType> WriteableReq for StreamerAddDataRequest<'a, K, V> {
    fn write(&self, writer: &mut dyn Write) -> io::Result<()> {
        write_i64(writer, self.resource_id)?;
        write_u8(writer, self.flags)?;
        write_i32(writer, self.entries.len() as i32)?;
        for (key, value) in self.entries {
            key.write(writer)?;
            value.write(writer)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        8 // resource_id
        + 1 // flags
        + 4 // entry count
        + self.entries.iter().map(|(k, v)| k.size() + v.size()).sum::<usize>()
    }
}

struct StreamerStartResponse {
    resource_id: i64,
}

impl ReadableReq for StreamerStartResponse {
    fn read(reader: &mut impl Read) -> IgniteResult<Self> {
        let resource_id = read_i64(reader).map_err(IgniteError::from)?;
        Ok(Self { resource_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_encode_streamer_start_request_with_entries() {
        let entries = vec![(1i32, 100i32), (2, 200)];
        let req = StreamerStartRequest {
            cache_id: 42,
            flags: FLAG_ALLOW_OVERWRITE | FLAG_KEEP_BINARY,
            per_node_buffer_size: -1,
            per_thread_buffer_size: -1,
            entries: &entries,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());

        // Verify cache_id
        assert_eq!(&buf[0..4], 42i32.to_le_bytes().as_slice());
        // Verify flags
        assert_eq!(buf[4], FLAG_ALLOW_OVERWRITE | FLAG_KEEP_BINARY);
    }

    #[test]
    fn should_encode_streamer_start_request_one_shot() {
        let entries = vec![(1i32, 100i32)];
        let req = StreamerStartRequest {
            cache_id: 7,
            flags: FLAG_ALLOW_OVERWRITE | FLAG_KEEP_BINARY | FLAG_CLOSE,
            per_node_buffer_size: 512,
            per_thread_buffer_size: 256,
            entries: &entries,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
        assert_eq!(buf[4], FLAG_ALLOW_OVERWRITE | FLAG_KEEP_BINARY | FLAG_CLOSE);
    }

    #[test]
    fn should_encode_add_data_request() {
        let entries = vec![(10i32, 20i32)];
        let req = StreamerAddDataRequest {
            resource_id: 99,
            flags: 0,
            entries: &entries,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
        assert_eq!(&buf[0..8], 99i64.to_le_bytes().as_slice());
    }

    #[test]
    fn should_encode_add_data_with_close_flag() {
        let empty: &[(i32, i32)] = &[];
        let req = StreamerAddDataRequest {
            resource_id: 42,
            flags: FLAG_CLOSE,
            entries: empty,
        };

        let mut buf = Vec::new();
        req.write(&mut buf).unwrap();
        assert_eq!(req.size(), buf.len());
        assert_eq!(buf[8], FLAG_CLOSE);
        // entry count = 0
        assert_eq!(&buf[9..13], 0i32.to_le_bytes().as_slice());
    }

    #[test]
    fn should_decode_streamer_start_response() {
        let mut bytes = Vec::new();
        write_i64(&mut bytes, 12345).unwrap();

        let mut cursor = std::io::Cursor::new(bytes);
        let resp = StreamerStartResponse::read(&mut cursor).unwrap();
        assert_eq!(resp.resource_id, 12345);
    }

    #[test]
    fn config_flags_reflect_settings() {
        let config = DataStreamerConfig::new()
            .with_allow_overwrite(true)
            .with_skip_store(true);

        let flags = config.flags(true, false);
        assert_ne!(flags & FLAG_ALLOW_OVERWRITE, 0);
        assert_ne!(flags & FLAG_SKIP_STORE, 0);
        assert_ne!(flags & FLAG_KEEP_BINARY, 0);
        assert_ne!(flags & FLAG_FLUSH, 0);
        assert_eq!(flags & FLAG_CLOSE, 0);
    }
}
