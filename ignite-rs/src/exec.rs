use crate::api::OpCode;
use crate::connection_async::AsyncConnection;
use crate::error::IgniteResult;
use crate::{ReadableReq, WriteableReq};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

pub(crate) type IgniteFuture<'a, T> = Pin<Box<dyn Future<Output = IgniteResult<T>> + 'a>>;

#[derive(Clone)]
pub(crate) struct TokioExec {
    pub(crate) conn: Arc<AsyncConnection>,
}

impl TokioExec {
    pub(crate) fn new(conn: Arc<AsyncConnection>) -> Self {
        Self { conn }
    }

    pub(crate) fn send<'a>(
        &'a self,
        op_code: OpCode,
        data: impl WriteableReq + 'a,
    ) -> IgniteFuture<'a, ()> {
        Box::pin(async move { self.conn.send(op_code, data).await })
    }

    pub(crate) fn send_and_read<'a, T: ReadableReq + 'a>(
        &'a self,
        op_code: OpCode,
        data: impl WriteableReq + 'a,
    ) -> IgniteFuture<'a, T> {
        Box::pin(async move { self.conn.send_and_read::<T>(op_code, data).await })
    }

    pub(crate) fn map<'a, T, U, F>(&'a self, ret: IgniteFuture<'a, T>, f: F) -> IgniteFuture<'a, U>
    where
        T: 'a,
        U: 'a,
        F: FnOnce(T) -> U + 'a,
    {
        Box::pin(async move { ret.await.map(f) })
    }
}
