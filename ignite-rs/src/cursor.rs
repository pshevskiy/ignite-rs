use crate::api::key_value::{CacheReq, CursorPageResp};
use crate::api::OpCode;
use crate::error::IgniteResult;
use crate::exec::TokioExec;
use crate::query::sql::{read_sql_fields_page, RawPageBody, SqlFieldsOpenResponse, SqlRow};
use crate::transport::RequestRoute;
use crate::{ReadableType, WritableType};
use std::mem;

pub struct EntryCursor<K: WritableType + ReadableType, V: WritableType + ReadableType> {
    exec: TokioExec,
    route: RequestRoute,
    cursor_id: i64,
    page_op: OpCode,
    close_on_exhaustion: bool,
    pending_rows: Vec<(Option<K>, Option<V>)>,
    has_more: bool,
    closed: bool,
}

impl<K: WritableType + ReadableType, V: WritableType + ReadableType> EntryCursor<K, V> {
    pub(crate) fn new(
        exec: TokioExec,
        route: RequestRoute,
        cursor_id: i64,
        page_op: OpCode,
        close_on_exhaustion: bool,
        pending_rows: Vec<(Option<K>, Option<V>)>,
        has_more: bool,
    ) -> Self {
        Self {
            exec,
            route,
            cursor_id,
            page_op,
            close_on_exhaustion,
            pending_rows,
            has_more,
            closed: false,
        }
    }

    pub fn cursor_id(&self) -> i64 {
        self.cursor_id
    }

    pub fn has_more(&self) -> bool {
        !self.pending_rows.is_empty() || self.has_more
    }

    pub async fn next_page(&mut self) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        if !self.pending_rows.is_empty() {
            let rows = mem::take(&mut self.pending_rows);
            if !self.has_more {
                self.mark_exhausted().await;
            }
            return Ok(rows);
        }

        if !self.has_more {
            self.mark_exhausted().await;
            return Ok(Vec::new());
        }

        let resp: CursorPageResp<K, V> = self
            .exec
            .send_and_read_with_route(
                self.page_op,
                CacheReq::CursorGetPage::<K, V>(self.cursor_id),
                self.route.clone(),
            )
            .await?;
        self.has_more = resp.has_more;
        if !self.has_more {
            self.mark_exhausted().await;
        }
        Ok(resp.rows)
    }

    pub async fn fetch_all(mut self) -> IgniteResult<Vec<(Option<K>, Option<V>)>> {
        let mut rows = Vec::new();
        loop {
            let page = self.next_page().await?;
            if page.is_empty() {
                break;
            }
            rows.extend(page);
        }
        Ok(rows)
    }

    pub async fn close(&mut self) -> IgniteResult<()> {
        if self.closed {
            return Ok(());
        }

        self.exec
            .send_with_route(
                OpCode::ResourceClose,
                CacheReq::CursorClose::<K, V>(self.cursor_id),
                self.route.clone(),
            )
            .await?;
        self.closed = true;
        Ok(())
    }

    async fn mark_exhausted(&mut self) {
        if self.closed {
            return;
        }

        if self.close_on_exhaustion {
            let _ = self.close().await;
        } else {
            self.closed = true;
        }
    }
}

impl<K: WritableType + ReadableType, V: WritableType + ReadableType> Drop for EntryCursor<K, V> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let exec = self.exec.clone();
            let route = self.route.clone();
            let cursor_id = self.cursor_id;
            handle.spawn(async move {
                let _ = exec
                    .send_with_route(
                        OpCode::ResourceClose,
                        CacheReq::CursorClose::<i32, i32>(cursor_id),
                        route,
                    )
                    .await;
            });
        }
    }
}

pub struct SqlFieldsCursor<Row> {
    exec: TokioExec,
    route: RequestRoute,
    cursor_id: i64,
    field_names: Vec<String>,
    pending_rows: Vec<Row>,
    has_more: bool,
    closed: bool,
}

impl<Row: SqlRow> SqlFieldsCursor<Row> {
    pub(crate) fn new(
        exec: TokioExec,
        route: RequestRoute,
        open: SqlFieldsOpenResponse,
    ) -> IgniteResult<Self> {
        let mut pending_rows = Vec::with_capacity(open.rows.len());
        for row in open.rows {
            pending_rows.push(Row::from_sql_values(row)?);
        }

        Ok(Self {
            exec,
            route,
            cursor_id: open.cursor_id,
            field_names: open.field_names,
            pending_rows,
            has_more: open.has_more,
            closed: false,
        })
    }

    pub fn cursor_id(&self) -> i64 {
        self.cursor_id
    }

    pub fn field_names(&self) -> &[String] {
        &self.field_names
    }

    pub fn has_more(&self) -> bool {
        !self.pending_rows.is_empty() || self.has_more
    }

    pub async fn next_page(&mut self) -> IgniteResult<Vec<Row>> {
        if !self.pending_rows.is_empty() {
            let rows = mem::take(&mut self.pending_rows);
            if !self.has_more {
                self.closed = true;
            }
            return Ok(rows);
        }

        if !self.has_more {
            self.closed = true;
            return Ok(Vec::new());
        }

        let resp: RawPageBody = self
            .exec
            .send_and_read_with_route(
                OpCode::QuerySqlFieldsCursorGetPage,
                CacheReq::CursorGetPage::<i32, i32>(self.cursor_id),
                self.route.clone(),
            )
            .await?;
        let (rows, has_more) = read_sql_fields_page(&resp.body, self.field_names.len())?;
        self.has_more = has_more;
        if !self.has_more {
            self.closed = true;
        }

        let mut decoded = Vec::with_capacity(rows.len());
        for row in rows {
            decoded.push(Row::from_sql_values(row)?);
        }
        Ok(decoded)
    }

    pub async fn fetch_all(mut self) -> IgniteResult<Vec<Row>> {
        let mut rows = Vec::new();
        loop {
            let page = self.next_page().await?;
            if page.is_empty() {
                break;
            }
            rows.extend(page);
        }
        Ok(rows)
    }

    pub async fn close(&mut self) -> IgniteResult<()> {
        if self.closed {
            return Ok(());
        }

        self.exec
            .send_with_route(
                OpCode::ResourceClose,
                CacheReq::CursorClose::<i32, i32>(self.cursor_id),
                self.route.clone(),
            )
            .await?;
        self.closed = true;
        Ok(())
    }
}

impl<Row> Drop for SqlFieldsCursor<Row> {
    fn drop(&mut self) {
        if self.closed {
            return;
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let exec = self.exec.clone();
            let route = self.route.clone();
            let cursor_id = self.cursor_id;
            handle.spawn(async move {
                let _ = exec
                    .send_with_route(
                        OpCode::ResourceClose,
                        CacheReq::CursorClose::<i32, i32>(cursor_id),
                        route,
                    )
                    .await;
            });
        }
    }
}
