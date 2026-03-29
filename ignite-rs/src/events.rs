use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

const EVENT_HISTORY_LIMIT: usize = 256;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EventSubscriptions {
    pub connection: bool,
    pub request: bool,
    pub lifecycle: bool,
}

impl EventSubscriptions {
    pub fn all() -> Self {
        Self {
            connection: true,
            request: true,
            lifecycle: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClientEvent {
    Connection(ConnectionEvent),
    Request(RequestEvent),
    Lifecycle(LifecycleEvent),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionEvent {
    pub kind: ConnectionEventKind,
    pub address: String,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionEventKind {
    ConnectAttempt,
    Connected,
    ConnectFailed,
    Closed,
    ReconnectAttempt,
    Reconnected,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestEvent {
    pub kind: RequestEventKind,
    pub op_code: i16,
    pub correlation_id: i64,
    pub address: String,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RequestEventKind {
    Started,
    Succeeded,
    Failed,
    Retried,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LifecycleEvent {
    pub kind: LifecycleEventKind,
    pub detail: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LifecycleEventKind {
    Created,
    Failed,
    Closed,
}

#[derive(Clone)]
pub(crate) struct EventBus {
    sender: broadcast::Sender<ClientEvent>,
    history: Arc<Mutex<VecDeque<ClientEvent>>>,
    subscriptions: EventSubscriptions,
}

impl EventBus {
    pub(crate) fn new(subscriptions: EventSubscriptions) -> Self {
        let (sender, _) = broadcast::channel(256);
        Self {
            sender,
            history: Arc::new(Mutex::new(VecDeque::with_capacity(EVENT_HISTORY_LIMIT))),
            subscriptions,
        }
    }

    pub(crate) fn subscribe(&self) -> broadcast::Receiver<ClientEvent> {
        let snapshot = self
            .history
            .lock()
            .expect("event history poisoned")
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        if snapshot.is_empty() {
            return self.sender.subscribe();
        }

        let mut upstream = self.sender.subscribe();
        let (sender, receiver) = broadcast::channel(256);

        for event in snapshot {
            let _ = sender.send(event);
        }

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                loop {
                    match upstream.recv().await {
                        Ok(event) => {
                            if sender.send(event).is_err() {
                                break;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(_)) => continue,
                        Err(broadcast::error::RecvError::Closed) => break,
                    }
                }
            });
        } else {
            std::thread::spawn(move || loop {
                match upstream.blocking_recv() {
                    Ok(event) => {
                        if sender.send(event).is_err() {
                            break;
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            });
        }

        receiver
    }

    pub(crate) fn emit_connection(
        &self,
        kind: ConnectionEventKind,
        address: impl Into<String>,
        detail: Option<String>,
    ) {
        if !self.subscriptions.connection {
            return;
        }
        let event = ClientEvent::Connection(ConnectionEvent {
            kind,
            address: address.into(),
            detail,
        });
        self.record(&event);
        let _ = self.sender.send(event);
    }

    pub(crate) fn has_request_subscribers(&self) -> bool {
        self.subscriptions.request
    }

    pub(crate) fn emit_request(
        &self,
        kind: RequestEventKind,
        op_code: i16,
        correlation_id: i64,
        address: impl Into<String>,
        detail: Option<String>,
    ) {
        if !self.subscriptions.request {
            return;
        }
        let event = ClientEvent::Request(RequestEvent {
            kind,
            op_code,
            correlation_id,
            address: address.into(),
            detail,
        });
        self.record(&event);
        let _ = self.sender.send(event);
    }

    pub(crate) fn emit_lifecycle(&self, kind: LifecycleEventKind, detail: Option<String>) {
        if !self.subscriptions.lifecycle {
            return;
        }
        let event = ClientEvent::Lifecycle(LifecycleEvent { kind, detail });
        self.record(&event);
        let _ = self.sender.send(event);
    }

    fn record(&self, event: &ClientEvent) {
        let mut history = self.history.lock().expect("event history poisoned");
        if history.len() == EVENT_HISTORY_LIMIT {
            history.pop_front();
        }
        history.push_back(event.clone());
    }
}
