//! Read-only UDS NDJSON client for the supervisor UI protocol (P1.1).
//!
//! repair1 read-side redesign — ONE coherent ownership pipeline:
//!
//! ```text
//! UnixStream ──► BufReader (single instance, lives as long as the
//!                connection; request phase and read loop share it,
//!                so server writes coalescing an ack with events are
//!                never lost to a discarded read-ahead buffer)
//!                 │ request phase: UiClient reads EXACTLY one line
//!                 │ per request from the SAME reader
//!                 ▼
//!            read_loop thread (spawned at subscribe_all; owns the
//!                 reader AND the frame-channel sender EXCLUSIVELY
//!                 from then on)
//!                 │  bounded line assembly (MAX_LINE_BYTES cap)
//!                 │  parse → Frame; try_send into sync_channel(1024)
//!                 │  FIRST Full  → BLOCKING-send exactly one Reset
//!                 │               marker (guaranteed to land after
//!                 │               the queued frames), then STOP
//!                 │  EOF mid-line / over-cap line / undecodable JSON
//!                 │               → same guaranteed Reset, then stop
//!                 │  any exit     → the ONLY sender drops → the
//!                 │               consumer observes Disconnected
//!                 │               (truthful even while the client
//!                 │               handle still exists)
//!                 ▼
//! sync_channel(1024) ──► consumer (main.rs logic pump): Reset or
//!                        Disconnected ⇒ reducer stale + bounded
//!                        re-connect (hello → pages → subscribe)
//! ```
//!
//! Termination: dropping UiClient shuts the socket down, the reader
//! hits EOF, delivers its final marker (if anyone still listens) and
//! acks a completion channel within a bounded wait. A dead or lying
//! server is therefore ALWAYS observable — a delivered Reset, a
//! Disconnected channel, or both — never silence.
//!
//! Request-phase deadline (polish1, P3.3 N2): every REQUEST-RESPONSE
//! op (hello / snapshot / snapshot-events / subscribe) waits for its
//! answer under [`REQUEST_DEADLINE`] — a server is expected to answer
//! these immediately, so a server that accepts and then stays silent
//! fails the connection bounded (TransportError → stale → the existing
//! bounded reconnect), instead of blocking app startup forever.
//! The EVENT-STREAM read loop deliberately carries NO idle deadline:
//! protocol v1 has no heartbeat, so a quiet stream after the handshake
//! is LEGAL and its silence is undetectable at this layer (an honest
//! boundary — a v2 heartbeat is separate protocol work, not a client
//! change). `request` therefore clears the socket read timeout again
//! before the subscribe phase hands the reader to the read loop.
//!
//! Read-only surface: hello / snapshot / snapshot-events / subscribe.
//! There is no write-back, control, approval, tool, shell or file op
//! anywhere in this module.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::sync::mpsc::{
    sync_channel, Receiver, SyncSender, TryRecvError, TrySendError,
};
use std::thread;
use std::time::Duration;

/// One server-sent line, parsed. Subscribe frames carry the raw event
/// object; responses carry their full JSON body.
#[derive(Debug, Clone, PartialEq)]
pub enum Frame {
    /// A subscribe-all delta: `{"type": "event", "event": {...}}`.
    Event(serde_json::Value),
    /// An explicit loss marker: `{"type": "gap", "gap": {...}}`.
    Gap(serde_json::Value),
    /// `{"type": "reset_required"}` — re-baseline before trusting state.
    Reset,
    /// `{"type": "heartbeat", ...}` — protocol liveness only.
    Heartbeat(serde_json::Value),
    /// Any response body (hello / snapshot / snapshot-events / errors).
    Response(serde_json::Value),
}

/// Connection-level error surfaced to the model (stale + reconnect).
#[derive(Debug, Clone, PartialEq)]
pub struct TransportError(pub String);

/// Bound of the UI-side frame channel. A slow consumer never pushes
/// back onto the hub: it receives exactly the queued frames, ONE
/// guaranteed Reset marker, then a Disconnected channel.
pub const CHANNEL_BOUND: usize = 1024;

/// A single line may never exceed this: over-cap lines are a protocol
/// error (bounded memory — bytes past the cap are consumed and
/// discarded up to the newline, then the reader stops with a marker).
pub const MAX_LINE_BYTES: usize = 256 * 1024;

/// Hard budget for one request-response round (hello / snapshot /
/// snapshot-events / subscribe). Rationale: a server answers these
/// IMMEDIATELY — they are table lookups plus a channel registration,
/// never user work — so anything still unanswered after 10 s is a
/// silent/lying server, not a slow one. The deadline fails the
/// connection (TransportError → stale → bounded reconnect); it NEVER
/// applies to the event-stream read loop (protocol v1 has no
/// heartbeat — a quiet stream is legal; see the module docs).
/// Tests inject smaller budgets via
/// [`UiClient::connect_with_request_deadline`] instead of sleeping.
pub const REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// A connected client. The frame-channel sender is owned by the read
/// thread EXCLUSIVELY (the old client-held clone kept the channel
/// forever-connected and masked the reader's death — repair1 fix), so
/// the consumer's `Disconnected` is reliable while the client handle
/// still lives. Dropping the client closes the socket, wakes the
/// reader (EOF), which delivers its final marker and joins within a
/// bounded wait (see [`UiClient::DROP_JOIN_TIMEOUT`]).
#[derive(Debug)]
pub struct UiClient {
    writer: UnixStream,
    reader: Option<BufReader<UnixStream>>, // None once moved to thread
    reader_done: Option<Receiver<()>>,
    reader_handle: Option<thread::JoinHandle<()>>,
    /// True when the server's hello carried `v != 1`. The wire is still
    /// answered (raw-server probes drive this), but the app refuses to
    /// apply anything from a version-mismatched server (repair1 #4).
    pub protocol_version_mismatch: bool,
    /// Per-request read budget (production default
    /// [`REQUEST_DEADLINE`]; tests shrink it so deadline paths run in
    /// milliseconds instead of sleeping the real 10 s).
    pub request_deadline: Duration,
}

impl UiClient {
    /// Connect to the socket and perform hello with the production
    /// [`REQUEST_DEADLINE`]. Returns the client and the hello response
    /// (with `initial_snapshot_cursor`).
    pub fn connect(
        socket_path: &str,
    ) -> Result<(Self, serde_json::Value), TransportError> {
        Self::connect_with_request_deadline(socket_path, REQUEST_DEADLINE)
    }

    /// Deadline-injectable connect (the test entry): identical to
    /// [`connect`](Self::connect) but each request-response wait is
    /// bounded by `deadline` instead of [`REQUEST_DEADLINE`].
    pub fn connect_with_request_deadline(
        socket_path: &str,
        deadline: Duration,
    ) -> Result<(Self, serde_json::Value), TransportError> {
        let stream = UnixStream::connect(socket_path)
            .map_err(|e| TransportError(format!("connect: {e}")))?;
        let mut client = UiClient {
            writer: stream
                .try_clone()
                .map_err(|e| TransportError(format!("clone: {e}")))?,
            reader: Some(BufReader::new(stream)),
            reader_done: None,
            reader_handle: None,
            protocol_version_mismatch: false,
            request_deadline: deadline,
        };
        let hello = client.request(
            "hello",
            serde_json::json!({"v": 1, "client": "supervisor-macos"}),
        )?;
        if hello.get("ok") != Some(&serde_json::Value::Bool(true)) {
            return Err(TransportError(format!("hello refused: {hello}")));
        }
        if hello.get("initial_snapshot_cursor").is_none() {
            return Err(TransportError(
                "hello: no initial cursor".to_string(),
            ));
        }
        client.protocol_version_mismatch =
            hello.get("v").and_then(|v| v.as_i64()) != Some(1);
        Ok((client, hello))
    }

    /// Send `subscribe` with the snapshot cursor, then hand the ONE
    /// persistent reader to the read loop. Frames the server coalesced
    /// into the ack write stay buffered in that same reader and are
    /// delivered by the thread — nothing is ever dropped.
    pub fn subscribe_all(
        &mut self,
        cursor: &serde_json::Value,
    ) -> Result<(Receiver<Frame>, serde_json::Value), TransportError> {
        let response = self.request(
            "subscribe",
            serde_json::json!({"v": 1, "stream_id": "*",
                                "snapshot_cursor": cursor}),
        )?;
        if response.get("ok") != Some(&serde_json::Value::Bool(true)) {
            return Err(TransportError(format!(
                "subscribe refused: {response}"
            )));
        }
        let (sender, receiver) = sync_channel(CHANNEL_BOUND);
        let (done_tx, done_rx) = sync_channel::<()>(1);
        let reader = self.reader.take().ok_or_else(|| {
            TransportError("reader already claimed".into())
        })?;
        // the sender moves into the thread EXCLUSIVELY: when the thread
        // exits, the channel closes and the consumer's Disconnected is
        // truthful even while this client handle still exists
        self.reader_done = Some(done_rx);
        self.reader_handle = Some(
            thread::Builder::new()
                .name("ui-read-loop".into())
                .spawn(move || {
                    read_loop(reader, sender);
                    let _ = done_tx.send(());
                })
                .map_err(|e| TransportError(format!("spawn: {e}")))?,
        );
        Ok((receiver, response))
    }

    /// One request/response round trip (hello / snapshot / snapshot-
    /// events / subscribe). Reads EXACTLY one line from the shared
    /// persistent reader, so any coalesced bytes past the newline stay
    /// buffered for the next request or the read loop. Only legal
    /// before `subscribe_all` claims the reader. The read waits under
    /// `self.request_deadline` (per-op budget; a silent server fails
    /// bounded — see the module docs) and the timeout is CLEARED again
    /// before returning, so the deadline-free event-stream read loop
    /// never inherits it.
    pub fn request(
        &mut self,
        op: &str,
        mut params: serde_json::Value,
    ) -> Result<serde_json::Value, TransportError> {
        if let Some(object) = params.as_object_mut() {
            object
                .entry("op".to_string())
                .or_insert_with(|| serde_json::Value::String(op.into()));
        }
        let mut line = serde_json::to_string(&params)
            .map_err(|e| TransportError(format!("encode: {e}")))?;
        line.push('\n');
        self.writer
            .write_all(line.as_bytes())
            .map_err(|e| TransportError(format!("write {op}: {e}")))?;
        self.writer
            .flush()
            .map_err(|e| TransportError(format!("flush {op}: {e}")))?;
        let reader = self.reader.as_mut().ok_or_else(|| {
            TransportError("request after subscribe".into())
        })?;
        // polish1 (P3.3 N2): arm the per-request read budget. WouldBlock
        // on a blocking socket means the budget expired (SO_RCVTIMEO).
        reader
            .get_ref()
            .set_read_timeout(Some(self.request_deadline))
            .map_err(|e| TransportError(format!("deadline {op}: {e}")))?;
        let read = read_line_capped(reader);
        // Disarm on EVERY path (success, IO error, deadline) before
        // mapping: a surviving connection must never leak the timeout
        // into the subscribe read loop — an idle event stream is legal.
        let _ = reader.get_ref().set_read_timeout(None);
        let (payload, overflow, eof) = match read {
            Ok(triple) => triple,
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock
                        | std::io::ErrorKind::TimedOut
                ) =>
            {
                return Err(TransportError(format!(
                    "read {op}: request deadline exceeded after {:?} \
                     — server accepted but never answered",
                    self.request_deadline
                )));
            }
            Err(e) => {
                return Err(TransportError(format!("read {op}: {e}")))
            }
        };
        if overflow || (eof && payload.is_empty()) {
            return Err(TransportError(format!(
                "{op}: protocol error ({})",
                if overflow { "line over cap" } else { "eof" }
            )));
        }
        serde_json::from_slice(&payload)
            .map_err(|e| TransportError(format!("decode {op}: {e}")))
    }

    /// Bounded-join budget for Drop. Normal case: the reader acks well
    /// within 2s of socket shutdown. The only stuck case is a reader
    /// blocked in the GUARANTEED marker send while the consumer holds
    /// the receiver but never drains — that thread is documented-leaked
    /// (it exits as soon as the consumer drains or drops the receiver;
    /// the socket is already shut down, so nothing but the thread
    /// itself remains).
    pub const DROP_JOIN_TIMEOUT: Duration = Duration::from_secs(2);
}

impl Drop for UiClient {
    fn drop(&mut self) {
        // Wake the reader (EOF) so it exits promptly and acks. The
        // thread holds the only sender: its exit closes the channel.
        let _ = self.writer.shutdown(std::net::Shutdown::Both);
        if let Some(done) = self.reader_done.take() {
            let _ = done.recv_timeout(Self::DROP_JOIN_TIMEOUT);
            // timeout = documented leak path (see DROP_JOIN_TIMEOUT)
        }
        if let Some(handle) = self.reader_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Pull buffered frames without blocking (once per UI frame). Returns
/// `(frames, disconnected)`; `disconnected` is true ONLY for a closed
/// channel (reader thread exited) — never conflated with Empty.
pub fn drain_frames(
    receiver: &Receiver<Frame>,
    limit: usize,
) -> (Vec<Frame>, bool) {
    let mut out = Vec::new();
    let mut disconnected = false;
    while out.len() < limit {
        match receiver.try_recv() {
            Ok(frame) => out.push(frame),
            Err(TryRecvError::Disconnected) => {
                disconnected = true;
                break;
            }
            Err(TryRecvError::Empty) => break,
        }
    }
    (out, disconnected)
}

/// The read loop: ONE BufReader for the connection's whole life. Every
/// frame goes through `try_send`; the FIRST Full becomes a guaranteed
/// (blocking) Reset delivery and ends reading — the consumer sees the
/// queued frames, that one marker, then Disconnected. EOF mid-line, an
/// over-cap line and undecodable JSON are protocol errors with the same
/// guaranteed marker. Silence is impossible by construction.
fn read_loop(mut reader: BufReader<UnixStream>, sender: SyncSender<Frame>) {
    loop {
        let (payload, overflow, eof) = match read_line_capped(&mut reader)
        {
            Ok(triple) => triple,
            Err(_) => {
                deliver_final_marker(&sender);
                return;
            }
        };
        if overflow {
            // over-cap line: bounded memory, explicit protocol error
            deliver_final_marker(&sender);
            return;
        }
        if payload.is_empty() {
            if eof {
                // clean server close: the channel close is the signal,
                // and an explicit Reset makes the stale transition
                // visible even to frame-only observers
                deliver_final_marker(&sender);
                return;
            }
            continue; // blank line
        }
        if eof {
            // truncated final line (half-line JSON): observable loss
            deliver_final_marker(&sender);
            return;
        }
        let value: serde_json::Value =
            match serde_json::from_slice(&payload) {
                Ok(v) => v,
                Err(_) => {
                    // undecodable complete line: protocol error
                    deliver_final_marker(&sender);
                    return;
                }
            };
        let frame = match value.get("type").and_then(|t| t.as_str()) {
            Some("event") => Frame::Event(
                value.get("event").cloned().unwrap_or(value.clone()),
            ),
            Some("gap") => {
                Frame::Gap(value.get("gap").cloned().unwrap_or(value.clone()))
            }
            Some("reset_required") => Frame::Reset,
            Some("heartbeat") => Frame::Heartbeat(value),
            _ => Frame::Response(value),
        };
        match sender.try_send(frame) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                // consumer too slow: exactly one GUARANTEED marker, then
                // stop reading (the model re-baselines on reconnect)
                deliver_final_marker(&sender);
                return;
            }
            Err(TrySendError::Disconnected(_)) => return,
        }
    }
}

/// BLOCKING delivery of the final Reset marker: it must land after
/// every frame already queued. If the consumer is gone this errors
/// immediately and the caller simply exits.
fn deliver_final_marker(sender: &SyncSender<Frame>) {
    let _ = sender.send(Frame::Reset);
}

/// Assemble one line with a hard byte cap. Returns
/// `(payload_without_newline, overflow, eof)`:
/// * `overflow` — the line exceeded [`MAX_LINE_BYTES`]; bytes past the
///   cap were consumed up to the newline and discarded (memory stays
///   bounded; the caller treats this as a protocol error);
/// * `eof` — the stream ended; a non-empty `payload` then is a
///   truncated (half) line.
fn read_line_capped(
    reader: &mut BufReader<UnixStream>,
) -> std::io::Result<(Vec<u8>, bool, bool)> {
    let mut line: Vec<u8> = Vec::with_capacity(1024);
    let mut overflow = false;
    loop {
        let done = {
            let buf = reader.fill_buf()?;
            if buf.is_empty() {
                return Ok((line, overflow, true));
            }
            match buf.iter().position(|&b| b == b'\n') {
                Some(pos) => {
                    if !overflow {
                        // cap check HERE too: the final partial segment
                        // of the line can push past the cap (found via
                        // 8192-aligned kernel slices landing exactly on
                        // the boundary)
                        let room =
                            MAX_LINE_BYTES.saturating_sub(line.len());
                        let take = pos.min(room);
                        line.extend_from_slice(&buf[..take]);
                        if take < pos {
                            overflow = true;
                        }
                    }
                    reader.consume(pos + 1);
                    true
                }
                None => {
                    let n = buf.len();
                    if !overflow && line.len() + n > MAX_LINE_BYTES {
                        let room =
                            MAX_LINE_BYTES.saturating_sub(line.len());
                        line.extend_from_slice(&buf[..room]);
                        overflow = true;
                    } else if !overflow {
                        line.extend_from_slice(&buf[..n]);
                    }
                    reader.consume(n);
                    false
                }
            }
        };
        if done {
            return Ok((line, overflow, false));
        }
    }
}

/// Fetch ALL pages of a paginated op (state or event pages). Honours
/// `next_page_cursor` continuation and `done`; any `ok:false` page is
/// an error (reset semantics live in the model).
pub fn fetch_all_pages(
    client: &mut UiClient,
    op: &str,
    cursor: &serde_json::Value,
    page_key: &str,
    max_pages: usize,
) -> Result<Vec<serde_json::Value>, TransportError> {
    let mut collected = Vec::new();
    let mut page_cursor: Option<serde_json::Value> = None;
    for _ in 0..max_pages {
        let mut params =
            serde_json::json!({"v": 1, "snapshot_cursor": cursor});
        if let Some(pc) = &page_cursor {
            params
                .as_object_mut()
                .expect("params object")
                .insert("page_cursor".into(), pc.clone());
        }
        let response = client.request(op, params)?;
        if response.get("ok") != Some(&serde_json::Value::Bool(true)) {
            return Err(TransportError(format!(
                "{op} page failed: {response}"
            )));
        }
        if let Some(items) = response.get(page_key).and_then(|v| v.as_array())
        {
            collected.extend(items.iter().cloned());
        }
        if response.get("done").and_then(|d| d.as_bool()) == Some(true) {
            return Ok(collected);
        }
        match response.get("next_page_cursor") {
            Some(next) if !next.is_null() => page_cursor = Some(next.clone()),
            _ => return Ok(collected), // no continuation: treat as done
        }
    }
    Err(TransportError(format!("{op}: exceeded {max_pages} pages")))
}

/// Reconnect backoff for the logic pump: bounded growth so a dead
/// socket can never become a retry storm (repair1 #2). Max interval
/// ~5.25s (attempt ≥ 9).
pub fn reconnect_backoff(attempt: usize) -> Duration {
    let step = (attempt as u64).min(9);
    Duration::from_millis(250 * (1 + step * step / 4))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Socketpair-backed reader with the given bytes. The writer runs in
    /// a DETACHED thread: write_all on a socketpair whose peer is not
    /// yet reading blocks once the kernel buffer fills, so large
    /// payloads must not be written inline (that deadlocked the first
    /// version of these tests).
    fn reader_over(data: &[u8]) -> BufReader<UnixStream> {
        let (a, mut b) = UnixStream::pair().unwrap();
        let payload = data.to_vec();
        std::thread::spawn(move || {
            let _ = b.write_all(&payload);
            let _ = b.flush();
            let _ = b.shutdown(std::net::Shutdown::Write);
        });
        BufReader::new(a)
    }

    #[test]
    fn read_line_capped_normal_lines() {
        let mut reader = reader_over(b"{\"a\":1}\n{\"a\":2}\n");
        let (l1, o1, e1) = read_line_capped(&mut reader).unwrap();
        assert_eq!(l1, b"{\"a\":1}");
        assert!(!o1);
        assert!(!e1);
        let (l2, _, e2) = read_line_capped(&mut reader).unwrap();
        assert_eq!(l2, b"{\"a\":2}");
        assert!(!e2);
        let (l3, _, e3) = read_line_capped(&mut reader).unwrap();
        assert!(l3.is_empty());
        assert!(e3); // clean EOF
    }

    #[test]
    fn read_line_capped_half_line_visible() {
        // truncated final line: non-empty payload + eof flag — the
        // caller turns it into an observable marker, never silence
        let mut reader = reader_over(b"{\"type\":\"trun");
        let (l, o, e) = read_line_capped(&mut reader).unwrap();
        assert!(!o);
        assert!(e);
        assert!(!l.is_empty());
    }

    #[test]
    fn read_line_capped_marks_overflow_and_discards() {
        let mut blob = vec![b'a'; MAX_LINE_BYTES + 4096];
        blob.push(b'\n');
        blob.extend_from_slice(b"{}\n");
        let mut reader = reader_over(&blob);
        let (payload, overflow, eof) = read_line_capped(&mut reader).unwrap();
        assert!(overflow);
        assert!(!eof);
        assert!(payload.len() <= MAX_LINE_BYTES); // memory stayed bounded
        // the newline was consumed exactly: the NEXT line parses fine
        let (payload2, overflow2, eof2) =
            read_line_capped(&mut reader).unwrap();
        assert!(!overflow2);
        assert_eq!(payload2, b"{}");
        assert!(!eof2);
    }

    #[test]
    fn backoff_is_bounded() {
        let first = reconnect_backoff(0);
        let huge = reconnect_backoff(1000);
        assert!(huge >= first);
        assert!(huge <= Duration::from_secs(6)); // no unbounded growth
    }

    /// polish1 (P3.3 N2): the request deadline exists, and the socket
    /// read timeout is DISARMED after a successful request — the
    /// event-stream read loop must never inherit an idle deadline
    /// (protocol v1 has no heartbeat; a quiet stream is legal).
    #[test]
    fn request_deadline_armed_then_cleared_for_event_stream() {
        assert_eq!(REQUEST_DEADLINE, Duration::from_secs(10));
        let (a, mut b) = UnixStream::pair().unwrap();
        let _answered = std::thread::spawn(move || {
            use std::io::Write as _;
            let _ = b.write_all(b"{\"ok\":true}\n");
            let _ = b.flush();
            // hold the peer open: no EOF, the client read must return
            // because the ANSWER arrived, not because the socket died
            std::thread::sleep(Duration::from_millis(500));
        });
        let mut client = UiClient {
            writer: a.try_clone().unwrap(),
            reader: Some(BufReader::new(a)),
            reader_done: None,
            reader_handle: None,
            protocol_version_mismatch: false,
            request_deadline: Duration::from_millis(250),
        };
        let response = client
            .request("hello", serde_json::json!({"v": 1}))
            .expect("answered request succeeds well inside the budget");
        assert_eq!(response.get("ok"), Some(&serde_json::Value::Bool(true)));
        let timeout = client
            .reader
            .as_ref()
            .unwrap()
            .get_ref()
            .read_timeout()
            .expect("read_timeout query");
        assert!(
            timeout.is_none(),
            "timeout CLEARED after the request (event stream stays deadline-free)"
        );
        drop(client);
        let _ = _answered.join();
    }
}
