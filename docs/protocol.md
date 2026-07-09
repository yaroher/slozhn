# slozhn wire protocol

**Protocol version: 1** (`slozhn_frame::PROTOCOL_VERSION = 1`)

This document specifies the slozhn envelope protocol — the frame format and
state machine that flows between a slozhn client and a slozhn server over a
byte- or message-oriented transport. It is written so that an implementer in
another language (Kotlin, Swift, ...) can build an interoperable client or
server **without reading the Rust source**.

Normative language follows RFC 2119: MUST / MUST NOT / SHOULD / SHOULD NOT /
MAY. Where this document and the design notes in
`docs/superpowers/specs/2026-07-08-slozhn-design.md` disagree, this document
and the reference implementation (`crates/slozhn-frame`, `crates/slozhn-session`)
are authoritative; disagreements found while writing this spec are listed in
§11.

The golden conformance vectors in `crates/slozhn-frame/tests/conformance.rs`
are the normative, machine-checked expression of this document (see §10).

## 1. Overview & transport assumptions

slozhn multiplexes many logical RPC streams over one physical connection,
modeled closely on gRPC-over-HTTP/2 (h2): a small connection-level handshake,
independent streams identified by an integer id, per-stream and
per-connection flow control, and graceful shutdown via GoAway.

The protocol is transport-agnostic at the `Frame` level: a transport MUST
provide an ordered, reliable, boundary-preserving channel of encoded `Frame`
messages in both directions (a `Stream`/`Sink` of `Frame` in the reference
implementation's terms). Two concrete transport bindings exist:

- **Byte transport → Frame** (`slozhn_frame::codec`): given an already
  boundary-preserving byte transport (each read yields exactly the bytes of
  one previously-written chunk — e.g. a WebSocket connection, where each
  `send` produces one `message` event on the peer), the codec applies **no
  additional length-delimiting**. One inbound byte chunk MUST decode as
  exactly one `Frame` via protobuf `Frame::decode`. A chunk that fails to
  decode as a `Frame` MUST end the transport (treated as a dead peer) — it
  MUST NOT be skipped or resynced.
- **WebSocket** (`slozhn-ws`, per the design notes): **one binary WS message
  carries exactly one encoded `Frame`.** WebSocket already delimits message
  boundaries, so no additional length prefix is written on the wire. An
  implementer targeting a different transport that does not preserve message
  boundaries (e.g. a raw TCP stream) MUST add their own length-delimiting
  framing before applying this spec; slozhn's reference transports never do
  this themselves.

Frames are encoded as **binary protobuf** (`protocols/slozhn/v1/frame.proto`,
proto3). There are no textual/JSON transports.

## 2. Frame envelope

```protobuf
message Frame {
  uint64 stream_id = 1; // 0 = connection-level frame
  uint64 seq = 2;       // session layer only (§8); 0 otherwise
  oneof kind {
    Open open = 3; Headers headers = 4; Message message = 5;
    HalfClose half_close = 6; Status status = 7; Cancel cancel = 8;
    Ping ping = 9; Pong pong = 10; WindowUpdate window_update = 11;
    GoAway go_away = 12; Hello hello = 13; Ack ack = 14;
  }
}
```

- **`stream_id`**: `0` MUST be used only for connection-level frames — `Ping`,
  `Pong`, `Hello`, `GoAway`, `Ack`, and a connection-level `WindowUpdate`
  (§5). All other kinds (`Open`, `Headers`, `Message`, `HalfClose`, `Status`,
  `Cancel`, a per-stream `WindowUpdate`) MUST carry a non-zero `stream_id`. A
  connection-level kind (`Ping`/`Pong`/`Hello`/`GoAway`/`Ack`) sent with a
  non-zero `stream_id` is a protocol error, as is a stream-level kind sent
  with `stream_id = 0`; both MUST kill the connection (§9).
- **Stream id allocation and parity**: stream ids follow the h2 pattern — the
  side that *opens* a stream picks the id, and the two sides' ids never
  collide because of parity. The **client's first opened stream id is 1**;
  it MUST allocate subsequent ids by adding 2 each time (1, 3, 5, ...). The
  **server's first opened stream id is 2**; it MUST allocate 2, 4, 6, ... In
  slozhn v1 only the client opens streams in practice, but the protocol and
  reference implementation are symmetric — a server MAY open a
  server-initiated stream using an even id, and a receiver MUST accept it.
  An `Open` frame whose `stream_id` parity does not match "peer-opens" from
  the receiver's point of view (e.g. a server receiving an `Open` with an
  even `stream_id`, which would mean the client used a server-parity id) is
  a protocol error that MUST kill the connection. `stream_id = 0` MUST NOT be
  used in an `Open`.
  - A receiver MUST also reject (connection error) an `Open` whose
    `stream_id` is already open, or is one of the ids the connection most
    recently reset (§4.4) — opening the *same* id twice is not treated as a
    race.
- **`seq`**: MUST be `0` unless the session layer (§8) is active on this
  connection. When the session layer is active, `seq` is assigned by the
  sender per §8 and MUST be treated as opaque by the frame layer itself —
  it plays no role in envelope semantics below the session layer.

## 3. Connection handshake

`Hello` is the mandatory first frame in both directions of a connection,
independent of `stream_id`/`seq` semantics elsewhere:

```protobuf
message Hello {
  uint32 version = 1;
  uint32 initial_stream_window = 2;
  uint32 initial_connection_window = 3;
  bytes  session_id = 4;      // session layer only (§8)
  bytes  resume_token = 5;    // session layer only (§8)
  uint64 last_recv_seq = 6;   // session layer only (§8)
  bool   resume_rejected = 7; // session layer only (§8)
}
```

Two handshake modes:

- **Network handshake** (`bind`): each side, upon starting the connection
  driver, MUST immediately send its own `Hello` (`stream_id = 0`) — the
  handshake is symmetric; neither side waits for the peer before sending its
  own `Hello`. Each side MUST then wait for the peer's `Hello` as the very
  first frame it reads from the transport. Any frame read before a `Hello`
  MUST be treated as a protocol error and kill the connection (there is no
  partial/degraded mode). A second `Hello` received *after* the handshake is
  complete (i.e. on an already-established connection) MUST also be treated
  as a protocol error — session-layer resume re-handshakes happen logically
  below the frame layer (see the pre-negotiated mode below and §8), not as a
  second `Hello` on an established `bind()` connection.
  - `version` MUST equal `PROTOCOL_VERSION` (currently 1). A mismatched
    version MUST fail the connection before any streams can be used.
  - `initial_stream_window` / `initial_connection_window` announce the
    sender's **receive** windows (i.e. how much the peer is allowed to send
    it before waiting for credit) — see §5. The reference implementation's
    default is `DEFAULT_WINDOW = 65536` bytes for both.
  - The session-layer fields (`session_id`, `resume_token`, `last_recv_seq`,
    `resume_rejected`) MUST be left at their zero values in a plain
    (non-sessioned) handshake.
- **Pre-negotiated handshake** (`bind_pre_negotiated`): the session layer
  (§8) performs its own `Hello` exchange (including resume) *before*
  handing a transport to the frame layer. In this mode the frame-layer
  connection driver MUST NOT send or wait for a `Hello` of its own; it takes
  its send/receive windows directly from the already-exchanged peer `Hello`.
  This is how a session-layer reconnect resumes a logical connection without
  a second frame-level `Hello` round trip.

## 4. Stream lifecycle

### 4.1 Overview

A stream's lifecycle mirrors gRPC: `Open → [Headers] → Message* → HalfClose
(local) / Message* (remote) → Status | Cancel`. The four gRPC streaming
shapes (unary, server-streaming, client-streaming, bidi) are all the same
state machine; they differ only in how many `Message` frames each side sends
before half-closing.

- **`Open{method, metadata}`**: only the *opener* of a stream sends `Open`.
  `method` MUST be a fully-qualified gRPC method path (`"/pkg.Service/Method"`).
  `metadata` carries request headers (ASCII or `-bin` binary, base64 already
  applied at the HTTP-bridge layer for `-bin` keys — see §9).
- **`Headers{metadata}`**: sent **only by the acceptor**, at most once, and
  MUST NOT be sent by the opener. A `Headers` frame received by the acceptor,
  or a second `Headers` on the same stream, is a protocol error.
- **`Message{payload, compressed}`**: application payload. `compressed` is
  reserved; the reference implementation never sets it. A single `Message`
  payload MUST NOT exceed `MAX_MESSAGE_SIZE` (4 MiB = 4 * 1024 * 1024 bytes);
  exceeding it is a protocol error regardless of stream state or window.
- **`HalfClose{}`**: "I am done sending on this stream", sent by either side
  independently of the other. A `Message` received after the sender's
  `HalfClose` on the same direction is a protocol error.
- **`Status{code, message, trailers}`**: terminal, sent by the acceptor to
  finish the RPC (gRPC status `code`/`message`/`trailers`). Terminates the
  stream in both directions at once (equivalent to both a local and a remote
  half-close).
- **`Cancel{}`**: terminal, MAY be sent by either side (in the reference
  implementation, typically the opener abandoning the RPC, or the acceptor
  rejecting a stream outright, e.g. during GoAway drain — §7). Also
  terminates the stream in both directions at once.

### 4.2 Frame-kind × stream-state table

State is tracked independently per side (each side has a local view of the
stream: "am I the opener", "have I sent/received a half-close", "am I
terminated"). The table below is from **one side's local point of view**,
for an incoming frame of the given kind:

| Incoming kind | Before terminal, opener-local-view | Before terminal, acceptor-local-view | After terminal (Status/Cancel seen) |
|---|---|---|---|
| `Open` | connection error (duplicate/second `Open`, §2) | n/a (received once, creates the slot) | connection error (`DuplicateOpen`) unless the id was recently reset, in which case a **new** `Open` on the *same* id is still an error (§4.4 only excuses non-`Open` kinds) |
| `Headers` | accepted once (first) → delivered to app; a 2nd `Headers` → protocol error | protocol error (acceptor MUST NOT receive `Headers`) | silently ignored (delivered as a no-op `RemoteHalfClose`-shaped event, not surfaced as an error) |
| `Message` | delivered if `MAX_MESSAGE_SIZE` respected and no remote half-close seen yet; error `AfterHalfClose` if remote already half-closed | same rule | silently ignored (size cap is still enforced even after terminal) |
| `HalfClose` | marks remote half-closed; always accepted | same | silently ignored, always "ok" |
| `Status` | terminates the stream in both directions | terminates the stream in both directions | ignored (already terminal) |
| `Cancel` | terminates the stream in both directions | terminates the stream in both directions | ignored (already terminal; this is the normal Status/Cancel race) |
| `WindowUpdate` | credits the send window; overflow beyond `MAX_WINDOW` (2^31 - 1) is a protocol error | same | if the stream id is in the recently-reset set (§4.4), silently ignored; otherwise `UnknownStream` |

Any frame that references a `stream_id` the receiver has never opened or
accepted, and which is **not** in the recently-reset set (§4.4), is a
protocol error (`UnknownStream`) and MUST kill the connection.

### 4.3 Error severity: everything is connection-fatal except Status(8)

The reference implementation has **no notion of a stream-scoped protocol
error that resets only one stream**. Every condition in §4.2 marked
"protocol error" (also: malformed/empty frame, a connection-level kind sent
on a nonzero stream id or vice versa, a second `Hello`, a version mismatch)
MUST end the *entire connection*, not just the offending stream — the driver
MUST send a best-effort `GoAway{code = PROTOCOL_ERROR}` and then close the
transport; every other active stream on that connection is thereby aborted
too (surfaced to gRPC callers as `UNAVAILABLE`, §9).

The **one deliberate exception** is the stream-limit rejection (§4.5): it is
encoded as an ordinary stream-level `Status{code: 8}` specifically so that
a peer exceeding the concurrent-stream limit does not take down unrelated
streams on the same connection.

### 4.4 Reset-id race tolerance

When a side locally terminates a stream (sends `Status`, `Cancel`, or
completes a mutual half-close), it MUST remember that stream id as
"recently reset" for some bounded window (the reference implementation
keeps the most recent 1024 reset ids). Any non-`Open` frame that
subsequently arrives for a recently-reset id — the in-flight tail of what
the peer sent before it had seen the local termination — MUST be **silently
dropped**: no error, no delivery to the application, and the connection MUST
stay alive. This is a legal race, not a protocol violation: the two sides
cannot instantaneously agree a stream is over.

An `Open` for a recently-reset id is treated differently: since ids are
never reused within a connection's lifetime, a fresh `Open` on a
recently-closed id is itself a protocol violation (`DuplicateOpen`), not a
race.

### 4.5 Stream limit

`Config.max_streams` bounds the number of concurrently open streams (both
directions combined) a connection will hold, as a DoS guard. An inbound
`Open` that would exceed the limit MUST be rejected with a stream-level
`Status{code: 8}` (gRPC `RESOURCE_EXHAUSTED`) — the offending stream id is
also added to the recently-reset set — and MUST NOT be delivered to the
application's accept queue. This is a stream-level rejection, not a
connection error (§4.3). A local attempt to *open* a stream beyond the
connection's own `max_streams` MUST fail immediately, locally, without ever
putting a frame on the wire.

## 5. Flow control

Flow control is a direct analogue of the h2 credit-window model, applied
**independently at two levels**: per-stream and per-connection. A `Message`
send MUST be permitted only while **both** the stream's send window and the
connection's send window are currently positive (`> 0`); if either is not,
the sender MUST hold the message until credit arrives (it MUST NOT drop it
or send it early).

- **Initial values**: each side's *receive* windows (its own per-stream and
  connection window) are announced in its own `Hello.initial_stream_window`
  / `Hello.initial_connection_window` (§3). The peer's send window for that
  direction is seeded from those values. The reference implementation's
  default is `DEFAULT_WINDOW = 65536` for both. A newly opened stream's send
  window starts at whatever `initial_stream_window` the peer announced.
- **`WindowUpdate{increment}` credit**: sent by a receiver as payload is
  consumed by the application, crediting the sender's window by `increment`
  bytes. A per-stream `WindowUpdate` (`stream_id != 0`) credits that
  stream's send window; the reference implementation sends **one
  stream-level `WindowUpdate` per consumed `Message`**, unconditionally (it
  is not batched or threshold-gated). A connection-level `WindowUpdate`
  (`stream_id = 0`) credits the shared connection send window; the reference
  implementation batches connection-level credit and only flushes it once
  the accumulated pending credit reaches half of the announced connection
  window (`local_conn_window / 2`).
  - `WindowUpdate.increment` MUST NOT push the addressed window's tracked
    value above `MAX_WINDOW = 2^31 - 1`; doing so is a protocol error
    (`WindowOverflow`) and MUST kill the connection (per-stream) — this is
    the **only** flow-control condition treated as a protocol error.
- **The borrow rule**: because `Message` payloads are never split across
  frames, a window MUST be permitted to go negative by at most the size of
  one message. Precisely: a sender MAY send a message whenever its window is
  currently `> 0`, **even if the message's byte length exceeds the current
  window**; the full payload length is then subtracted, which may drive the
  window negative. The sender MUST NOT send a *second* message while the
  window is `<= 0` — it must wait for enough credit to bring the window back
  above zero. There is no additional check capping *how far* negative a
  single overshoot may push the window beyond the window's own initial
  value; the only independent bound on a single message's size is
  `MAX_MESSAGE_SIZE` (§4.1) — a message that respects `MAX_MESSAGE_SIZE` is
  never rejected purely for exceeding the *window's* available credit; it is
  only ever delayed (blocked) when the window is already `<= 0` before the
  send.
- The reference implementation does **not** independently re-validate an
  incoming `Message`'s size against the *receiver's own* announced window;
  the only receive-side size check is the flat `MAX_MESSAGE_SIZE` cap
  (§4.1). Receive-window bookkeeping is purely local accounting used to
  decide when to emit `WindowUpdate` credit, not an enforcement gate on
  what may be received.

## 6. Keepalive

`Ping{opaque}` / `Pong{opaque}` are connection-level (`stream_id = 0`), used
for physical-transport liveness checks. A `Ping` received at the connection
level MUST be answered with a `Pong` carrying the **same `opaque` value**,
unmodified, as soon as possible. `opaque` MAY be any `uint64` value chosen
by the pinger (the reference implementation uses a locally incrementing
counter per outstanding ping, and a sentinel `u64::MAX` for the session
layer's own keepalive pings — §8); a `Pong`'s `opaque` MUST be matched back
to the corresponding outstanding `Ping` by the pinger.

## 7. GoAway

`GoAway{last_stream_id, code, message}` (`stream_id = 0`) signals graceful
shutdown intent from the sender:

- **`last_stream_id`**: the highest id of a peer-opened stream the sender
  will still process. Any stream the sender itself opened (or the peer opens)
  with an id higher than this after the sender's own `GoAway` MUST be
  rejected.
- **`code`**: `0 = Graceful`, `1 = ProtocolError` (used by the reference
  implementation's own best-effort `GoAway` sent right before it tears down
  a connection on a protocol violation, §4.3), `2 = Internal`. Any other
  numeric value received MUST be treated as `Internal`.
- Sending `GoAway` MUST cause the sender to: (a) reject any subsequent
  locally-initiated `Command::Open` immediately, without putting an `Open`
  on the wire, and (b) reply to any inbound `Open` the peer sends afterward
  (a legal race — the peer has not seen the `GoAway` yet) with `Cancel`
  rather than accepting it or treating it as an error.
- Receiving a peer's `GoAway` MUST cause the receiver to: (a) fail (with a
  stream error) any already-blocked pending sends on locally-opened streams
  whose id is greater than the peer's announced `last_stream_id` (those
  streams will never be serviced), and (b) reject any subsequent
  locally-initiated `Command::Open` locally (without ever sending it) as
  long as either side has sent a `GoAway`.
- **Existing streams with `stream_id <= last_stream_id` MUST be allowed to
  run to completion** (both directions keep exchanging `Message`,
  `HalfClose`, `Status`, `Cancel`, `WindowUpdate` normally) even after
  `GoAway` has been sent or received by either side.
- Note the asymmetry: sending your own `GoAway` is what causes you to reject
  *inbound* `Open`s (because you already told the peer your `last_stream_id`
  boundary); receiving the peer's `GoAway` only stops *you* from opening new
  outbound streams (because the peer told you it won't route them) — it does
  not by itself make you reject inbound `Open`s from that peer.

## 8. Session layer (optional extension, `slozhn-session`)

The session layer sits **above** the frame layer (`crates/slozhn-session`)
and is fully optional; a plain `bind()` connection has no notion of `seq`,
`Ack`, resume, or replay. When active, it wraps the physical transport
before handing it to `bind_pre_negotiated` (§3), so the frame layer itself
never sees a raw `Hello`/resume exchange — only the already-negotiated
windows.

### 8.1 Sessioned frame kinds

A frame is **sessioned** (gets a monotonically increasing `seq` stamped on
egress, is buffered for replay, and is deduplicated on ingress) if and only
if its kind is one of: `Open`, `Headers`, `Message`, `HalfClose`, `Status`,
`Cancel`, `WindowUpdate`, `GoAway`. The following kinds are **not**
sessioned — they are never stamped with a `seq`, never buffered, and pass
through the session layer transparently: `Hello`, `Ack`, `Ping`, `Pong`, and
a frame with no `kind` at all.

`seq` numbering starts at `1` for the first sessioned frame sent on a
session and increases by exactly 1 per sessioned frame (irrespective of
`stream_id` — it is one global counter per direction per session, not
per-stream).

### 8.2 Cumulative Ack

The receiver of sessioned frames tracks `last_recv_seq`, the highest
contiguous `seq` it has accepted (a frame with `seq <= last_recv_seq` is a
duplicate and MUST be silently consumed — not delivered to the application,
not re-acked). It MUST periodically send `Ack{last_seq: last_recv_seq}`
(`stream_id = 0`) back to the sender — cumulative, i.e. it acknowledges
every sessioned frame with `seq <= last_seq`, not just one. The reference
implementation's default cadence is: an `Ack` is due once `ack_every` (16)
sessioned frames have been accepted since the last `Ack`, or `ack_delay`
(250 ms) after the first unacknowledged frame if fewer have arrived by then.

On receiving an `Ack{last_seq}`, the sender MUST trim its replay buffer
(§8.3) of every sessioned frame it holds with `seq <= last_seq`.

### 8.3 Replay buffer and overflow

Every sessioned frame a side sends is held in a **bounded** in-memory replay
buffer (default cap `replay_buffer_bytes = 1 MiB`) until the peer's
cumulative `Ack` confirms it was received. If stamping and buffering a new
outgoing sessioned frame would push the buffer's tracked byte usage past its
cap, this MUST be treated as **honest session death**: the session MUST be
terminated (no silent dropping of frames is permitted), the transport
returns closed to its caller, and every stream that was relying on this
session surfaces `UNAVAILABLE` to the application exactly as it would on an
unresumed disconnect (§9).

### 8.4 Resume handshake

On a physical reconnect, the client re-negotiates using `Hello`'s
session-layer fields (`session_id`, `resume_token`, `last_recv_seq`,
`resume_rejected`) — this exchange happens *before* the frame layer is
re-attached (`bind_pre_negotiated`, §3):

```
 client                                          server
   | ---- Hello{session_id="", resume_token="",       new session
   |       last_recv_seq=0} ------------------------> |
   | <---- Hello{session_id=S, resume_token=T,         |
   |        last_recv_seq=0, resume_rejected=false} -- |
   |            ... session S active, frames flow ...  |
   |            ... physical connection breaks ...     |
   | ---- reconnect: Hello{session_id=S,                resume
   |       resume_token=T, last_recv_seq=<client's      |
   |       highest received seq from server>} --------> |
   | <---- Hello{session_id=S, resume_token=T,          |
   |        last_recv_seq=<server's highest received    |
   |        seq from client>,                           |
   |        resume_rejected=false} -------------------- |
   | <==== replay: server's buffered frames with        |
   |        seq > client's announced last_recv_seq ===  |
   | ==== replay: client's buffered frames with          |
   |       seq > server's announced last_recv_seq ====> |
```

- `Hello.last_recv_seq` sent by side A means "the highest `seq` A has
  received from the peer so far". The receiving side B MUST use that value
  to trim its own outgoing replay buffer (discarding everything already
  confirmed received) and then resend, in `seq` order, everything still
  buffered beyond it (`replay_after(last_recv_seq)`).
- A **new** session is requested by sending an empty `session_id` in
  `Hello`. A server MUST reply with a freshly generated `session_id` and
  `resume_token`, and `resume_rejected = false`. A server MAY refuse to
  create a new session (e.g. it is at its configured concurrent-session
  capacity) by replying with `resume_rejected = true` and empty
  `session_id`/`resume_token`; the client MUST treat this exactly like a
  rejected resume (below) — there is no separate wire signal for "session
  cap reached" versus "resume rejected".
- A **resume** is requested by sending a non-empty `session_id` (+
  `resume_token`) in `Hello`. The server MUST reply with
  `resume_rejected = true` if the `session_id` is unknown, or the
  `resume_token` does not match the one on record for that session (wrong
  token, expired/evicted session, or a server that has restarted and lost
  all session state). `resume_rejected = true` MUST be treated by the client
  as **fatal for the session**: the client MUST NOT retry the same session
  and MUST surface `UNAVAILABLE` on every RPC that depended on it — exactly
  as if there were no session layer at all and the physical connection had
  simply died.
- **While a resume handshake is outstanding** (the reconnect `Hello` has
  been sent but no server reply has arrived yet), the sending side MUST NOT
  send anything else on that physical connection — **only the `Hello` may
  be in flight**. Sessioned application data queued during the gap stays in
  the replay buffer (it is *not* re-sent speculatively before the resume is
  confirmed); doing otherwise would desynchronize `seq` ordering and break
  the receiver's dedup invariant (`seq` must arrive in non-decreasing order
  per direction).
- Dedup on receive is exactly the ingress rule of §8.2: any sessioned frame
  with `seq <= last_recv_seq`, replayed or not, MUST be silently consumed
  without being delivered a second time. Exactly-once delivery to the
  application follows from this dedup plus the resend-on-resume behavior
  above.

### 8.5 Keepalive and idle detection at the session level

Physical-connection keepalive (`Ping`/`Pong`, §6) is **not** itself
sessioned (§8.1) — it is a property of the current physical connection, not
the durable session. The client MAY send a `Ping` on an interval while a
session's physical connection is active and treat a missing `Pong` within a
configured timeout as a broken physical connection, triggering a
reconnect+resume cycle (transparent to the application; in-flight sessioned
frames are unaffected because they live in the replay buffer, not on the
wire). The server side of the session layer independently tracks silence: no
inbound frame for longer than its configured idle timeout means it MUST
"detach" from the current physical connection (stop trying to write to it,
start a TTL timer) and wait for the client to resume; if the TTL elapses
with no resume, the server MUST discard the session entirely (a resume
against a discarded session MUST be rejected, §8.4).

## 9. Error taxonomy

| Failure | Trigger | Effect |
|---|---|---|
| Connection-fatal protocol error | Any condition in §4.3 (unknown/duplicate stream, bad parity, headers/message ordering violation, oversized message, window overflow, malformed handshake, connection/stream kind mismatch, empty frame) | Best-effort `GoAway{code=ProtocolError}` then the transport is torn down; **every** stream on the connection ends, surfaced to gRPC callers as `UNAVAILABLE` (grpc-status 14) via the synthetic trailers path in `slozhn_frame::http` (a stream whose transport disappeared before a `Status` was ever seen gets a fabricated `Status{code: 14, message: "connection lost"}`) |
| Stream-level rejection | Stream-limit exceeded (§4.5) | Ordinary `Status{code: 8}` (`RESOURCE_EXHAUSTED`) on that one stream only; the rest of the connection is unaffected |
| Stream cancellation | Explicit `Cancel` from either side, or the opener dropping its receive handle before seeing a terminal frame | The stream ends (`StreamError::Cancelled` to local callers; the peer sees `Cancel`); the connection is unaffected |
| Reset-id race (§4.4) | A stray frame for a just-closed stream id, in flight before the peer saw the local termination | Silently dropped; no error surfaced anywhere, connection unaffected |
| Graceful drain | Local or peer `GoAway` (§7) | New stream opens refused (locally or on the wire, per §7's asymmetry); existing streams run to completion |
| Session-fatal | Replay buffer overflow (§8.3), or `resume_rejected = true` (§8.4) | The session ends *honestly* — no silent frame loss; every RPC depending on that session surfaces `UNAVAILABLE`, exactly as an unresumed physical disconnect would |

Applications never see envelope-level errors directly for RPC-scoped
failures — those are always translated to an ordinary gRPC `Status` (with
`grpc-status`/`grpc-message`/trailing metadata carried via HTTP trailers at
the `slozhn_frame::http` bridge layer, `status_to_trailers` /
`trailers_to_status`). A response `Status` frame missing entirely (silent
stream end with no `Status`) is bridged to `grpc-status: 14 UNAVAILABLE`
with message `"connection lost"`; trailers with no `grpc-status` header at
all (should not occur from a spec-conformant peer) are read back as
`grpc-status: 2 INTERNAL`.

## 10. Conformance

`crates/slozhn-frame/tests/conformance.rs` is a table-driven suite of golden
frame-sequence tests exercised against the reference implementation
(`slozhn_frame::connection::bind` / `bind_pre_negotiated`, driven through
`slozhn_frame::loopback::pair()`, matching the style of
`crates/slozhn-frame/tests/loopback.rs`). Each test is annotated with the
spec section (§N) it pins. **The golden vectors are normative**: a
conformant reimplementation of this protocol MUST reproduce the same
observable frame kinds, ordering, and error outcomes as those tests assert.
Where this document describes a rule in prose and the conformance suite
pins a concrete sequence for it, the conformance suite is the executable
source of truth for exact framing details (e.g. precisely which frame kinds
appear on the wire, in what order, for a given RPC shape).

## 11. Discrepancies found versus the design notes

The following differences were found between
`docs/superpowers/specs/2026-07-08-slozhn-design.md` §8 and the actual
`slozhn-session` implementation. This document (and the code) is
authoritative; the design notes were written earlier in the project and are
not updated to reflect the shipped shape:

1. **No `ttl` field on the wire.** The design notes describe resume fields
   as `resume: Option<{session_id, token, last_recv_seq}>, ttl`, implying
   `ttl` is negotiated as part of the `Hello` exchange. In the actual
   `Hello` message there is no `ttl` field at all — TTL is a purely
   server-local configuration value (`ServerSessionConfig::ttl`, default 60s,
   how long the server waits, detached, before discarding a session) and is
   never transmitted to the client.
2. **Resume fields are flat on `Hello`, not a nested `Option`.** The design
   notes' `resume: Option<{...}>` framing suggests a distinct
   present/absent resume sub-message. The actual protobuf simply adds
   `session_id`, `resume_token`, `last_recv_seq`, `resume_rejected` as plain
   fields directly on `Hello`; "no resume requested" is encoded as an empty
   `session_id`, not a distinct oneof/optional wrapper.
3. **The design notes' description of protocol errors is a simplification.**
   §5 of the design notes states connection errors arise from "unknown
   stream_id, a second Open, Message after HalfClose" without qualification.
   The actual implementation additionally treats **recently-reset** stream
   ids specially (§4.4 above): frames referencing a stream that *this side*
   itself just terminated are a legal, silently-tolerated race, not a
   protocol error — only frames for stream ids that were *never* opened (or
   whose closure this side does not remember) are connection-fatal. This is
   a refinement, not a contradiction, but it is not mentioned in the design
   notes at all and is essential for an interoperable implementation to get
   right (naively treating every post-terminal frame as an error would make
   normal shutdown races fail spuriously).
