//! Sans-io session core: sequencing, replay buffer, dedup, Ack cadence.
//! Used by both sides; knows nothing about transports or timers.

use std::collections::VecDeque;

use prost::Message as _;
use slozhn_frame::proto::v1::{Frame, frame};

use crate::SessionError;

/// Does the frame participate in the session (gets a seq / is deduped)?
pub(crate) fn sessioned(f: &Frame) -> bool {
    !matches!(
        f.kind,
        Some(frame::Kind::Hello(_))
            | Some(frame::Kind::Ack(_))
            | Some(frame::Kind::Ping(_))
            | Some(frame::Kind::Pong(_))
            | None
    )
}

pub enum Ingress {
    /// Deliver upward; `ack_due` — the counter reached ack_every.
    Deliver { frame: Frame, ack_due: bool },
    /// Duplicate after replay or control frame (Ack) — does not go up.
    Consumed,
}

pub struct SessionCore {
    next_seq: u64,
    last_recv_seq: u64,
    buffer: VecDeque<(u64, Frame)>,
    buffer_bytes: usize,
    cap_bytes: usize,
    unacked_recv: u32,
    ack_every: u32,
}

impl SessionCore {
    pub fn new(cap_bytes: usize, ack_every: u32) -> Self {
        Self {
            next_seq: 1,
            last_recv_seq: 0,
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            cap_bytes,
            ack_every,
            unacked_recv: 0,
        }
    }

    pub fn last_recv_seq(&self) -> u64 {
        self.last_recv_seq
    }

    /// Current replay buffer occupancy: `(bytes_used, cap_bytes)`. Used by
    /// the client transport to apply Sink backpressure before hitting
    /// `BufferOverflow`.
    pub fn buffer_usage(&self) -> (usize, usize) {
        (self.buffer_bytes, self.cap_bytes)
    }

    /// Outgoing frame: stamp seq + replay buffer. Overflow = session death.
    pub fn on_egress(&mut self, mut f: Frame) -> Result<Frame, SessionError> {
        if !sessioned(&f) {
            return Ok(f);
        }
        f.seq = self.next_seq;
        self.next_seq += 1;
        let size = f.encoded_len();
        if self.buffer_bytes + size > self.cap_bytes {
            return Err(SessionError::BufferOverflow);
        }
        self.buffer_bytes += size;
        self.buffer.push_back((f.seq, f.clone()));
        Ok(f)
    }

    /// Incoming frame: Ack → trim; duplicate → Consumed; fresh → Deliver.
    pub fn on_ingress(&mut self, f: Frame) -> Ingress {
        if let Some(frame::Kind::Ack(ack)) = &f.kind {
            let last = ack.last_seq;
            self.trim(last);
            return Ingress::Consumed;
        }
        if !sessioned(&f) {
            return Ingress::Deliver {
                frame: f,
                ack_due: false,
            };
        }
        if f.seq <= self.last_recv_seq {
            return Ingress::Consumed; // duplicate after replay
        }
        self.last_recv_seq = f.seq;
        self.unacked_recv += 1;
        let ack_due = self.unacked_recv >= self.ack_every;
        Ingress::Deliver { frame: f, ack_due }
    }

    /// Any unacknowledged incoming frames (for the Ack timer)?
    pub fn ack_pending(&self) -> bool {
        self.unacked_recv > 0
    }

    /// Build an Ack and reset the counter.
    pub fn make_ack(&mut self) -> Frame {
        self.unacked_recv = 0;
        Frame {
            stream_id: 0,
            seq: 0,
            kind: Some(frame::Kind::Ack(slozhn_frame::proto::v1::Ack {
                last_seq: self.last_recv_seq,
            })),
        }
    }

    fn trim(&mut self, acked: u64) {
        while let Some((seq, f)) = self.buffer.front() {
            if *seq <= acked {
                self.buffer_bytes -= f.encoded_len();
                self.buffer.pop_front();
            } else {
                break;
            }
        }
    }

    /// Resume: trim to what the peer acknowledged and return the replay tail.
    pub fn replay_after(&mut self, peer_last_recv: u64) -> Vec<Frame> {
        self.trim(peer_last_recv);
        self.buffer.iter().map(|(_, f)| f.clone()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use slozhn_frame::proto::v1::{Message, Ping};

    fn msg(n: usize) -> Frame {
        Frame {
            stream_id: 1,
            seq: 0,
            kind: Some(frame::Kind::Message(Message {
                payload: Bytes::from(vec![0u8; n]),
                compressed: false,
            })),
        }
    }

    fn ack(last: u64) -> Frame {
        Frame {
            stream_id: 0,
            seq: 0,
            kind: Some(frame::Kind::Ack(slozhn_frame::proto::v1::Ack {
                last_seq: last,
            })),
        }
    }

    #[test]
    fn egress_stamps_monotone_seq_from_one() {
        let mut c = SessionCore::new(1024, 16);
        assert_eq!(c.on_egress(msg(1)).unwrap().seq, 1);
        assert_eq!(c.on_egress(msg(1)).unwrap().seq, 2);
    }

    #[test]
    fn ping_bypasses_session() {
        let mut c = SessionCore::new(1024, 16);
        let ping = Frame {
            stream_id: 0,
            seq: 0,
            kind: Some(frame::Kind::Ping(Ping { opaque: 1 })),
        };
        assert_eq!(c.on_egress(ping.clone()).unwrap().seq, 0);
        assert!(matches!(
            c.on_ingress(ping),
            Ingress::Deliver { ack_due: false, .. }
        ));
        assert!(!c.ack_pending());
    }

    #[test]
    fn ack_trims_buffer() {
        let mut c = SessionCore::new(1024, 16);
        for _ in 0..3 {
            c.on_egress(msg(8)).unwrap();
        }
        assert_eq!(c.replay_after(0).len(), 3);
        assert!(matches!(c.on_ingress(ack(2)), Ingress::Consumed));
        assert_eq!(c.replay_after(0).len(), 1); // only seq 3 remains
    }

    #[test]
    fn dedup_drops_replayed() {
        let mut c = SessionCore::new(1024, 16);
        let mut f = msg(1);
        f.seq = 5;
        assert!(matches!(c.on_ingress(f.clone()), Ingress::Deliver { .. }));
        assert!(matches!(c.on_ingress(f.clone()), Ingress::Consumed)); // duplicate
        let mut f2 = msg(1);
        f2.seq = 4;
        assert!(matches!(c.on_ingress(f2), Ingress::Consumed)); // stale
    }

    #[test]
    fn ack_due_on_nth_frame() {
        let mut c = SessionCore::new(1024, 3);
        for i in 1..=2u64 {
            let mut f = msg(1);
            f.seq = i;
            assert!(matches!(
                c.on_ingress(f),
                Ingress::Deliver { ack_due: false, .. }
            ));
        }
        let mut f = msg(1);
        f.seq = 3;
        assert!(matches!(
            c.on_ingress(f),
            Ingress::Deliver { ack_due: true, .. }
        ));
        let a = c.make_ack();
        assert!(matches!(a.kind, Some(frame::Kind::Ack(a)) if a.last_seq == 3));
        assert!(!c.ack_pending());
    }

    #[test]
    fn overflow_kills_session() {
        let mut c = SessionCore::new(64, 16);
        c.on_egress(msg(30)).unwrap();
        assert!(matches!(
            c.on_egress(msg(40)),
            Err(SessionError::BufferOverflow)
        ));
    }

    #[test]
    fn replay_after_returns_tail_in_order() {
        let mut c = SessionCore::new(4096, 16);
        for _ in 0..5 {
            c.on_egress(msg(4)).unwrap();
        }
        let replay = c.replay_after(2);
        let seqs: Vec<u64> = replay.iter().map(|f| f.seq).collect();
        assert_eq!(seqs, vec![3, 4, 5]);
    }
}
