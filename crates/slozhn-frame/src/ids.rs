#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Client,
    Server,
}

impl Side {
    fn first_id(self) -> u64 {
        match self {
            Side::Client => 1,
            Side::Server => 2,
        }
    }

    /// Whether this side can be the initiator of a stream with this id.
    pub fn opens(self, id: u64) -> bool {
        id != 0 && id % 2 == self.first_id() % 2
    }

    pub fn peer(self) -> Side {
        match self {
            Side::Client => Side::Server,
            Side::Server => Side::Client,
        }
    }
}

#[derive(Debug)]
pub struct StreamIdAllocator {
    next: u64,
}

impl StreamIdAllocator {
    pub fn new(side: Side) -> Self {
        Self { next: side.first_id() }
    }

    pub fn next_id(&mut self) -> u64 {
        let id = self.next;
        self.next += 2;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_allocates_odd_server_even() {
        let mut c = StreamIdAllocator::new(Side::Client);
        let mut s = StreamIdAllocator::new(Side::Server);
        assert_eq!((c.next_id(), c.next_id(), c.next_id()), (1, 3, 5));
        assert_eq!((s.next_id(), s.next_id()), (2, 4));
    }

    #[test]
    fn opens_checks_parity() {
        assert!(Side::Client.opens(1) && Side::Client.opens(3));
        assert!(!Side::Client.opens(2));
        assert!(Side::Server.opens(2) && !Side::Server.opens(1));
        assert!(!Side::Client.opens(0) && !Side::Server.opens(0)); // 0 = connection
    }
}
