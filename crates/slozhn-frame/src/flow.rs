use crate::error::ProtocolError;
use crate::MAX_WINDOW;

/// Credit window (h2 model). Messages are never split, so the window may
/// go negative by the size of one message: send is allowed while
/// available > 0, consume deducts the full size.
#[derive(Debug)]
pub struct Window(i64);

impl Window {
    pub fn new(initial: u32) -> Self {
        Self(i64::from(initial))
    }

    pub fn available(&self) -> i64 {
        self.0
    }

    pub fn can_send(&self) -> bool {
        self.0 > 0
    }

    pub fn consume(&mut self, n: usize) {
        self.0 -= n as i64;
    }

    pub fn credit(&mut self, n: u32) -> Result<(), ProtocolError> {
        let next = self.0 + i64::from(n);
        if next > MAX_WINDOW {
            return Err(ProtocolError::WindowOverflow);
        }
        self.0 = next;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ProtocolError;

    #[test]
    fn borrow_below_zero_then_blocked() {
        let mut w = Window::new(10);
        assert!(w.can_send());
        w.consume(25); // one message larger than the window — allowed (borrow)
        assert_eq!(w.available(), -15);
        assert!(!w.can_send()); // next send waits
        w.credit(16).unwrap();
        assert!(w.can_send()); // 1 > 0
    }

    #[test]
    fn credit_overflow_is_protocol_error() {
        let mut w = Window::new(u32::MAX);
        assert_eq!(w.credit(u32::MAX), Err(ProtocolError::WindowOverflow));
    }
}
