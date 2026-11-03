// FORGE PATCH (FORGE_PATCHES.md #1): this file is new; it is not part of upstream.

use std::io::{ErrorKind, Read, Result as IoResult};
use std::sync::mpsc::{channel, Receiver, Sender};

/// Wraps a request body that is still being read off the connection, and reports,
/// exactly once, whether it was read to its end.
///
/// The connection must not parse another request until it knows. Bytes of a body the
/// handler did not read are still in the socket, and parsing them as the next request is
/// request smuggling: whoever wrote the body chooses the "request" the server answers
/// next. Upstream discarded the remainder of a length-delimited body when it was dropped
/// (see `EqualReader`), which could abort the process or block a thread forever, and
/// discarded nothing at all of a chunked one, which is the smuggling case.
///
/// Nothing is discarded here. A body read to its end lets the connection carry on; a
/// body dropped early, or that failed to read, closes the connection after the response.
pub struct BodyReader<R> {
    inner: R,
    consumed: Option<Sender<bool>>,
}

impl<R: Read> BodyReader<R> {
    pub fn new(inner: R) -> (BodyReader<R>, Receiver<bool>) {
        let (tx, rx) = channel();
        (
            BodyReader {
                inner,
                consumed: Some(tx),
            },
            rx,
        )
    }
}

impl<R> BodyReader<R> {
    fn report(&mut self, consumed: bool) {
        if let Some(tx) = self.consumed.take() {
            // The connection may already be gone; then nobody needs the answer.
            let _ = tx.send(consumed);
        }
    }
}

impl<R: Read> Read for BodyReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if buf.is_empty() {
            // A zero-length read returns 0 without meaning end-of-body.
            return Ok(0);
        }
        match self.inner.read(buf) {
            Ok(0) => {
                // Reported as soon as it is known, so a pipelined next request can be
                // read while the handler is still working on this one.
                self.report(true);
                Ok(0)
            }
            Ok(n) => Ok(n),
            Err(e) if e.kind() == ErrorKind::Interrupted => Err(e),
            Err(e) => {
                self.report(false);
                Err(e)
            }
        }
    }
}

impl<R> Drop for BodyReader<R> {
    fn drop(&mut self) {
        // Dropped before its end: the rest is still on the wire.
        self.report(false);
    }
}

#[cfg(test)]
mod tests {
    use super::BodyReader;
    use std::io::{Cursor, Read};

    #[test]
    fn a_body_read_to_its_end_reports_consumed() {
        let (mut body, consumed) = BodyReader::new(Cursor::new(b"hello".to_vec()));
        let mut s = String::new();
        body.read_to_string(&mut s).unwrap();
        assert_eq!(s, "hello");
        assert_eq!(
            consumed.try_recv(),
            Ok(true),
            "known at EOF, before the drop"
        );
        drop(body);
        assert!(consumed.try_recv().is_err(), "reported exactly once");
    }

    #[test]
    fn a_body_dropped_early_reports_abandoned() {
        let (mut body, consumed) = BodyReader::new(Cursor::new(b"hello".to_vec()));
        let mut one = [0u8; 1];
        body.read_exact(&mut one).unwrap();
        drop(body);
        assert_eq!(consumed.recv(), Ok(false));
    }

    #[test]
    fn an_unread_body_reports_abandoned() {
        let (body, consumed) = BodyReader::new(Cursor::new(b"hello".to_vec()));
        drop(body);
        assert_eq!(consumed.recv(), Ok(false));
    }

    #[test]
    fn a_read_error_reports_abandoned() {
        struct Broken;
        impl Read for Broken {
            fn read(&mut self, _: &mut [u8]) -> std::io::Result<usize> {
                Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "cut",
                ))
            }
        }
        let (mut body, consumed) = BodyReader::new(Broken);
        assert!(body.read(&mut [0u8; 4]).is_err());
        assert_eq!(consumed.try_recv(), Ok(false));
    }
}
