use std::io::Read;
use std::io::Result as IoResult;
use std::io::{Error as IoError, ErrorKind};

/// A `Reader` that reads exactly the number of bytes from a sub-reader.
///
/// If the limit is reached, it returns EOF.
///
/// FORGE PATCH (FORGE_PATCHES.md #1): upstream read and discarded whatever was left of
/// the body when this was dropped, through `vec![0; remaining]` — a buffer the size of
/// the client's *declared* `Content-Length`. One request declaring more than RAM + swap
/// aborted the process, and a client that declared a body and never sent it blocked the
/// dropping thread indefinitely (there is no socket read timeout). Nothing is discarded
/// here any more: `BodyReader` reports whether the body was read to its end, and the
/// connection is closed rather than re-synchronised when it was not.
pub struct EqualReader<R>
where
    R: Read,
{
    reader: R,
    size: usize,
}

impl<R> EqualReader<R>
where
    R: Read,
{
    pub fn new(reader: R, size: usize) -> EqualReader<R> {
        EqualReader { reader, size }
    }
}

impl<R> Read for EqualReader<R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if self.size == 0 {
            return Ok(0);
        }

        let buf = if buf.len() < self.size {
            buf
        } else {
            &mut buf[..self.size]
        };

        match self.reader.read(buf) {
            // FORGE PATCH (#1): the connection ended before the declared length. Upstream
            // reported a plain EOF, so a handler reading to the end could not tell a
            // truncated body from a complete short one and would accept it as whole.
            Ok(0) if !buf.is_empty() => Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "connection closed before the declared Content-Length was received",
            )),
            Ok(len) => {
                self.size -= len;
                Ok(len)
            }
            err @ Err(_) => err,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EqualReader;
    use std::io::Read;

    #[test]
    fn test_limit() {
        use std::io::Cursor;

        let mut org_reader = Cursor::new("hello world".to_string().into_bytes());

        {
            let mut equal_reader = EqualReader::new(org_reader.by_ref(), 5);

            let mut string = String::new();
            equal_reader.read_to_string(&mut string).unwrap();
            assert_eq!(string, "hello");
        }

        let mut string = String::new();
        org_reader.read_to_string(&mut string).unwrap();
        assert_eq!(string, " world");
    }

    // FORGE PATCH (#1): upstream's `test_not_enough` asserted that dropping a partly-read
    // body discarded the rest. Dropping now leaves it unread; the connection layer closes
    // the connection instead of reading on.
    #[test]
    fn dropping_a_partly_read_body_discards_nothing() {
        use std::io::Cursor;

        let mut org_reader = Cursor::new("hello world".to_string().into_bytes());

        {
            let mut equal_reader = EqualReader::new(org_reader.by_ref(), 5);

            let mut vec = [0];
            equal_reader.read_exact(&mut vec).unwrap();
            assert_eq!(vec[0], b'h');
        }

        let mut string = String::new();
        org_reader.read_to_string(&mut string).unwrap();
        assert_eq!(string, "ello world");
    }

    #[test]
    fn a_huge_declared_length_costs_nothing_when_dropped() {
        use std::io::Cursor;

        let mut org_reader = Cursor::new(b"abc".to_vec());
        drop(EqualReader::new(org_reader.by_ref(), usize::MAX / 2));
        assert_eq!(org_reader.position(), 0);
    }

    #[test]
    fn a_body_cut_short_is_an_error_not_a_short_body() {
        use std::io::Cursor;

        let mut equal_reader = EqualReader::new(Cursor::new(b"abc".to_vec()), 10);
        let mut out = Vec::new();
        let err = equal_reader.read_to_end(&mut out).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
        assert_eq!(out, b"abc");
    }
}
