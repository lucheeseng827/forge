// FORGE PATCH (FORGE_PATCHES.md #3): this file is new; it is not part of upstream.

use std::io::{Error as IoError, ErrorKind, Read, Result as IoResult};

/// Longest chunk-size line accepted, extensions included and CRLF excluded, in bytes.
const MAX_SIZE_LINE_BYTES: usize = 1024;

/// Decodes a `Transfer-Encoding: chunked` request body.
///
/// It replaces `chunked_transfer::Decoder` on the request side, which had two defects:
/// - It buffered a chunk-size line, extensions included, until its CR arrived, however
///   long it was. A client could grow server memory for as long as it kept sending,
///   inside a body whose decoded length the handler had capped.
/// - When the connection ended inside a chunk it returned a plain EOF, so a handler
///   that read to the end accepted a truncated body as complete.
///
/// Here the size line is bounded. A body that ends before its last chunk is an
/// `UnexpectedEof` error, and a malformed one is `InvalidData`. As upstream, trailers
/// are not accepted: the last chunk must be followed directly by CRLF.
pub struct ChunkedDecoder<R> {
    source: R,
    state: State,
}

enum State {
    /// Expecting a chunk-size line.
    Size,
    /// Inside a chunk, with this many data bytes still to read.
    Data(usize),
    /// The last chunk and its CRLF have been read.
    Done,
}

impl<R: Read> ChunkedDecoder<R> {
    pub fn new(source: R) -> ChunkedDecoder<R> {
        ChunkedDecoder {
            source,
            state: State::Size,
        }
    }

    fn next_byte(&mut self) -> IoResult<u8> {
        let mut byte = [0u8; 1];
        loop {
            match self.source.read(&mut byte) {
                Ok(0) => return Err(truncated()),
                Ok(_) => return Ok(byte[0]),
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
    }

    fn expect_crlf(&mut self) -> IoResult<()> {
        if self.next_byte()? == b'\r' && self.next_byte()? == b'\n' {
            Ok(())
        } else {
            Err(malformed("missing CRLF after a chunk"))
        }
    }

    /// `chunk-size [ ";" extensions ] CRLF`; extensions are ignored, as upstream.
    fn read_size_line(&mut self) -> IoResult<usize> {
        let mut line = Vec::new();
        loop {
            let byte = self.next_byte()?;
            if byte == b'\r' {
                break;
            }
            if line.len() >= MAX_SIZE_LINE_BYTES {
                return Err(malformed("chunk-size line too long"));
            }
            line.push(byte);
        }
        if self.next_byte()? != b'\n' {
            return Err(malformed("chunk-size line not ended by CRLF"));
        }
        let size = line.split(|&b| b == b';').next().unwrap_or(&[]);
        let size = std::str::from_utf8(size)
            .map(|s| s.trim_matches(|c| c == ' ' || c == '\t'))
            .unwrap_or("");
        if size.is_empty() || !size.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(malformed("invalid chunk size"));
        }
        usize::from_str_radix(size, 16).map_err(|_| malformed("chunk size out of range"))
    }
}

impl<R: Read> Read for ChunkedDecoder<R> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            match self.state {
                State::Done => return Ok(0),
                State::Size => {
                    let size = self.read_size_line()?;
                    if size == 0 {
                        self.expect_crlf()?;
                        self.state = State::Done;
                        return Ok(0);
                    }
                    self.state = State::Data(size);
                }
                State::Data(left) => {
                    let want = buf.len().min(left);
                    let n = self.source.read(&mut buf[..want])?;
                    if n == 0 {
                        return Err(truncated());
                    }
                    if n == left {
                        self.expect_crlf()?;
                        self.state = State::Size;
                    } else {
                        self.state = State::Data(left - n);
                    }
                    return Ok(n);
                }
            }
        }
    }
}

fn truncated() -> IoError {
    IoError::new(
        ErrorKind::UnexpectedEof,
        "connection closed inside a chunked body",
    )
}

fn malformed(what: &'static str) -> IoError {
    IoError::new(ErrorKind::InvalidData, what)
}

#[cfg(test)]
mod tests {
    use super::ChunkedDecoder;
    use std::io::{Cursor, ErrorKind, Read};

    fn decode(wire: &[u8]) -> std::io::Result<Vec<u8>> {
        let mut out = Vec::new();
        ChunkedDecoder::new(wire).read_to_end(&mut out)?;
        Ok(out)
    }

    #[test]
    fn decodes_chunks_and_ignores_extensions() {
        assert_eq!(
            decode(b"3\r\nhel\r\nb\r\nlo world!!!\r\n0\r\n\r\n").unwrap(),
            b"hello world!!!"
        );
        assert_eq!(decode(b"0\r\n\r\n").unwrap(), b"");
        assert_eq!(
            decode(b"1;ext name=value\r\na\r\nA \t; x;y\r\n0123456789\r\n0;z\r\n\r\n").unwrap(),
            b"a0123456789"
        );
    }

    #[test]
    fn nothing_past_the_last_chunk_is_read() {
        let mut wire = Cursor::new(b"2\r\nok\r\n0\r\n\r\nGET / HTTP/1.1\r\n".to_vec());
        let mut out = Vec::new();
        ChunkedDecoder::new(&mut wire)
            .read_to_end(&mut out)
            .unwrap();
        assert_eq!(out, b"ok");
        assert_eq!(wire.position(), 12, "the next request is left on the wire");
    }

    #[test]
    fn a_body_cut_short_is_an_error_not_an_eof() {
        for wire in [
            &b"64\r\n0123456789012345678901234567890123456789"[..], // inside a chunk
            b"3\r\nhel\r\n",                                        // before the last chunk
            b"3\r\nhel",                                            // before a chunk's CRLF
            b"3",                                                   // inside a size line
            b"0\r\n",                                               // before the final CRLF
        ] {
            let err = decode(wire).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::UnexpectedEof, "{:?}", wire);
        }
    }

    #[test]
    fn an_endless_size_line_is_refused_after_a_bounded_read() {
        let mut wire = Cursor::new(vec![b'0'; 1 << 20]);
        let err = ChunkedDecoder::new(&mut wire)
            .read(&mut [0u8; 16])
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(wire.position() <= 1025, "read {} bytes", wire.position());

        let mut ext = b"1;".to_vec();
        ext.extend(vec![b'x'; 1 << 20]);
        assert_eq!(decode(&ext).unwrap_err().kind(), ErrorKind::InvalidData);

        let mut longest = vec![b'0'; 1023];
        longest.extend_from_slice(b"1\r\nx\r\n0\r\n\r\n");
        assert_eq!(
            decode(&longest).unwrap(),
            b"x",
            "a 1 KiB size line is accepted"
        );
    }

    #[test]
    fn malformed_framing_is_invalid_data() {
        for wire in [
            &b"X\r\n"[..],
            b"1X\r\n",
            b"-1\r\n",
            b"+1\r\na\r\n0\r\n\r\n",
            b"\r\n",
            b"1 A\r\n",
            b"F\rF",
            b"1\r\nab\r\n",
            b"10000000000000000\r\n",
            b"0\r\nTrailer: x\r\n\r\n",
        ] {
            let err = decode(wire).unwrap_err();
            assert_eq!(err.kind(), ErrorKind::InvalidData, "{:?}", wire);
        }
    }
}
