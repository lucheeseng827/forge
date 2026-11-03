//! `forge serve-batch` against clients that do not play by the rules of HTTP.
//!
//! Each of these once took the server down or desynchronised a connection, through the
//! HTTP layer rather than forge's own handlers (see `vendor/tiny_http/FORGE_PATCHES.md`).
//! They run the real binary in a child process, so a regression shows up as a failed
//! assertion here rather than as an abort that takes the test harness with it.

use std::io::{ErrorKind, Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const KEY: &str = "sk-hostile-test";

struct Server {
    child: Child,
    port: u16,
    dir: PathBuf,
    /// The server's stderr. A panic shows up here even when it only kills one of the
    /// HTTP layer's threads, which is all it does in the debug build these tests run.
    /// The release build is `panic = "abort"`, where the same panic kills the process.
    log: PathBuf,
}

impl Server {
    /// `forge serve-batch` with a key and `threads` accept threads, on a free port.
    fn start(name: &str, threads: u32) -> Self {
        Self::start_limited(name, threads, None)
    }

    /// As [`Server::start`], with at most `nofile` open file descriptors when given.
    fn start_limited(name: &str, threads: u32, nofile: Option<u32>) -> Self {
        let dir = std::env::temp_dir().join(format!("forge-hostile-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let port = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let log = dir.with_extension("stderr.log");
        let bin = env!("CARGO_BIN_EXE_forge");
        let mut cmd = match nofile {
            None => Command::new(bin),
            Some(n) => {
                let mut c = Command::new("sh");
                c.arg("-c")
                    .arg(format!("ulimit -n {n} && exec \"$0\" \"$@\""))
                    .arg(bin);
                c
            }
        };
        let child = cmd
            .env("FORGE_API_KEY", KEY)
            .args(["serve-batch", "--listen", &format!("127.0.0.1:{port}")])
            .arg("--data-dir")
            .arg(&dir)
            .args(["--workers", "http://127.0.0.1:1"])
            .args(["--threads", &threads.to_string()])
            .stdout(Stdio::null())
            .stderr(std::fs::File::create(&log).unwrap())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        while TcpStream::connect(("127.0.0.1", port)).is_err() {
            assert!(Instant::now() < deadline, "serve-batch never started");
            std::thread::sleep(Duration::from_millis(50));
        }
        Server {
            child,
            port,
            dir,
            log,
        }
    }

    fn connect(&self) -> TcpStream {
        let s = TcpStream::connect(("127.0.0.1", self.port)).unwrap();
        s.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        s
    }

    fn alive(&mut self) -> bool {
        self.child.try_wait().unwrap().is_none()
    }

    /// The server still answers a fresh, well-formed request promptly.
    fn assert_healthy(&mut self, after: &str) {
        assert!(self.alive(), "the server process died after {after}");
        let log = std::fs::read_to_string(&self.log).unwrap_or_default();
        assert!(
            !log.contains("panicked"),
            "the server panicked after {after}:\n{log}"
        );
        let mut s = self.connect();
        s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .unwrap();
        let reply = read_all(&mut s);
        assert!(
            reply.starts_with("HTTP/1.1 200"),
            "health did not answer after {after}: {reply:?}"
        );
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_file(&self.log);
    }
}

/// Everything the server sends until it closes the connection or goes quiet for the
/// socket's read timeout.
fn read_all(s: &mut TcpStream) -> String {
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(_) => break, // timed out: the server kept the connection open
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// As [`read_all`], and whether the server ended the connection (EOF or reset) rather
/// than leaving it open past the socket's read timeout.
fn read_until_closed(s: &mut TcpStream) -> (String, bool) {
    let mut out = Vec::new();
    let mut buf = [0u8; 4096];
    let closed = loop {
        match s.read(&mut buf) {
            Ok(0) => break true,
            Ok(n) => out.extend_from_slice(&buf[..n]),
            Err(e) => break !matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut),
        }
    };
    (String::from_utf8_lossy(&out).into_owned(), closed)
}

/// Read exactly one response's head (through the blank line).
fn read_head(s: &mut TcpStream) -> String {
    let mut out = Vec::new();
    let mut byte = [0u8; 1];
    while !out.ends_with(b"\r\n\r\n") {
        match s.read(&mut byte) {
            Ok(1) => out.push(byte[0]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn status_lines(reply: &str) -> usize {
    reply.matches("HTTP/1.1 ").count()
}

/// A request that declares a body far larger than memory and is rejected before the
/// body is read. The HTTP layer used to discard the unread remainder by allocating a
/// buffer of the declared size, so this one request aborted the process.
#[test]
fn an_impossible_content_length_on_a_rejected_request_does_not_kill_the_server() {
    let mut server = Server::start("huge-cl", 2);
    let mut s = server.connect();
    s.write_all(
        b"POST /v1/batches HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer wrong\r\n\
          Content-Length: 4611686018427387904\r\n\r\n",
    )
    .unwrap();
    s.write_all(&[b'x'; 2000]).unwrap();
    let head = read_head(&mut s);
    assert!(head.starts_with("HTTP/1.1 401"), "{head:?}");
    drop(s);
    std::thread::sleep(Duration::from_millis(300));
    server.assert_healthy("an oversized Content-Length");
}

/// Clients that declare a body and then go quiet must not hold the server's accept
/// threads. Discarding the unread body used to read it to the end on the thread that
/// handled the request, with no timeout, so `threads` such connections froze the
/// server for as long as the clients kept them open.
#[test]
fn clients_that_never_send_their_declared_body_do_not_freeze_the_server() {
    let mut server = Server::start("idle-body", 2);
    let idle: Vec<TcpStream> = (0..3)
        .map(|_| {
            let mut s = server.connect();
            s.write_all(
                b"POST /v1/batches HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer wrong\r\n\
                  Content-Length: 1000000\r\n\r\n",
            )
            .unwrap();
            let head = read_head(&mut s);
            assert!(head.starts_with("HTTP/1.1 401"), "{head:?}");
            s // held open, body never sent
        })
        .collect();
    server.assert_healthy("three connections that never sent their body");
    drop(idle);
}

/// The bytes of a body the handler did not read must never be parsed as the next
/// request on the connection. An unread chunked body used to be: here its chunk-size
/// line doubles as a request line, so the server answered a second, smuggled request.
#[test]
fn an_unread_chunked_body_is_never_parsed_as_another_request() {
    let mut server = Server::start("chunked", 2);
    let mut s = server.connect();
    s.write_all(
        b"POST /v1/batches HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer wrong\r\n\
          Transfer-Encoding: chunked\r\n\r\n\
          1a; /v1/batches HTTP/1.1\r\nHost: smuggled\r\n\r\n",
    )
    .unwrap();
    let reply = read_all(&mut s);
    assert!(reply.starts_with("HTTP/1.1 401"), "{reply:?}");
    assert_eq!(
        status_lines(&reply),
        1,
        "the unread body was answered as a second request: {reply:?}"
    );
    server.assert_healthy("an unread chunked body");
}

/// A request whose body WAS read keeps its connection: the fix closes only the
/// connections whose body was abandoned, never a well-behaved keep-alive client's.
#[test]
fn a_consumed_body_keeps_the_connection_alive_and_an_abandoned_one_closes_it() {
    let server = Server::start("keepalive", 2);

    // An upload over 1 KiB (the size above which the body is streamed, not buffered),
    // read in full by the handler, then a second request on the same connection.
    let body = format!(
        "{}\n",
        r#"{"custom_id":"k","method":"POST","url":"/v1/chat/completions","body":{"model":"m","pad":"PAD"}}"#
            .replace("PAD", &"p".repeat(4000))
    );
    let mut s = server.connect();
    write!(
        s,
        "POST /v1/files HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {KEY}\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let head = read_head(&mut s);
    assert!(head.starts_with("HTTP/1.1 200"), "{head:?}");
    let len: usize = head
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .unwrap();
    let mut rest = vec![0u8; len];
    s.read_exact(&mut rest).unwrap();
    s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: x\r\n\r\n")
        .unwrap();
    let head = read_head(&mut s);
    assert!(
        head.starts_with("HTTP/1.1 200"),
        "a keep-alive connection whose body was consumed must stay usable: {head:?}"
    );

    // A body the handler never read: the response arrives, then the connection closes.
    let mut s = server.connect();
    s.write_all(
        b"POST /v1/batches HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer wrong\r\n\
          Content-Length: 5000\r\n\r\n",
    )
    .unwrap();
    s.write_all(&[b'x'; 5000]).unwrap();
    let reply = read_all(&mut s);
    assert!(reply.starts_with("HTTP/1.1 401"), "{reply:?}");
    let _ = s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: x\r\n\r\n");
    let after = read_all(&mut s);
    assert_eq!(
        after, "",
        "the abandoned connection must be closed: {after:?}"
    );
    let _ = s.shutdown(Shutdown::Both);
}

/// Request and header lines are bounded. Each line used to be buffered until its CRLF
/// arrived, however long it was, so a client could grow the server's memory by
/// streaming one endless header.
#[test]
fn an_oversized_header_line_or_header_count_is_refused() {
    let mut server = Server::start("headers", 2);

    // The limit is on the line, CRLF excluded: 16 KiB exactly passes, one byte more not.
    for (len, accepted) in [(16 * 1024, true), (16 * 1024 + 1, false)] {
        let mut s = server.connect();
        let pad = "a".repeat(len - "X-Pad: ".len());
        let _ = write!(
            s,
            "GET /v1/health HTTP/1.1\r\nHost: x\r\nX-Pad: {pad}\r\nConnection: close\r\n\r\n"
        );
        let reply = read_all(&mut s);
        assert_eq!(
            reply.starts_with("HTTP/1.1 200"),
            accepted,
            "a {len}-byte header line: {:?}",
            &reply[..reply.len().min(80)]
        );
    }

    let mut s = server.connect();
    let pad = "a".repeat(64 * 1024);
    let _ = write!(
        s,
        "GET /v1/health HTTP/1.1\r\nHost: x\r\nX-Pad: {pad}\r\n\r\n"
    );
    let reply = read_all(&mut s);
    assert!(
        reply.is_empty() || reply.starts_with("HTTP/1.1 400"),
        "a 64 KiB header line must be refused, got {:?}",
        &reply[..reply.len().min(80)]
    );

    let mut s = server.connect();
    let mut req = String::from("GET /v1/health HTTP/1.1\r\nHost: x\r\n");
    for i in 0..500 {
        req.push_str(&format!("X-H{i}: v\r\n"));
    }
    req.push_str("\r\n");
    let _ = s.write_all(req.as_bytes());
    let reply = read_all(&mut s);
    assert!(
        reply.is_empty() || reply.starts_with("HTTP/1.1 400"),
        "500 headers must be refused, got {:?}",
        &reply[..reply.len().min(80)]
    );

    server.assert_healthy("oversized headers");
}

/// A client that resets its connection as soon as it has sent a request. The HTTP layer
/// still read that request, then panicked unwrapping the peer address it could no
/// longer look up. In the release build that aborted the server; one such connection
/// was enough.
#[test]
fn a_connection_reset_as_it_is_accepted_does_not_panic_the_server() {
    let mut server = Server::start("reset", 2);
    for _ in 0..200 {
        let Ok(mut s) = TcpStream::connect(("127.0.0.1", server.port)) else {
            continue;
        };
        let _ = s.write_all(b"GET /v1/health HTTP/1.1\r\nHost: x\r\n\r\n");
        // A zero linger turns the close into a reset.
        socket2::SockRef::from(&s)
            .set_linger(Some(Duration::ZERO))
            .unwrap();
    }
    std::thread::sleep(Duration::from_millis(500));
    server.assert_healthy("connections reset as they were accepted");
}

/// A request line naming HTTP/2.0 got its 505 through a second response writer, which
/// waits for the request's own writer to be released while the request still held it.
/// The client got no answer, and a thread and a connection stayed stuck for good, one
/// per request, until the server ran out of file descriptors and stopped accepting.
#[test]
fn an_http_2_request_line_is_answered_and_closed() {
    let mut server = Server::start("http2", 2);
    let mut s = server.connect();
    s.write_all(b"GET /v1/health HTTP/2.0\r\nHost: x\r\n\r\n")
        .unwrap();
    let reply = read_all(&mut s);
    assert!(reply.starts_with("HTTP/1.1 505"), "{reply:?}");
    server.assert_healthy("an HTTP/2.0 request line");
}

/// A chunk-size line is bounded like a header line. It used to be buffered until its
/// CR arrived, however long it was, inside a body whose decoded size the handler had
/// capped, so one request could grow the server's memory for as long as it kept sending.
#[test]
fn an_endless_chunk_size_line_is_refused() {
    let mut server = Server::start("chunk-line", 2);
    let mut s = server.connect();
    write!(
        s,
        "POST /v1/files HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {KEY}\r\n\
         Transfer-Encoding: chunked\r\n\r\n"
    )
    .unwrap();
    let _ = s.write_all(&[b'0'; 8 * 1024]);
    // The server must stop reading and end the connection, not wait for a CR that
    // never comes. It answers 400, but closes with the client's bytes still unread,
    // which resets the connection, and a reset can overtake that answer.
    let (reply, closed) = read_until_closed(&mut s);
    assert!(
        closed && (reply.is_empty() || reply.starts_with("HTTP/1.1 400")),
        "an endless chunk-size line must be refused, not buffered (closed: {closed}): {:?}",
        &reply[..reply.len().min(80)]
    );
    server.assert_healthy("an endless chunk-size line");
}

/// A chunked upload the client cut short is a failed upload, not a smaller file. The
/// decoder reported a connection ending inside a chunk as the end of the body, so the
/// truncated bytes were stored and answered 200.
#[test]
fn a_chunked_upload_cut_short_is_not_stored() {
    let mut server = Server::start("chunk-cut", 2);
    let mut s = server.connect();
    write!(
        s,
        "POST /v1/files?filename=cut.jsonl HTTP/1.1\r\nHost: x\r\n\
         Authorization: Bearer {KEY}\r\nTransfer-Encoding: chunked\r\n\r\n64\r\n"
    )
    .unwrap();
    s.write_all(&[b'x'; 40]).unwrap();
    s.shutdown(Shutdown::Write).unwrap();
    let reply = read_all(&mut s);
    assert!(reply.starts_with("HTTP/1.1 400"), "{reply:?}");

    let mut s = server.connect();
    write!(
        s,
        "GET /v1/files HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {KEY}\r\n\
         Connection: close\r\n\r\n"
    )
    .unwrap();
    let list = read_all(&mut s);
    assert!(
        list.starts_with("HTTP/1.1 200") && !list.contains("cut.jsonl"),
        "{list}"
    );
    server.assert_healthy("a chunked upload cut short");
}

/// Running out of file descriptors for a moment used to end the server, and idle
/// connections are enough to cause it. Any `accept` error stopped the HTTP layer
/// accepting for good, with the process still up. A connection accepted with the last
/// free descriptor panicked the accept thread instead, because each connection needs a
/// second descriptor. Two limits one apart make sure one run ends on that odd
/// descriptor. Once the idle connections are gone, the server must accept again.
#[test]
fn running_out_of_file_descriptors_for_a_while_does_not_stop_the_server() {
    for nofile in [64, 65] {
        let mut server = Server::start_limited(&format!("nofile-{nofile}"), 2, Some(nofile));
        let idle: Vec<TcpStream> = (0..100)
            .filter_map(|_| TcpStream::connect(("127.0.0.1", server.port)).ok())
            .collect();
        std::thread::sleep(Duration::from_millis(500));
        drop(idle);
        std::thread::sleep(Duration::from_millis(500));
        server.assert_healthy(&format!("running out of {nofile} file descriptors"));
    }
}
