# tiny_http — forge's patched copy

This is **tiny_http 0.12.0** from crates.io (upstream git
`212b1c45852fef2093dc1374875a9393c55eb4b9`), vendored with the patches below. It is
the HTTP server under `forge serve-batch` and the coordinator wire
(`forge-agent serve`). Both forge workspaces take it through `[patch.crates-io]`, and it
keeps the version `0.12.0` so their `tiny_http = "0.12"` requirements still match.

0.12.0 is upstream's latest release. The defects below are present in it, and every
one is reachable by a client with no credentials. Vendoring is the smallest change
that fixes them without replacing the HTTP stack.

**What is vendored:** `src/`, the two licence files, `README.md` and `CHANGELOG.md`,
unchanged except as listed here. Upstream's `tests/`, `examples/`, `benches/` and its
dev-dependencies are omitted. `forge.diff` is the complete source change against
upstream (`patch -p1 < forge.diff` reproduces it on a pristine `src/`), and every
patched site is marked `FORGE PATCH`.

## #1 — A request body the handler did not read

**Defects.** When a request was dropped with part of its body unread:

- A length-delimited body over 1 KiB was discarded by `EqualReader`'s `Drop` through
  `vec![0; remaining]`, a buffer the size of the client's *declared* `Content-Length`.
  A request declaring more than RAM + swap aborted the process. Reproduced against
  `forge serve-batch`: a wrong-key `POST` with `Content-Length: 4611686018427387904`
  got its `401`, then the server died with SIGABRT.
- The same discard read the rest of the body on the thread that dropped the request,
  with no socket timeout. A client that declared a body and sent nothing held that
  thread until it disconnected. In forge, that thread is a server accept thread, so a
  handful of such connections froze the server.
- A chunked body was not discarded at all. The connection went on to parse its bytes
  as the next request, so the body's author chose the "request" the server answered
  next: request smuggling, and a response desync behind any proxy that pools
  connections.

**Fix.** Nothing is discarded on drop any more.

- `util/body_reader.rs` (new): wraps the two body forms still on the wire
  (length-delimited over 1 KiB, and chunked). It reports exactly once whether the body
  was read to its end: at EOF, or as abandoned on a read error or a drop.
- `client.rs`: before parsing another request, the connection waits for that report.
  Anything but "consumed" ends the connection after the response has been written.
- `request.rs`: wraps those bodies and hands the report to the connection.
- `util/equal_reader.rs`: the draining `Drop` is gone. A connection that ends before
  the declared length is an `UnexpectedEof` error, not a plain EOF. Upstream let a
  handler that read to the end accept a truncated body as complete.

**Behaviour change.** A keep-alive client whose request body the handler did not read
now has its connection closed after the response, instead of reused. A body that was
read in full keeps the connection, as before. Bodies of 1 KiB or less are read whole
when the request is parsed, so they are unaffected.

## #2 — Unbounded request and header lines

**Defect.** `read_next_line` buffered a request or header line until its CRLF arrived,
however long it was, and the header count had no limit. A client could grow server
memory for as long as it kept sending.

**Fix.** A line over 16 KiB (CRLF excluded), or more than 128 headers, is answered
`400` and the connection closed (`client.rs`).

## #3 — Chunked request bodies

**Defects.** Request bodies were decoded by `chunked_transfer::Decoder`, which:

- Buffered a chunk-size line, extensions included, until its CR arrived, however long
  it was. A handler's cap on the decoded body does not count those bytes, so one
  request could grow server memory for as long as the client kept sending.
  Reproduced against the coordinator: 200 MiB of `0` after a chunked header took it
  from 12 MB to 218 MB, against a 64 MiB body cap.
- Returned a plain EOF when the connection ended inside a chunk. A handler reading to
  the end then took a truncated body as complete. Reproduced against `serve-batch`:
  `64\r\n` (a 100-byte chunk), then 40 bytes and a FIN, stored a 40-byte file and
  answered `200`.

**Fix.** `util/chunked_decoder.rs` (new) replaces it on the request side.

- A chunk-size line over 1 KiB is an `InvalidData` error.
- A body that ends before its last chunk is an `UnexpectedEof` error, so the
  handler's read fails and the connection closes after its response.
- As before, extensions are ignored and trailers are not accepted.

`request.rs` uses it, and `chunked_transfer` is still used to encode responses.

## #4 — A peer gone before its address was read

**Defect.** A client that reset its connection right after sending a request left
`peer_addr()` failing at accept time. The connection still parsed the request it had
already sent, then `unwrap()`ped the missing address. In forge's release build
(`panic = "abort"`) that aborted the server, and one such connection was enough. In
a debug build it killed one thread.

**Fix.** `client.rs`: a connection whose peer address could not be read is closed
before anything is parsed, and the address is no longer unwrapped.

## #5 — A request naming HTTP/2.0 or later

**Defect.** The `505` reply was written through a second response writer. That writer
waits for the previous one to be released, but the request still held it, so the thread
was stuck for good. Each such request leaked a thread and two file descriptors. 200 of
them took a server from 10 to 410 descriptors, and once the descriptors ran out, the
accept loop stopped (#6).

**Fix.** `client.rs`: the `505` is written on the request's own writer, and the
connection is closed.

## #6 — Running out of file descriptors or threads

**Defects.** Enough idle connections exhaust a process's file descriptors or threads,
since there are no read timeouts (see below). Each connection holds two descriptors and
a thread. Running out then ended the server, though no single request was at fault:

- Any error from `accept()` ended the accept loop for good. It was logged, handed to
  `recv`, and nothing accepted a connection again, while the process stayed up.
- A connection accepted with the last free descriptor could not have it duplicated.
  `RefinedTcpStream::new` unwrapped that, panicking the accept thread, which aborted a
  `panic = "abort"` build.
- `TaskPool` started each connection's thread with `thread::spawn`, which panics on
  the accept thread when the OS refuses a thread, for example at a container's pids
  limit.

**Fix.**
- `lib.rs`: on an accept error the loop logs it, backs off 100 ms so a lasting
  condition does not spin, and keeps accepting. Only the server's close trigger ends it.
- `util/refined_tcp_stream.rs`: the duplicate is fallible, and a connection that cannot
  get one is closed like a failed accept.
- `util/task_pool.rs`: a thread the OS refuses is logged, and the connection it would
  have served is closed.

The thread case has no automated test, because a test cannot make the OS refuse a
thread reliably. The descriptor cases are covered in `forge-cli/tests/hostile_http.rs`.

## Not addressed here

- **No socket read timeouts.** A client that connects and sends a partial request
  head holds one of tiny_http's connection threads, not one of forge's accept
  threads, until it disconnects. Enough of them exhaust file descriptors, which since
  #6 only refuses connections while it lasts. Put a reverse proxy with timeouts in front
  of any port reachable from an untrusted network.
- Everything else upstream ships unchanged, including the optional TLS backends,
  which forge does not enable.

## Verifying

- This crate's own tests, including new ones for #1 and #3:
  `CARGO_TARGET_DIR=/tmp/tiny cargo test --lib` from this directory. It is excluded
  from the forge workspace, so it builds as its own root here. Delete the
  `Cargo.lock` that run leaves behind.
- End to end against the real binary: `forge-cli/tests/hostile_http.rs`, plus the
  oversized-body case in `forge-agent/tests/cli.rs`. Every test in
  `hostile_http.rs` fails against unpatched 0.12.0. They capture the server's stderr
  and fail on any panic, so #4 is caught in the debug build too, where a panic does
  not take the process down.

## Dropping the vendor

When upstream releases fixes for #1 to #6, delete this directory and both
`[patch.crates-io]` entries, then run the regression tests above. They are written
against behaviour, not this implementation, so they stay valid against any fix.
