# Security Policy

## Reporting a Vulnerability

Please report security issues **privately**. Do not open a public issue for a
suspected vulnerability.

- Preferred: open a private [GitHub Security Advisory](https://github.com/lucheeseng827/forge/security/advisories/new)
  with details and reproduction steps.
- If you cannot use Security Advisories, contact the maintainer via their GitHub
  profile ([@lucheeseng827](https://github.com/lucheeseng827)) to arrange a private channel.

We aim to acknowledge reports within a few business days and follow a 90-day
coordinated disclosure window, crediting reporters who wish to be named.

## Supported Versions

Until 1.0, the latest published `0.x` minor is supported. From 1.0 onward we
support the latest minor and the previous one.

| Version | Supported |
|---------|-----------|
| latest `0.x` | ✅ |
| older   | ❌ |

## Security model — read before you deploy

forge is designed to run **inside your own trust boundary**, coordinating
endpoints you own. What an operator must understand:

- **Worker endpoints are trusted.** forge POSTs batch items to the
  OpenAI-compatible endpoints you list and stores what they return. It does not
  authenticate *itself* to arbitrary third parties beyond the bearer key you
  configure, and it treats worker responses as data, not code.
- **`forge serve-batch` bearer auth is a single shared key** (`FORGE_API_KEY`,
  or `--api-key`). It gates every route, but there is no per-tenant isolation —
  run one instance per trust domain and keep it on a network you control. Front
  it with your own TLS/authenticating proxy for anything beyond loopback. Without
  a key the server is open, and it says so in a startup warning when bound beyond
  loopback.
- **The coordinator lease-proxy takes a shared token** (`FORGE_COORDINATOR_TOKEN`
  on both `forge-agent serve` and `forge-agent run`). `serve` refuses a
  non-loopback bind without one unless explicitly told otherwise
  (`--insecure-no-auth`). The token is checked in constant time before a request
  body is read. It travels in plain HTTP unless you put TLS in front, so on an
  untrusted network terminate TLS at a proxy or mesh.
- **Credentials are environment-only.** Server secrets (`FORGE_API_KEY`,
  `FORGE_COORDINATOR_TOKEN`), worker keys (`FORGE_WORKER_API_KEY`) and
  object-store credentials (the standard `AWS_*` / `GOOGLE_*` / `AZURE_*` env
  vars) are read from the environment, so none of them has to appear in a
  process listing. A blank secret is a startup error rather than an open server.
  Keys are never written to the checkpoint DB, results, or logs.
- **The HTTP layer is a patched tiny_http** (`vendor/tiny_http`, changes in its
  `FORGE_PATCHES.md`). It closes the connection instead of reading on after a body the
  handler did not consume, bounds header and chunk-size lines, treats a body cut short
  as an error, and keeps accepting after an accept error. It sets **no read timeouts**,
  so a client can hold a connection open for as long as it likes. Put a reverse proxy
  with timeouts in front of any port an untrusted network can reach.
- **Checkpoint and result files are plaintext.** Prompts and completions land
  on disk (or your object store) unencrypted — apply disk/bucket encryption
  and file permissions appropriate to your data class.
