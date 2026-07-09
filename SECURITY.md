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
- **`forge serve-batch` bearer auth is a single shared key** (`--api-key`).
  It gates every route, but there is no per-tenant isolation — run one
  instance per trust domain and keep it on a network you control. Front it
  with your own TLS/authenticating proxy for anything beyond loopback.
- **The coordinator lease-proxy (`serve_coordinator`) is unauthenticated by
  design** and must only be reachable from your own workers' network.
- **Credentials are environment-only.** Object-store credentials come from the
  standard `AWS_*` / `GOOGLE_*` / `AZURE_*` env vars; API keys are never
  written to the checkpoint DB, results, or logs.
- **Checkpoint and result files are plaintext.** Prompts and completions land
  on disk (or your object store) unencrypted — apply disk/bucket encryption
  and file permissions appropriate to your data class.
