# forge — single static binary in a FROM-scratch image.
#
# Build:  docker build -t <registry>/forge:v0.1.0 .
# Run:    docker run --rm <registry>/forge:v0.1.0 --help
#
# The binary is statically linked against musl (bundled SQLite included), so the
# runtime stage is empty: no shell, no libc, no package manager — the image IS
# the ~4 MB binary plus CA certificates for HTTPS upstreams.

FROM rust:1-alpine AS build
RUN apk add --no-cache musl-dev
# Build identity. Both are OPTIONAL and both are absent rather than guessed when
# not supplied: a release container has no .git, and Cargo exports TARGET only to
# build scripts (which this workspace does not have).
ARG FORGE_GIT_SHA
ARG FORGE_TARGET
ENV FORGE_GIT_SHA=$FORGE_GIT_SHA FORGE_TARGET=$FORGE_TARGET
WORKDIR /src
COPY . .
# No `|| true` here: `cargo build … || true` exits 0 on a FAILED build, which would
# ship an image built from whatever was in the layer cache. Stripping is handled by
# `strip = true` in the release profile, so there is nothing to tolerate the absence
# of.
RUN cargo build -p forge-cli --release --locked

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /src/target/release/forge /forge
# Non-root (distroless "nonroot" convention); the binary needs no privileges.
USER 65532:65532
ENTRYPOINT ["/forge"]
CMD ["--help"]
