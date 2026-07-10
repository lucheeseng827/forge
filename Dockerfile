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
WORKDIR /src
COPY . .
RUN cargo build -p forge-cli --release --locked \
    && strip target/release/forge || true

FROM scratch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=build /src/target/release/forge /forge
# Non-root (distroless "nonroot" convention); the binary needs no privileges.
USER 65532:65532
ENTRYPOINT ["/forge"]
CMD ["--help"]
