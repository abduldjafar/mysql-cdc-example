# ─── Build Stage ──────────────────────────────────────────────────────────────
FROM rust:alpine AS builder

# musl-dev required for static linking on Alpine
RUN apk add --no-cache musl-dev

WORKDIR /app

# Copy manifests first so dependency layer is cached independently
COPY Cargo.toml Cargo.lock ./

# Dummy src to pre-build ALL dependencies (both lib and bin targets)
RUN mkdir src \
    && echo "fn main() {}" > src/main.rs \
    && echo "// dummy" > src/lib.rs \
    && cargo build --release \
    && rm -rf src

# Copy actual source and rebuild only the application binary
COPY src ./src
# Touch modified files so cargo detects the change
RUN touch src/main.rs src/lib.rs && cargo build --release

# ─── Runtime Stage ────────────────────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /app/target/release/binlog-tap /app/binlog-tap
COPY table_config.toml /app/table_config.toml

ENV RUST_LOG=info

ENTRYPOINT ["/app/binlog-tap"]
