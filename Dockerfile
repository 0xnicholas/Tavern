FROM lukemathwalker/cargo-chef:latest-rust-1 AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release -p tavern-server

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y ca-certificates wget && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN mkdir -p /app/data
COPY --from=builder /app/target/release/tavern-server /usr/local/bin/tavern-server
COPY configs /app/configs
ENV RUST_LOG=info
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --spider -q http://localhost:3000/health || exit 1
EXPOSE 3000
ENTRYPOINT ["tavern-server"]
