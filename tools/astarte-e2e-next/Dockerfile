FROM lukemathwalker/cargo-chef:0.1.73-rust-1.91.1-trixie AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder 
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN cargo build --release --bin astarte-e2e-cli

FROM chef AS tester 
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --tests --recipe-path recipe.json
COPY . .
RUN cargo test --bin astarte-e2e-cli

FROM debian:trixie-20260112-slim AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/astarte-e2e-cli /usr/local/bin/astarte-e2e
ENTRYPOINT ["/usr/local/bin/astarte-e2e"]
