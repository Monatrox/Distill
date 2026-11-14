FROM docker.io/library/rust:1.97-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM docker.io/library/debian:bookworm-slim

RUN useradd --create-home --uid 10001 distill
RUN install -d -m 0700 -o distill -g distill /var/lib/distill
COPY --from=builder /src/target/release/distill /usr/local/bin/distill

ENV DISTILL_STATE_PATH=/var/lib/distill/state.sqlite3
USER distill
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/distill"]
