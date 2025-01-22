# Compile
FROM rust:1.84 AS compiler

WORKDIR /

COPY ./ .

RUN cargo build --release -p delta-search

# Run
FROM debian:bookworm-slim

# Copy the binary from the compiler step above
COPY --from=compiler /target/release/delta-search /usr/src/delta-search

WORKDIR /

# Same as in the web service
EXPOSE 3000

WORKDIR /delta-db
# Use a separate volume for the storage of data
VOLUME /delta-db

CMD ["/usr/src/delta-search"]
