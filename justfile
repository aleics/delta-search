# lint project
lint:
    cargo clippy --all-features --tests

# format
format:
    cargo fmt && cargo clippy --fix

# execute tests
test:
    cargo test --all-features -- --nocapture

# execute benchmarks
bench:
    cargo bench --all-features

# build docker image with current implementation
docker-build:
    docker build -t delta-search .

# start container as daemon
docker-start:
    docker compose up -d

# stop container
docker-stop:
    docker compose down

# stop and delete container
docker-clean:
    docker compose rm -s -f
