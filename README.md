# delta-search

[![CI status](https://github.com/aleics/delta-search/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/aleics/delta-search/actions?query=branch%3Amain)

`delta-search` is a simple database engine created for learning purposes and built in Rust :crab:

:warning: _Do not use in production environments_.

## Usage

To see `delta-search` in action and understand how to use it, check out the different [examples](https://github.com/aleics/delta-search/tree/main/examples).

### Docker
`delta-search` can be run in Docker by building the image:

```shell
docker build -t delta-search .
```

And then starting a container:

```shell
docker run -dp 127.0.0.1:3000:3000 --rm --name delta-search delta-search
```

Or using `docker compose`:

```shell
docker compose up
```

### API
You can execute different operations via REST API to create and update entities, as well as running queries. The API
is available after running `delta-search` via:

```shell
cargo run --release
```

#### REST API

 - `POST /entities/{entity_name}`: define a new entity with a given name.
 - `PUT /data/{entity_name}`: store or update data in bulk in an entity entry.
 - `POST /deltas/{entity_name}`: store deltas with a given context in an entity entry.
 - `PUT /indices/{entity_name}`: create a new index for a given property in an entity entry.
 - `GET /indices/{entity_name}/options`: list the filter options available for the faceted search.
 - `POST /indices/{entity_name}/search`: send a search query for a given entity.

## Motivation

`delta-search` aims to provide simple filtering and sorting capabilities, while allowing to apply temporary _deltas_ in
memory, on top of the existing data stored on disk  using [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). This is especially interesting when  visualizing
potential changes in your data without persisting them yet.
