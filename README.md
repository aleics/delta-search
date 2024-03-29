# delta-db

[![CI status](https://github.com/aleics/delta-db/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/aleics/delta-db/actions?query=branch%3Amain)

`delta-db` is a simple database engine created for learning purposes and built in Rust :crab:

## Usage

To see `delta-db` in action and understand how to use it, check out the
different [examples](https://github.com/aleics/delta-db/tree/main/examples).

## Motivation

`delta-db` aims to provide simple filtering and sorting capabilities, while allowing to apply temporary _deltas_ in
memory, on top of the existing data stored on disk
using [LMDB](https://en.wikipedia.org/wiki/Lightning_Memory-Mapped_Database). This is especially interesting when
visualizing potential changes in your data without persisting them yet.
