#!/usr/bin/env sh
cargo test

cargo flamegraph --profile flame --bin one-billion-rows -- "$@" data/all_cities.txt
open flamegraph.svg
