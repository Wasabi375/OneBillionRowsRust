#!/usr/bin/env sh
cargo build --release --bin generator

./target/release/generator $@
