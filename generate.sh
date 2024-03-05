#!/usr/bin/env sh
cargo build --release -p generator

./target/release/generator $@
