#!/usr/bin/env sh
cargo build --release
cargo test --release
hyperfine --min-runs 5 "./target/release/one-billion-rows data/all_cities.txt"
