#!/usr/bin/env sh
cargo build --release
cargo test --release

command="./target/release/one-billion-rows"
while [ $# -gt 0 ]; do
    command="${command} $1"
    shift
done
command="${command} data/all_cities.txt"

hyperfine --warmup 1 --min-runs 5 "$command"
