#!/usr/bin/env sh
cargo build --release -p generator

echo "generating full data set"
./target/release/generator -p full
echo "generating data set cities400"
./target/release/generator -p cities400
echo "generating test data set"
./target/release/generator -p test

