#!/usr/bin/env sh

set -xe

cd libsql-c

cargo build --target aarch64-apple-darwin --features encryption --release
cargo build --target aarch64-unknown-linux-gnu --features encryption --release
cargo build --target x86_64-unknown-linux-gnu --features encryption --release

rm -rf ../lib

mkdir -p \
  ../lib/x86_64-unknown-linux-gnu \
  ../lib/aarch64-unknown-linux-gnu \
  ../lib/aarch64-apple-darwin \

cp ./libsql.h ../lib/libsql.h
cp ./target/x86_64-unknown-linux-gnu/release/liblibsql.a ../lib/x86_64-unknown-linux-gnu/
cp ./target/aarch64-unknown-linux-gnu/release/liblibsql.a ../lib/aarch64-unknown-linux-gnu/
cp ./target/aarch64-apple-darwin/release/liblibsql.a ../lib/aarch64-apple-darwin/
