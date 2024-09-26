#!/usr/bin/env sh

set -xe

cd libsql-c

cargo zigbuild --target aarch64-apple-darwin --release
cargo zigbuild --target x86_64-apple-darwin --release
RUSTFLAGS="-C target-feature=-crt-static" cargo zigbuild --target aarch64-unknown-linux-musl --release
RUSTFLAGS="-C target-feature=-crt-static" cargo zigbuild --target x86_64-unknown-linux-musl --release

rm -rf ../lib

mkdir -p \
  ../lib/aarch64-unknown-linux-musl \
  ../lib/x86_64-unknown-linux-musl \
  ../lib/aarch64-apple-darwin \
  ../lib/x86_64-apple-darwin \

cp ./libsql.h ../lib/libsql.h
cp ./target/x86_64-unknown-linux-musl/release/liblibsql.a ../lib/x86_64-unknown-linux-musl/
cp ./target/aarch64-unknown-linux-musl/release/liblibsql.a ../lib/aarch64-unknown-linux-musl/
cp ./target/x86_64-apple-darwin/release/liblibsql.a ../lib/x86_64-apple-darwin/
cp ./target/aarch64-apple-darwin/release/liblibsql.a ../lib/aarch64-apple-darwin/
