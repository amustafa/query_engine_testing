#!/bin/sh
RUSTFLAGS='-C target-cpu=native' cargo build --release && /usr/bin/time -l ./target/release/datafusion_playground
