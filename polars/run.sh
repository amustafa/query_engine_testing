RUSTFLAGS='-C target-cpu=native' cargo build --release && /usr/bin/time -l ./target/release/polars-test
