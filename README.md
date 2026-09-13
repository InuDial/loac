# loac

`loac` is a typed local actor runtime for Rust.

This repository contains two crates:

- [`loac`](crates/loac), the actor runtime;
- [`loac-macros`](crates/loac-macros), its procedural macros.

See the [runtime guide](crates/loac/README.md) for usage.

## Development

Run these checks before submitting changes:

```bash
cargo fmt --all -- --check
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
cargo doc --workspace --no-deps --locked
```

See the [release guide](docs/development/releasing.md) for publishing.

## License

LOAC is licensed under the [MIT License](LICENSE-MIT).
