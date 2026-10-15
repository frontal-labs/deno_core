# Contributing

Thanks for helping improve `deno_core`.

## Before opening a pull request

- Keep changes focused and preserve the crate's public API unless the change is intentional.
- Add or update tests for behavior changes.
- Update documentation when a public behavior or workflow changes.
- Do not commit generated `target/` output.

## Local checks

Use the toolchain in `rust-toolchain.toml` when present. Before submitting a pull request, run:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D clippy::all
cargo test --all-targets --all-features
```

This repository also contains Deno tooling. Run `deno fmt --check` and `deno lint` when changing TypeScript or JavaScript files.

## Pull requests

Describe the motivation, the user-visible effect, and the validation performed. Keep refactors separate from behavior changes where practical so they are easier to review.

