# Workspace tests

Integration tests are kept with their owning workspace package:

- `core/tests/` tests the public runtime and module-specifier API.
- `serde_v8/tests/` tests V8 serialization boundaries.
- `ops/compile_test_runner/` owns the proc-macro compile suite.
- `testing/` contains the JavaScript check-in fixtures and runner.

Run all Rust tests with `cargo test --workspace`.
