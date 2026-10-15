# Proc-macro tests

`deno_ops` is a procedural macro crate, so its expansion tests are owned by
`ops/compile_test_runner`. The runner compiles the shared pass/fail fixtures
against the public `op2` macro and keeps generated output checks together.
