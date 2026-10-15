deno_ops_compile_test_runner::prelude!();

#[op2(fast)]
fn op_public_api(value: u32) -> u32 {
  value + 1
}

#[test]
fn compile_test_runner_prelude_is_available_to_integration_tests() {
  main();
  assert_eq!(op_public_api::name(), "op_public_api");
}
