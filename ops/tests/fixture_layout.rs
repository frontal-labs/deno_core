use std::path::Path;

#[test]
fn op2_compile_fixture_suite_has_expected_categories() {
  let fixture_root = Path::new("src/op2/test_cases");
  assert!(fixture_root.join("compiler_pass").is_dir());
  assert!(fixture_root.join("sync").is_dir());
  assert!(fixture_root.join("async").is_dir());
}
