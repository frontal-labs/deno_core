use std::path::Path;

use deno_core::normalize_path;
use deno_core::resolve_import;
use deno_core::resolve_path;
use deno_core::v8_version;
use deno_core::ModuleSpecifier;

#[test]
fn resolves_absolute_and_relative_module_specifiers() {
  let absolute: ModuleSpecifier =
    deno_core::resolve_url("https://example.com/mod.ts").unwrap();
  assert_eq!(absolute.as_str(), "https://example.com/mod.ts");

  let relative = resolve_import("./dep.ts", "file:///project/mod.ts").unwrap();
  assert_eq!(relative.as_str(), "file:///project/dep.ts");

  let path = resolve_path("src/../mod.ts", Path::new("/project")).unwrap();
  assert_eq!(path.as_str(), "file:///project/mod.ts");
}

#[test]
fn normalizes_paths_and_exposes_v8_version() {
  assert_eq!(
    normalize_path("/project/./src/../mod.ts"),
    std::path::PathBuf::from("/project/mod.ts")
  );
  assert!(!v8_version().is_empty());
}
