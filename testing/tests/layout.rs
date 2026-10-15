use std::path::Path;

#[test]
fn checkin_fixture_layout_is_present() {
  assert!(Path::new("unit").is_dir());
  assert!(Path::new("integration").is_dir());
  assert!(Path::new("checkin.d.ts").is_file());
}
