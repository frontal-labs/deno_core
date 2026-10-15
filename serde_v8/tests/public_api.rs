use serde::Deserialize;
use serde::Serialize;

use serde_v8::utils::js_exec;
use serde_v8::utils::v8_do;

#[derive(Debug, Deserialize, Serialize, PartialEq)]
struct PublicPayload {
  name: String,
  values: Vec<u32>,
}

#[test]
fn round_trips_structured_values_through_v8() {
  v8_do(|| {
    let isolate = &mut v8::Isolate::new(v8::CreateParams::default());
    let handle_scope = &mut v8::HandleScope::new(isolate);
    let context = v8::Context::new(handle_scope);
    let scope = &mut v8::ContextScope::new(handle_scope, context);
    let value = js_exec(scope, "({name: 'core', values: [1, 2, 3]})");

    let decoded: PublicPayload = serde_v8::from_v8(scope, value).unwrap();
    assert_eq!(
      decoded,
      PublicPayload {
        name: "core".to_string(),
        values: vec![1, 2, 3],
      }
    );

    let encoded = serde_v8::to_v8(
      scope,
      PublicPayload {
        name: "serde_v8".to_string(),
        values: vec![4, 5],
      },
    )
    .unwrap();
    let json = v8::json::stringify(scope, encoded).unwrap();
    assert_eq!(
      json.to_rust_string_lossy(scope),
      r#"{"name":"serde_v8","values":[4,5]}"#
    );
  });
}
