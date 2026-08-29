// Copyright 2018-2025 the Deno authors. MIT license.

use crate::error::JsStackFrame;
use crate::gotham_state::GothamState;
use crate::io::ResourceTable;
use crate::ops_metrics::OpMetricsFn;
use crate::runtime::JsRuntimeState;
use crate::runtime::OpDriverImpl;
use crate::FeatureChecker;
use crate::OpDecl;
use futures::task::AtomicWaker;
use std::cell::RefCell;
use std::ops::Deref;
use std::ops::DerefMut;
use std::rc::Rc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use v8::fast_api::CFunction;
use v8::Isolate;

pub type PromiseId = i32;
pub type OpId = u16;

#[cfg(debug_assertions)]
thread_local! {
  static CURRENT_OP: std::cell::Cell<Option<&'static OpDecl>> = None.into();
  /// Every op frame currently on this thread's stack, *including* the
  /// `#[op2(reentrant)]` ones that `CURRENT_OP` deliberately does not record.
  /// Diagnostics only: it is what names the holder when an op state borrow
  /// conflicts, which the panic alone cannot do (the failing op's backtrace
  /// dead-ends at V8's JIT frames).
  static OP_STACK: RefCell<Vec<(&'static str, std::thread::ThreadId)>> =
    const { RefCell::new(Vec::new()) };
}

#[cfg(debug_assertions)]
pub struct ReentrancyGuard {
  /// Whether this frame is the one that set `CURRENT_OP`.
  blocking: bool,
}

#[cfg(debug_assertions)]
impl Drop for ReentrancyGuard {
  fn drop(&mut self) {
    if self.blocking {
      CURRENT_OP.with(|f| f.set(None));
    }
    let _ = OP_STACK.try_with(|f| {
      if let Ok(mut stack) = f.try_borrow_mut() {
        stack.pop();
      }
    });
  }
}

/// Creates an op re-entrancy check for the given [`OpDecl`].
#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn reentrancy_check(decl: &'static OpDecl) -> Option<ReentrancyGuard> {
  // Reentrant ops still get a frame pushed, but must not arm the check --
  // being able to invoke ops from inside them is the point of the marker.
  let blocking = !decl.is_reentrant;
  if blocking {
    let current = CURRENT_OP.with(|f| f.get());
    if let Some(current) = current {
      panic!("op {} was not marked as #[op2(reentrant)], but re-entrantly invoked op {}", current.name, decl.name);
    }
    CURRENT_OP.with(|f| f.set(Some(decl)));
  }
  let _ = OP_STACK.try_with(|f| {
    if let Ok(mut stack) = f.try_borrow_mut() {
      stack.push((decl.name, std::thread::current().id()));
    }
  });
  Some(ReentrancyGuard { blocking })
}

/// Renders this thread's op frames, outermost first.
#[doc(hidden)]
pub fn op_stack_description() -> String {
  #[cfg(debug_assertions)]
  {
    OP_STACK
      .try_with(|f| match f.try_borrow() {
        Ok(stack) if stack.is_empty() => "<no op frames>".to_string(),
        Ok(stack) => stack
          .iter()
          .map(|(name, thread)| format!("{name} on {thread:?}"))
          .collect::<Vec<_>>()
          .join(" -> "),
        Err(_) => "<op stack unavailable>".to_string(),
      })
      .unwrap_or_else(|_| "<op stack unavailable>".to_string())
  }
  #[cfg(not(debug_assertions))]
  {
    "<only tracked in debug builds>".to_string()
  }
}

/// Borrows the op state for an op's generated wrapper.
///
/// Identical to `RefCell::borrow` except that a conflict reports which op
/// frames are live on this thread. The bare `RefCell` panic cannot: ops are
/// invoked through `extern "C"` frames, so the panic aborts and its backtrace
/// stops at `Builtins_CallApiCallbackGeneric` without ever reaching the Rust
/// frame that holds the conflicting borrow.
#[doc(hidden)]
#[inline(always)]
pub fn borrow_op_state(state: &RefCell<OpState>) -> std::cell::Ref<'_, OpState> {
  match state.try_borrow() {
    Ok(state) => state,
    Err(_) => op_state_borrow_failed("shared"),
  }
}

/// Mutable counterpart of [`borrow_op_state`].
#[doc(hidden)]
#[inline(always)]
pub fn borrow_op_state_mut(
  state: &RefCell<OpState>,
) -> std::cell::RefMut<'_, OpState> {
  match state.try_borrow_mut() {
    Ok(state) => state,
    Err(_) => op_state_borrow_failed("mutable"),
  }
}

#[doc(hidden)]
#[cold]
#[inline(never)]
fn op_state_borrow_failed(kind: &str) -> ! {
  panic!(
    "op state is already borrowed; this op wanted a {kind} borrow on {:?}. Live op frames: {}",
    std::thread::current().id(),
    op_stack_description()
  );
}

#[derive(Clone, Copy)]
pub struct OpMetadata {
  /// A description of the op for use in sanitizer output.
  pub sanitizer_details: Option<&'static str>,
  /// The fix for the issue described in `sanitizer_details`.
  pub sanitizer_fix: Option<&'static str>,
}

impl OpMetadata {
  pub const fn default() -> Self {
    Self {
      sanitizer_details: None,
      sanitizer_fix: None,
    }
  }
}

/// Per-op context.
///
// Note: We don't worry too much about the size of this struct because it's allocated once per realm, and is
// stored in a contiguous array.
pub struct OpCtx {
  /// The id for this op. Will be identical across realms.
  pub id: OpId,

  /// A stashed Isolate that ops can make use of. This is a raw isolate pointer, and as such, is
  /// extremely dangerous to use.
  pub isolate: *mut Isolate,

  #[doc(hidden)]
  pub state: Rc<RefCell<OpState>>,
  #[doc(hidden)]
  pub enable_stack_trace: bool,

  pub(crate) decl: OpDecl,
  pub(crate) fast_fn_info: Option<CFunction>,
  pub(crate) metrics_fn: Option<OpMetricsFn>,

  op_driver: Rc<OpDriverImpl>,
  runtime_state: *const JsRuntimeState,
}

impl OpCtx {
  #[allow(clippy::too_many_arguments)]
  pub(crate) fn new(
    id: OpId,
    isolate: *mut Isolate,
    op_driver: Rc<OpDriverImpl>,
    decl: OpDecl,
    state: Rc<RefCell<OpState>>,
    runtime_state: *const JsRuntimeState,
    metrics_fn: Option<OpMetricsFn>,
    enable_stack_trace: bool,
  ) -> Self {
    // If we want metrics for this function, create the fastcall `CFunctionInfo` from the metrics
    // `CFunction`. For some extremely fast ops, the parameter list may change for the metrics
    // version and require a slightly different set of arguments (for example, it may need the fastcall
    // callback information to get the `OpCtx`).
    let fast_fn_info = if metrics_fn.is_some() {
      decl.fast_fn_with_metrics
    } else {
      decl.fast_fn
    };

    Self {
      id,
      state,
      runtime_state,
      decl,
      op_driver,
      fast_fn_info,
      isolate,
      metrics_fn,
      enable_stack_trace,
    }
  }

  #[inline(always)]
  pub const fn decl(&self) -> &OpDecl {
    &self.decl
  }

  #[inline(always)]
  pub const fn metrics_enabled(&self) -> bool {
    self.metrics_fn.is_some()
  }

  /// Generates four external references for each op. If an op does not have a fastcall, it generates
  /// "null" slots to avoid changing the size of the external references array.
  pub const fn external_references(&self) -> [v8::ExternalReference; 4] {
    extern "C" fn placeholder() {}

    let ctx_ptr = v8::ExternalReference {
      pointer: self as *const OpCtx as _,
    };
    let null = v8::ExternalReference {
      pointer: placeholder as _,
    };

    if self.metrics_enabled() {
      let slow_fn = v8::ExternalReference {
        function: self.decl.slow_fn_with_metrics,
      };
      if let (Some(fast_fn), Some(fast_fn_info)) =
        (self.decl.fast_fn_with_metrics, self.fast_fn_info)
      {
        let fast_fn = v8::ExternalReference {
          pointer: fast_fn.address() as _,
        };
        let fast_info = v8::ExternalReference {
          type_info: fast_fn_info.type_info(),
        };
        [ctx_ptr, slow_fn, fast_fn, fast_info]
      } else {
        [ctx_ptr, slow_fn, null, null]
      }
    } else {
      let slow_fn = v8::ExternalReference {
        function: self.decl.slow_fn,
      };
      if let (Some(fast_fn), Some(fast_fn_info)) =
        (self.decl.fast_fn, self.fast_fn_info)
      {
        let fast_fn = v8::ExternalReference {
          pointer: fast_fn.address() as _,
        };
        let fast_info = v8::ExternalReference {
          type_info: fast_fn_info.type_info(),
        };
        [ctx_ptr, slow_fn, fast_fn, fast_info]
      } else {
        [ctx_ptr, slow_fn, null, null]
      }
    }
  }

  pub(crate) fn op_driver(&self) -> &OpDriverImpl {
    &self.op_driver
  }

  /// Get the [`JsRuntimeState`] for this op.
  pub(crate) fn runtime_state(&self) -> &JsRuntimeState {
    // SAFETY: JsRuntimeState outlives OpCtx
    unsafe { &*self.runtime_state }
  }
}

/// Allows an embedder to track operations which should
/// keep the event loop alive.
#[derive(Debug, Clone)]
pub struct ExternalOpsTracker {
  counter: Arc<AtomicUsize>,
}

impl ExternalOpsTracker {
  pub fn ref_op(&self) {
    self.counter.fetch_add(1, Ordering::Relaxed);
  }

  pub fn unref_op(&self) {
    let _ =
      self
        .counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |x| {
          if x == 0 {
            None
          } else {
            Some(x - 1)
          }
        });
  }

  pub(crate) fn has_pending_ops(&self) -> bool {
    self.counter.load(Ordering::Relaxed) > 0
  }
}

pub type OpStackTraceCallback = Box<dyn Fn(Vec<JsStackFrame>)>;

/// Maintains the resources and ops inside a JS runtime.
pub struct OpState {
  pub resource_table: ResourceTable,
  pub(crate) gotham_state: GothamState,
  pub waker: Arc<AtomicWaker>,
  pub feature_checker: Arc<FeatureChecker>,
  pub external_ops_tracker: ExternalOpsTracker,
  pub op_stack_trace_callback: Option<OpStackTraceCallback>,
}

impl OpState {
  pub fn new(
    maybe_feature_checker: Option<Arc<FeatureChecker>>,
    op_stack_trace_callback: Option<OpStackTraceCallback>,
  ) -> OpState {
    OpState {
      resource_table: Default::default(),
      gotham_state: Default::default(),
      waker: Arc::new(AtomicWaker::new()),
      feature_checker: maybe_feature_checker.unwrap_or_default(),
      external_ops_tracker: ExternalOpsTracker {
        counter: Arc::new(AtomicUsize::new(0)),
      },
      op_stack_trace_callback,
    }
  }

  /// Clear all user-provided resources and state.
  pub(crate) fn clear(&mut self) {
    std::mem::take(&mut self.gotham_state);
    std::mem::take(&mut self.resource_table);
  }
}

impl Deref for OpState {
  type Target = GothamState;

  fn deref(&self) -> &Self::Target {
    &self.gotham_state
  }
}

impl DerefMut for OpState {
  fn deref_mut(&mut self) -> &mut Self::Target {
    &mut self.gotham_state
  }
}
