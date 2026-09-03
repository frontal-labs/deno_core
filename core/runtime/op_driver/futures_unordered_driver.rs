// Copyright 2018-2025 the Deno authors. MIT license.

use super::future_arena::FutureAllocation;
use super::future_arena::FutureArena;
use super::op_results::*;
use super::OpDriver;
use super::OpInflightStats;
use crate::OpId;
use crate::PromiseId;
use bit_set::BitSet;
use deno_error::JsErrorClass;
use deno_unsync::UnsyncWaker;
use futures::stream::FuturesUnordered;
use futures::task::noop_waker_ref;
use futures::FutureExt;
use futures::Stream;
use std::cell::Cell;
use std::cell::RefCell;
use std::future::ready;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::ready;
use std::task::Context;
use std::task::Poll;

/// [`OpDriver`] implementation built on a tokio [`JoinSet`].
pub struct FuturesUnorderedDriver<
  C: OpMappingContext + 'static = V8OpMappingContext,
> {
  len: Cell<usize>,
  /// Polled inline from [`OpDriver::poll_ready`], i.e. on whichever thread is
  /// currently executing the isolate -- never from a spawned task.
  ///
  /// This runtime hands an isolate to a blocking-pool thread
  /// (`spawn_blocking_non_send`) while the `spawn_pinned` LocalSet worker that
  /// owns its `LocalSet` keeps running. A background pump therefore polled op
  /// futures on the worker thread while V8 ran the same isolate on the blocking
  /// thread, and both touched one `Rc<RefCell<OpState>>` whose borrow counter is
  /// a non-atomic `Cell<isize>` -- a data race that surfaced as intermittent
  /// "RefCell already borrowed" panics behind `extern "C"` frames, where a panic
  /// cannot unwind and aborts the process.
  ///
  /// Draining here instead makes the exclusion structural: `poll_ready` is
  /// reachable only from the event loop, which holds `&mut` on the runtime, so
  /// the borrow checker guarantees no second thread is inside the isolate.
  results: SubmissionQueueResults<
    FuturesUnordered<FutureAllocation<PendingOp<C>, PendingOpInfo>>,
  >,
  queue: SubmissionQueue<
    FuturesUnordered<FutureAllocation<PendingOp<C>, PendingOpInfo>>,
  >,
  arena: FutureArena<PendingOp<C>, PendingOpInfo>,
}

impl<C: OpMappingContext + 'static> Drop for FuturesUnorderedDriver<C> {
  fn drop(&mut self) {
    self.shutdown()
  }
}

impl<C: OpMappingContext> Default for FuturesUnorderedDriver<C> {
  fn default() -> Self {
    let (queue, results) = new_submission_queue();

    Self {
      len: Default::default(),
      results,
      queue,
      arena: Default::default(),
    }
  }
}

impl<C: OpMappingContext> FuturesUnorderedDriver<C> {
  /// Spawn a polled task inside a [`FutureAllocation`], along with a function that can map it to a [`PendingOp`].
  #[inline(always)]
  fn spawn(&self, task: FutureAllocation<PendingOp<C>, PendingOpInfo>) {
    self.len.set(self.len.get() + 1);
    self.queue.spawn(task);
  }
}

impl<C: OpMappingContext> OpDriver<C> for FuturesUnorderedDriver<C> {
  fn submit_op_fallible<
    R: 'static,
    E: JsErrorClass + 'static,
    const LAZY: bool,
    const DEFERRED: bool,
  >(
    &self,
    op_id: OpId,
    promise_id: i32,
    op: impl Future<Output = Result<R, E>> + 'static,
    rv_map: C::MappingFn<R>,
  ) -> Option<Result<R, E>> {
    {
      let info = PendingOpMappingInfo::<_, _, true>(
        PendingOpInfo(promise_id, op_id),
        rv_map,
      );
      let mut pinned = self.arena.allocate(info, op);

      if LAZY {
        self.spawn(pinned.erase());
        return None;
      }

      // We poll every future here because it's much faster to return a result than
      // spin the event loop to get it.
      match pinned.poll_unpin(&mut Context::from_waker(noop_waker_ref())) {
        Poll::Pending => self.spawn(pinned.erase()),
        Poll::Ready(res) => {
          if DEFERRED {
            drop(pinned);
            self.spawn(self.arena.allocate(info, ready(res)).erase())
          } else {
            return Some(res);
          }
        }
      };

      None
    }
  }

  fn submit_op_infallible<
    R: 'static,
    const LAZY: bool,
    const DEFERRED: bool,
  >(
    &self,
    op_id: OpId,
    promise_id: i32,
    op: impl Future<Output = R> + 'static,
    rv_map: C::MappingFn<R>,
  ) -> Option<R> {
    {
      let info = PendingOpMappingInfo::<_, _, false>(
        PendingOpInfo(promise_id, op_id),
        rv_map,
      );
      let mut pinned = self.arena.allocate(info, op);

      if LAZY {
        self.spawn(pinned.erase());
        return None;
      }

      // We poll every future here because it's much faster to return a result than
      // spin the event loop to get it.
      match Pin::new(&mut pinned)
        .poll(&mut Context::from_waker(noop_waker_ref()))
      {
        Poll::Pending => self.spawn(pinned.erase()),
        Poll::Ready(res) => {
          if DEFERRED {
            drop(pinned);
            self.spawn(self.arena.allocate(info, ready(res)).erase())
          } else {
            return Some(res);
          }
        }
      };

      None
    }
  }

  #[inline(always)]
  fn poll_ready(
    &self,
    cx: &mut Context,
  ) -> Poll<(PromiseId, OpId, OpResult<C>)> {
    // Drive the op futures here rather than from a spawned task. The waker
    // registered by `poll_next_unpin` is the event loop's own, so a completing
    // op wakes the loop directly instead of hopping through a second waker.
    match self.results.poll_next_unpin(cx) {
      Poll::Ready(PendingOp(PendingOpInfo(promise_id, op_id), resp)) => {
        self.len.set(self.len.get() - 1);
        Poll::Ready((promise_id, op_id, resp))
      }
      Poll::Pending => Poll::Pending,
    }
  }

  #[inline(always)]
  fn len(&self) -> usize {
    self.len.get()
  }

  fn shutdown(&self) {
    self.queue.queue.queue.borrow_mut().clear();
    // Also drop anything buffered by a re-entrant submission; previously this
    // was left behind on shutdown.
    self.queue.queue.pending.set(Vec::new());
  }

  fn stats(&self, op_exclusions: &BitSet) -> OpInflightStats {
    let q = self.queue.queue.queue.borrow();
    let mut v: Vec<PendingOpInfo> = Vec::with_capacity(self.len.get());
    for f in q.iter() {
      let context = f.context();
      if !op_exclusions.contains(context.1 as _) {
        v.push(context);
      }
    }
    OpInflightStats {
      ops: v.into_boxed_slice(),
    }
  }
}

impl<F: Future<Output = R>, R> SubmissionQueueFutures for FuturesUnordered<F> {
  type Future = F;
  type Output = F::Output;

  fn len(&self) -> usize {
    self.len()
  }

  fn spawn(&mut self, f: Self::Future) {
    self.push(f)
  }

  fn poll_next_unpin(&mut self, cx: &mut Context) -> Poll<Self::Output> {
    Poll::Ready(ready!(Pin::new(self).poll_next(cx)).unwrap())
  }
}

struct Queue<F: SubmissionQueueFutures> {
  queue: RefCell<F>,
  /// Futures submitted re-entrantly (via [`SubmissionQueue::spawn`]) while
  /// `queue` is already borrowed by [`SubmissionQueueResults::poll_next_unpin`]
  /// — e.g. an op whose completion callback synchronously spawns another op.
  /// Buffered here to avoid a `RefCell` double-borrow, then drained into
  /// `queue` on the next poll.
  ///
  /// `Cell`, not `RefCell`: this buffer exists precisely to absorb re-entrant
  /// submissions, so a borrow conflict *on the buffer itself* is fatal in the
  /// same way the conflict it was added to prevent is fatal — ops are submitted
  /// behind `extern "C"` frames where a panic cannot unwind and aborts the
  /// process. `Cell::take`/`set` cannot panic, and nothing between them can run
  /// user code, so no window exists for a conflicting access to appear.
  pending: Cell<Vec<F::Future>>,
  item_waker: UnsyncWaker,
}

// Manual impl (not `#[derive]`) so the `pending: Vec<F::Future>` field does not
// impose an unnecessary `F::Future: Default` bound. `F: Default` holds via the
// `SubmissionQueueFutures: Default` supertrait.
impl<F: SubmissionQueueFutures> Default for Queue<F> {
  fn default() -> Self {
    Self {
      queue: RefCell::new(F::default()),
      pending: Cell::new(Vec::new()),
      item_waker: UnsyncWaker::default(),
    }
  }
}

pub trait SubmissionQueueFutures: Default {
  type Future: Future<Output = Self::Output>;
  type Output;

  fn len(&self) -> usize;
  fn spawn(&mut self, f: Self::Future);
  fn poll_next_unpin(&mut self, cx: &mut Context) -> Poll<Self::Output>;
}

pub struct SubmissionQueueResults<F: SubmissionQueueFutures> {
  queue: Rc<Queue<F>>,
}

impl<F: SubmissionQueueFutures> SubmissionQueueResults<F> {
  pub fn poll_next_unpin(&self, cx: &mut Context) -> Poll<F::Output> {
    // `try_borrow_mut`, not `borrow_mut`: the borrow below is held across
    // `queue.poll_next_unpin`, which polls arbitrary op futures, and those are
    // free to re-enter this driver. A re-entrant poll finding the queue already
    // borrowed is not a bug to abort on -- the outer poll is draining it and
    // will make the same progress -- but panicking here is fatal, because op
    // futures are polled behind `extern "C"` frames where a panic cannot
    // unwind and takes the process down.
    //
    // Report Pending instead, after registering the waker so `item_waker` (woken
    // by every `SubmissionQueue::spawn`, and by the outer poll's own progress)
    // brings us back.
    let Ok(mut queue) = self.queue.queue.try_borrow_mut() else {
      self.queue.item_waker.register(cx.waker());
      return Poll::Pending;
    };
    // Drain futures that were submitted re-entrantly while `queue` was borrowed
    // during a prior poll.
    //
    // Take the buffer out and drop the borrow *before* spawning, rather than
    // draining in place. `queue.spawn` is a trait method, so this side cannot
    // know whether it re-enters `SubmissionQueue::spawn`; draining in place held
    // `pending` across every one of those calls, and a re-entrant submission
    // then found `queue` borrowed (handled) *and* `pending` borrowed (not), so
    // it aborted on the very buffer added to prevent aborting.
    //
    // Anything submitted while this loop runs lands in the fresh buffer and is
    // drained on the next poll, which `item_waker` has already scheduled.
    let pending = self.queue.pending.take();
    for f in pending {
      queue.spawn(f);
    }
    self.queue.item_waker.register(cx.waker());
    if queue.len() == 0 {
      return Poll::Pending;
    }
    queue.poll_next_unpin(cx)
  }
}

pub struct SubmissionQueue<F: SubmissionQueueFutures> {
  queue: Rc<Queue<F>>,
}

impl<F: SubmissionQueueFutures> SubmissionQueue<F> {
  pub fn spawn(&self, f: F::Future) {
    match self.queue.queue.try_borrow_mut() {
      Ok(mut queue) => queue.spawn(f),
      // Re-entrant submission while the queue is being polled: defer to the
      // pending buffer (drained at the start of the next poll). `wake_by_ref`
      // below guarantees that re-poll happens.
      Err(_) => {
        // `take` + `push` + `set`: `Vec::push` cannot run user code, so nothing
        // can observe or mutate the buffer while it is out of the cell.
        let mut pending = self.queue.pending.take();
        pending.push(f);
        self.queue.pending.set(pending);
      }
    }
    self.queue.item_waker.wake_by_ref();
  }
}

/// Create a [`SubmissionQueue`] and [`SubmissionQueueResults`] that allow for submission of tasks
/// and reception of task results. We may add work to the [`SubmissionQueue`] from any task, and the
/// [`SubmissionQueueResults`] will be polled from a single location.
pub fn new_submission_queue<F: SubmissionQueueFutures>(
) -> (SubmissionQueue<F>, SubmissionQueueResults<F>) {
  let queue: Rc<Queue<F>> = Default::default();
  (
    SubmissionQueue {
      queue: queue.clone(),
    },
    SubmissionQueueResults { queue },
  )
}

#[cfg(test)]
mod submission_queue_tests {
  use super::*;
  use std::future::Ready;

  thread_local! {
    /// Lets the fake queue below call back into the `SubmissionQueue` that owns
    /// it, which is what an op's completion callback does in the real runtime.
    static REENTRY: RefCell<Option<SubmissionQueue<ReentrantQueue>>> =
      const { RefCell::new(None) };
  }

  fn reenter() {
    REENTRY.with(|slot| {
      if let Some(queue) = slot.borrow().as_ref() {
        queue.spawn(ready(()));
      }
    });
  }

  /// A queue that submits one extra future back into the `SubmissionQueue` from
  /// inside each of the two places the driver calls into it. `spawn` is the
  /// interesting one: the driver used to call it while holding a `pending`
  /// borrow.
  #[derive(Default)]
  struct ReentrantQueue {
    inner: FuturesUnordered<Ready<()>>,
    reenter_on_poll: bool,
    reenter_on_spawn: bool,
  }

  impl SubmissionQueueFutures for ReentrantQueue {
    type Future = Ready<()>;
    type Output = ();

    fn len(&self) -> usize {
      self.inner.len()
    }

    fn spawn(&mut self, f: Self::Future) {
      if self.reenter_on_spawn {
        // Once only: this stands in for a completion callback scheduling more
        // work while the driver is draining its pending buffer.
        self.reenter_on_spawn = false;
        reenter();
      }
      self.inner.push(f);
    }

    fn poll_next_unpin(&mut self, cx: &mut Context) -> Poll<Self::Output> {
      if self.reenter_on_poll {
        self.reenter_on_poll = false;
        reenter();
      }
      match Pin::new(&mut self.inner).poll_next(cx) {
        Poll::Ready(Some(())) => Poll::Ready(()),
        _ => Poll::Pending,
      }
    }
  }

  /// Regression: a re-entrant submission that arrives while the driver is
  /// draining `pending` must not abort.
  ///
  /// `poll_next_unpin` holds `queue` for its whole body, so a re-entrant
  /// `spawn` correctly diverts to `pending`. Draining that buffer in place then
  /// held `pending` across every `queue.spawn` call, and the next re-entrant
  /// submission found *both* borrowed — aborting on the very buffer added to
  /// prevent aborting. `RefCell` panics are non-unwinding here, so before the
  /// fix this test killed the process rather than failing.
  #[test]
  fn drain_does_not_hold_pending_across_spawn() {
    let (queue, mut results) = new_submission_queue::<ReentrantQueue>();
    // A second handle onto the same `Queue`, which is what the runtime hands to
    // op callbacks. `SubmissionQueue` is not `Clone`, but the `Rc` is.
    REENTRY.with(|slot| {
      *slot.borrow_mut() = Some(SubmissionQueue {
        queue: results.queue.clone(),
      })
    });

    // First poll: re-enter from inside the poll, so `queue` is borrowed and the
    // submission lands in `pending`.
    queue.spawn(ready(()));
    results.queue.queue.borrow_mut().reenter_on_poll = true;
    let cx = &mut Context::from_waker(noop_waker_ref());
    let _ = results.poll_next_unpin(cx);

    // Second poll: `pending` is non-empty and gets drained. Arm the re-entrant
    // submission to fire from inside `spawn`, i.e. mid-drain.
    results.queue.queue.borrow_mut().reenter_on_spawn = true;
    let _ = results.poll_next_unpin(cx);

    // Reaching here at all is the assertion; the pre-fix driver aborted above.
    // The late submission is still accounted for rather than dropped.
    let _ = results.poll_next_unpin(cx);
    REENTRY.with(|slot| *slot.borrow_mut() = None);
  }
}
