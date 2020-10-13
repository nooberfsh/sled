//! The default garbage collector.
//!
//! For each thread, a participant is lazily initialized on its first use, when the current thread
//! is registered in the default collector.  If initialized, the thread's participant will get
//! destructed on thread exit, which in turn unregisters the thread.

use super::collector::{Collector, LocalHandle};
use super::guard::Guard;
use crate::Lazy;

/// The global data for the default garbage collector.
static COLLECTOR: Lazy<Collector, fn() -> Collector> =
    Lazy::new(Collector::new);

thread_local! {
    /// The per-thread participant for the default garbage collector.
    static HANDLE: LocalHandle = COLLECTOR.register();
}

/// Pins the current thread.
#[inline]
pub fn pin() -> Guard {
    with_handle(|handle| handle.pin())
}

#[inline]
fn with_handle<F, R>(mut f: F) -> R
where
    F: FnMut(&LocalHandle) -> R,
{
    HANDLE.try_with(|h| f(h)).unwrap_or_else(|_| f(&COLLECTOR.register()))
}

#[cfg(test)]
mod tests {
    use crossbeam_utils::thread;

    #[test]
    fn pin_while_exiting() {
        struct Foo;

        impl Drop for Foo {
            fn drop(&mut self) {
                // Pin after `HANDLE` has been dropped. This must not panic.
                super::pin();
            }
        }

        thread_local! {
            static FOO: Foo = Foo;
        }

        thread::scope(|scope| {
            scope.spawn(|_| {
                // Initialize `FOO` and then `HANDLE`.
                FOO.with(|_| ());
                super::pin();
                // At thread exit, `HANDLE` gets dropped first and `FOO` second.
            });
        })
        .unwrap();
    }
}
