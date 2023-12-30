use std::sync::atomic::Ordering;

use crossbeam_epoch::{unprotected, Atomic, Guard, Owned};
use crossbeam_utils::CachePadded;

struct Node<T> {
    item: Option<T>,
    prev: Atomic<Node<T>>,
    next: Atomic<Node<T>>,
}

impl<T> Node<T> {
    fn sentinel() -> Self {
        Self {
            item: None,
            prev: Atomic::null(),
            next: Atomic::null(),
        }
    }

    fn new(item: T) -> Self {
        Self {
            item: Some(item),
            prev: Atomic::null(),
            next: Atomic::null(),
        }
    }
}

unsafe impl<T: Sync> Sync for Node<T> {}
unsafe impl<T: Sync> Send for Node<T> {}

pub struct DoubleLink<T: Sync + Send> {
    head: CachePadded<Atomic<Node<T>>>,
    tail: CachePadded<Atomic<Node<T>>>,
}

impl<T: Sync + Send> DoubleLink<T> {
    pub fn new() -> Self {
        let sentinel = Owned::new(Node::sentinel()).into_shared(unsafe { unprotected() });
        unsafe { sentinel.deref().prev.store(sentinel, Ordering::Relaxed) };
        Self {
            head: CachePadded::new(Atomic::from(sentinel)),
            tail: CachePadded::new(Atomic::from(sentinel)),
        }
    }

    pub fn enqueue(&self, item: T, guard: &Guard) {
        let node = Owned::new(Node::new(item)).into_shared(guard);
        loop {
            let ltail = self.tail.load(Ordering::Acquire, guard);
            let lprev = unsafe { ltail.deref().prev.load(Ordering::Relaxed, guard).deref() };
            unsafe { node.deref() }.prev.store(ltail, Ordering::Relaxed);
            // Try to help the previous enqueue to complete.
            if lprev.next.load(Ordering::SeqCst, guard).is_null() {
                lprev.next.store(ltail, Ordering::Relaxed);
            }
            if self
                .tail
                .compare_exchange(ltail, node, Ordering::SeqCst, Ordering::SeqCst, guard)
                .is_ok()
            {
                unsafe { ltail.deref() }.next.store(node, Ordering::Release);
                return;
            }
        }
    }

    pub fn dequeue<'g>(&self, guard: &'g Guard) -> Option<&'g T> {
        loop {
            let lhead = self.head.load(Ordering::Acquire, guard);
            let lnext = unsafe { lhead.deref().next.load(Ordering::Acquire, guard) };
            // Check if this queue is empty.
            if lnext.is_null() {
                return None;
            }

            if self
                .head
                .compare_exchange(lhead, lnext, Ordering::SeqCst, Ordering::SeqCst, guard)
                .is_ok()
            {
                let item = unsafe { lnext.deref().item.as_ref().unwrap() };
                unsafe { guard.defer_destroy(lhead) };
                return Some(item);
            }
        }
    }
}

impl<T: Sync + Send> Drop for DoubleLink<T> {
    fn drop(&mut self) {
        while self.dequeue(unsafe { unprotected() }).is_some() {}
        unsafe {
            drop(
                self.head
                    .load(Ordering::Relaxed, unprotected())
                    .into_owned(),
            )
        };
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::DoubleLink;
    use crossbeam_epoch::pin;
    use crossbeam_utils::thread::scope;

    #[test]
    fn simple() {
        let queue = DoubleLink::new();
        let guard = &pin();
        assert!(queue.dequeue(guard).is_none());
        queue.enqueue(1, guard);
        queue.enqueue(2, guard);
        queue.enqueue(3, guard);
        assert_eq!(*queue.dequeue(guard).unwrap(), 1);
        assert_eq!(*queue.dequeue(guard).unwrap(), 2);
        assert_eq!(*queue.dequeue(guard).unwrap(), 3);
        assert!(queue.dequeue(guard).is_none());
    }

    #[test]
    fn smoke() {
        const THREADS: usize = 100;
        const ELEMENTS_PER_THREAD: usize = 10000;

        let queue = DoubleLink::new();
        let mut found = Vec::new();
        found.resize_with(THREADS * ELEMENTS_PER_THREAD, || AtomicU32::new(0));

        scope(|s| {
            for t in 0..THREADS {
                let queue = &queue;
                s.spawn(move |_| {
                    for i in 0..ELEMENTS_PER_THREAD {
                        queue.enqueue((t * ELEMENTS_PER_THREAD + i).to_string(), &pin());
                    }
                });
            }
        })
        .unwrap();

        scope(|s| {
            for _ in 0..THREADS {
                let queue = &queue;
                let found = &found;
                s.spawn(move |_| {
                    for _ in 0..ELEMENTS_PER_THREAD {
                        let guard = pin();
                        let res = queue.dequeue(&guard).unwrap();
                        assert_eq!(
                            found[res.parse::<usize>().unwrap()].fetch_add(1, Ordering::Relaxed),
                            0
                        );
                    }
                });
            }
        })
        .unwrap();

        assert!(
            found
                .iter()
                .filter(|v| v.load(Ordering::Relaxed) == 0)
                .count()
                == 0
        );
    }
}
fn main() {}
