use std::{
    ptr::null_mut,
    sync::atomic::{fence, AtomicPtr, Ordering},
};

use crossbeam_utils::CachePadded;
use haphazard::{Domain, HazardPointer};

struct Node<T> {
    item: Option<T>,
    prev: *mut Node<T>,
    next: AtomicPtr<Node<T>>,
}

impl<T> Node<T> {
    fn sentinel() -> Self {
        Self {
            item: None,
            prev: null_mut(),
            next: AtomicPtr::new(null_mut()),
        }
    }

    fn new(item: T) -> Self {
        Self {
            item: Some(item),
            prev: null_mut(),
            next: AtomicPtr::new(null_mut()),
        }
    }
}

unsafe impl<T: Sync> Sync for Node<T> {}
unsafe impl<T: Sync> Send for Node<T> {}

pub struct DoubleLink<T: Sync + Send> {
    head: CachePadded<AtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
}

pub struct Shield {
    pri: HazardPointer<'static>,
    sub: HazardPointer<'static>,
}

impl Shield {
    pub fn new() -> Self {
        Self {
            pri: HazardPointer::new(),
            sub: HazardPointer::new(),
        }
    }
}

impl<T: Sync + Send> DoubleLink<T> {
    pub fn new() -> Self {
        let sentinel = Box::into_raw(Box::new(Node::sentinel()));
        unsafe { (*sentinel).prev = sentinel };
        Self {
            head: CachePadded::new(AtomicPtr::new(sentinel)),
            tail: CachePadded::new(AtomicPtr::new(sentinel)),
        }
    }

    pub fn enqueue(&self, item: T, shield: &mut Shield) {
        let node = Box::into_raw(Box::new(Node::new(item)));
        let node_mut = unsafe { &mut *node };
        loop {
            let ltail = protect_link(&self.tail, &mut shield.pri);
            let lprev = unsafe { &*ltail }.prev;
            // The author's implementation remove this second protection by using customized HP.
            shield.sub.protect_raw(lprev);
            fence(Ordering::SeqCst);
            if self.tail.load(Ordering::Acquire) != ltail {
                continue;
            }

            let lprev = unsafe { &*lprev };
            node_mut.prev = ltail;
            // Try to help the previous enqueue to complete.
            if lprev.next.load(Ordering::SeqCst).is_null() {
                lprev.next.store(ltail, Ordering::Relaxed);
            }
            if self
                .tail
                .compare_exchange(ltail, node, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                unsafe { &*ltail }.next.store(node, Ordering::Release);
                shield.pri.reset_protection();
                shield.sub.reset_protection();
                return;
            }
        }
    }

    pub fn dequeue<'h>(&self, shield: &'h mut Shield) -> Option<&'h T> {
        loop {
            let lhead = protect_link(&self.head, &mut shield.pri);
            let lnext = unsafe { &*lhead }.next.load(Ordering::Acquire);
            // Check if this queue is empty.
            if lnext.is_null() {
                shield.pri.reset_protection();
                return None;
            }
            // The author's implementation remove this second protection by using customized HP.
            shield.sub.protect_raw(lnext);
            fence(Ordering::SeqCst);
            if self.head.load(Ordering::Acquire) != lhead {
                continue;
            }

            if self
                .head
                .compare_exchange(lhead, lnext, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let item = unsafe { (*lnext).item.as_ref().unwrap() };
                unsafe { Domain::global().retire_ptr::<_, Box<_>>(lhead) };
                shield.pri.reset_protection();
                return Some(item);
            }
        }
    }
}

impl<T: Sync + Send> Drop for DoubleLink<T> {
    fn drop(&mut self) {
        let shield = &mut Shield::new();
        while self.dequeue(shield).is_some() {}
        unsafe { drop(Box::from_raw(self.head.load(Ordering::Relaxed))) };
    }
}

fn protect_link<T>(link: &AtomicPtr<Node<T>>, hazptr: &mut HazardPointer<'static>) -> *mut Node<T> {
    let mut ptr = link.load(Ordering::Relaxed);
    loop {
        hazptr.protect_raw(ptr);
        fence(Ordering::SeqCst);
        let new_ptr = link.load(Ordering::Acquire);
        if ptr == new_ptr {
            return ptr;
        }
        ptr = new_ptr;
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::{DoubleLink, Shield};
    use crossbeam_utils::thread::scope;

    #[test]
    fn simple() {
        let queue = DoubleLink::new();
        let shield = &mut Shield::new();
        assert!(queue.dequeue(shield).is_none());
        queue.enqueue(1, shield);
        queue.enqueue(2, shield);
        queue.enqueue(3, shield);
        assert_eq!(*queue.dequeue(shield).unwrap(), 1);
        assert_eq!(*queue.dequeue(shield).unwrap(), 2);
        assert_eq!(*queue.dequeue(shield).unwrap(), 3);
        assert!(queue.dequeue(shield).is_none());
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
                    let mut shield = Shield::new();
                    for i in 0..ELEMENTS_PER_THREAD {
                        queue.enqueue((t * ELEMENTS_PER_THREAD + i).to_string(), &mut shield);
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
                    let mut shield = Shield::new();
                    for _ in 0..ELEMENTS_PER_THREAD {
                        let res = queue.dequeue(&mut shield).unwrap();
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
