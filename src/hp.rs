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

pub struct DoubleLink<T> {
    head: CachePadded<AtomicPtr<Node<T>>>,
    tail: CachePadded<AtomicPtr<Node<T>>>,
}

pub struct Holder {
    pri: HazardPointer<'static>,
    sub: HazardPointer<'static>,
}

impl Holder {
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

    pub fn enqueue(&self, item: T, holder: &mut Holder) {
        let node = Box::into_raw(Box::new(Node::new(item)));
        let node_mut = unsafe { &mut *node };
        loop {
            let ltail = protect_link(&self.tail, &mut holder.pri);
            let lprev = unsafe { &*ltail }.prev;
            // The author's implementation remove this second protection by using customized HP.
            holder.sub.protect_raw(lprev);
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
                holder.pri.reset_protection();
                holder.sub.reset_protection();
                return;
            }
        }
    }

    pub fn dequeue<'h>(&self, holder: &'h mut Holder) -> Option<&'h T> {
        loop {
            let lhead = protect_link(&self.head, &mut holder.pri);
            let lnext = unsafe { &*lhead }.next.load(Ordering::Acquire);
            // Check if this queue is empty.
            if lnext.is_null() {
                holder.pri.reset_protection();
                return None;
            }
            // The author's implementation remove this second protection by using customized HP.
            holder.sub.protect_raw(lnext);
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
                holder.pri.reset_protection();
                return Some(item);
            }
        }
    }
}

impl<T> Drop for DoubleLink<T> {
    fn drop(&mut self) {
        
    }
}

fn protect_link<T>(link: &AtomicPtr<Node<T>>, hazptr: &mut HazardPointer<'static>) -> *mut Node<T> {
    let mut ptr = link.load(Ordering::Relaxed);
    loop {
        hazptr.protect_raw(ptr);
        let new_ptr = link.load(Ordering::Acquire);
        if ptr == new_ptr {
            return ptr;
        }
        ptr = new_ptr;
    }
}

#[test]
fn simple() {
    let queue = DoubleLink::new();
    let holder = &mut Holder::new();
    assert!(queue.dequeue(holder).is_none());
    queue.enqueue(1, holder);
    queue.enqueue(2, holder);
    queue.enqueue(3, holder);
    assert_eq!(*queue.dequeue(holder).unwrap(), 1);
    assert_eq!(*queue.dequeue(holder).unwrap(), 2);
    assert_eq!(*queue.dequeue(holder).unwrap(), 3);
    assert!(queue.dequeue(holder).is_none());
}
