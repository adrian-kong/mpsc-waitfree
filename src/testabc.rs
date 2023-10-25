//! # riffy
//!
//! riffy is an unbounded, wait-free, multi-producer-single-consumer queue.
//!
//! It's a Rust-port of [Jiffy](https://github.com/DolevAdas/Jiffy)
//! which is implemented in C++ and described in [this arxiv paper](https://arxiv.org/abs/2010.14189).

use core::ptr;
use std::cell::Cell;
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::mem::{ManuallyDrop, MaybeUninit};
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;

use crate::testabc::State::{Empty, Handled, Set};

/// The number of nodes within a single buffer.
pub const BUFFER_SIZE: usize = 5;

/// Represents the state of a node within a buffer.
#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum State {
    /// Initial state, the node contains no data.
    Empty,
    /// The enqueue process was successful, the node contains data.
    Set,
    /// The dequeue process was successful, the node contains no data.
    Handled,
}

impl From<State> for u8 {
    fn from(state: State) -> Self {
        state as u8
    }
}

impl From<u8> for State {
    fn from(state: u8) -> Self {
        match state {
            0 => State::Empty,
            1 => State::Set,
            2 => State::Handled,
            _ => unreachable!(),
        }
    }
}

impl PartialEq<State> for u8 {
    fn eq(&self, other: &State) -> bool {
        *self == *other as u8
    }
}

/// A node is contained in a `BufferList` and owns
/// the actual data that has been enqueued. A node
/// has a state which is updated during enqueue and
/// dequeue operations.
pub struct Node<T> {
    pub(crate) data: MaybeUninit<T>,
    /// The state of the node needs to be atomic to make
    /// state changes visible to the dequeue thread.
    is_set: AtomicU8,
}

impl<T> std::fmt::Debug for Node<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.is_set.load(Acquire)).unwrap();
        Ok(())
    }
}

impl<T> Node<T> {
    /// Load the given data into the node and change its state to `Set`.
    unsafe fn set_data(&mut self, data: T) {
        self.data.as_mut_ptr().write(data);
        self.is_set.store(State::Set.into(), Release);
    }

    fn state(&self) -> State {
        self.is_set.load(Acquire).into()
    }

    fn set_state(&mut self, state: State) {
        self.is_set.store(state.into(), Release)
    }
}

impl<T> Default for Node<T> {
    fn default() -> Self {
        Node {
            data: MaybeUninit::uninit(),
            is_set: AtomicU8::new(State::Empty.into()),
        }
    }
}

/// The buffer list holds a fixed number of nodes.
/// Buffer lists are connected with each other and
/// form a linked list. Enqueue operations always
/// append to the linked list by creating a new buffer
/// and atomically updating the `next` pointer of the
/// last buffer list in the queue.
pub struct BufferList<T> {
    /// A fixed size vector of nodes that hold the data.
    pub(crate) nodes: Vec<Node<T>>,
    /// A pointer to the previous buffer list.
    pub prev: *mut BufferList<T>,
    /// An atomic pointer to the next buffer list.
    pub next: AtomicPtr<BufferList<T>>,
    /// The position to read the next element from inside
    /// the buffer list. The head index is only updated
    /// by the dequeue thread.
    pub head: usize,
    /// The position of that buffer list in the queue.
    /// That index is used to compute the number of elements
    /// previously added to the queue.
    pub pos: usize,
}

impl<T> BufferList<T> {
    pub fn new(size: usize, position_in_queue: usize) -> Self {
        BufferList::with_prev(size, position_in_queue, ptr::null_mut())
    }

    pub fn with_prev(size: usize, pos: usize, prev: *mut BufferList<T>) -> Self {
        let mut curr_buffer = Vec::with_capacity(size);
        curr_buffer.resize_with(size, Node::default);
        BufferList {
            nodes: curr_buffer,
            prev,
            next: AtomicPtr::new(ptr::null_mut()),
            head: 0,
            pos,
        }
    }
}

unsafe impl<T> Send for BufferList<T> {}
unsafe impl<T> Sync for BufferList<T> {}

/// A multi-producer-single-consumer queue.
///
/// # Examples
///
/// ```
/// use riffy::MpscQueue;
///
/// let mut q = MpscQueue::new();
///
/// assert_eq!(q.dequeue(), None);
///
/// q.enqueue(42);
/// q.enqueue(84);
///
/// assert_eq!(q.dequeue(), Some(42));
/// assert_eq!(q.dequeue(), Some(84));
/// assert_eq!(q.dequeue(), None);
/// ```
#[derive(Debug)]
pub struct MpscQueue<T> {
    pub(crate) head_of_queue: Cell<*mut BufferList<T>>,
    pub(crate) tail_of_queue: AtomicPtr<BufferList<T>>,

    buffer_size: usize,
    tail: AtomicUsize,
}

unsafe impl<T> Send for MpscQueue<T> {}
unsafe impl<T> Sync for MpscQueue<T> {}

impl<T> MpscQueue<T> {
    pub fn new() -> Self {
        let head_of_queue = BufferList::new(BUFFER_SIZE, 1);
        let head = Box::new(head_of_queue);
        // forget about it but keep pointer
        let head = Box::into_raw(head);
        // make pointer atomic
        let tail = AtomicPtr::new(head);
        MpscQueue {
            head_of_queue: Cell::new(head),
            tail_of_queue: tail,
            buffer_size: BUFFER_SIZE,
            tail: AtomicUsize::new(0),
        }
    }

    pub fn enqueue(&self, data: T) -> Result<(), T> {
        // Retrieve an index where we insert the new element.
        // Since this is called by multiple enqueue threads,
        // the generated index can be either past or before
        // the current tail buffer of the queue.
        let location = self.tail.fetch_add(1, SeqCst);
        // Track if the element is inserted in the last buffer.
        let mut is_last_buffer = true;
        let mut temp_tail = unsafe { &mut *self.tail_of_queue.load(Acquire) };
        let mut num_elements = self.buffer_size * temp_tail.pos;
        // println!(
        //     "location={location} num_elem={num_elements}, prev_buffer={} pos={}",
        //     self.size_without_buffer(temp_tail),
        //     temp_tail.pos
        // );
        while location >= num_elements {
            let next = temp_tail.next.load(Acquire);
            // println!("{location} {num_elements} {}", next.is_null());
            // create new buffer if reached end of tail buffer
            if next.is_null() {
                let new_buffer_ptr = self.allocate_buffer(temp_tail);
                match temp_tail.next.compare_exchange(
                    ptr::null_mut(),
                    new_buffer_ptr,
                    SeqCst,
                    SeqCst,
                ) {
                    Ok(_) => {
                        // println!("created and moving up");
                        self.tail_of_queue.store(new_buffer_ptr, SeqCst);
                    }
                    Err(new_ptr) => {
                        // println!("failed to create but moving up");
                        MpscQueue::drop_buffer(new_buffer_ptr);
                        // temp_tail = unsafe { &mut *new_ptr };
                    }
                }
            } else {
                // println!("there is already a pointer");
                // let new_tail = unsafe { &mut *next };
                // self.tail_of_queue
                //     .compare_exchange(temp_tail, new_tail, SeqCst, SeqCst)
                //     .ok();
            }
            temp_tail = unsafe { &mut *self.tail_of_queue.load(Acquire) };
            num_elements = self.buffer_size * temp_tail.pos;
        }

        let mut prev_size = self.size_without_buffer(temp_tail);
        while location < prev_size {
            // println!("prev - {} {}", location, prev_size);
            temp_tail = unsafe { &mut *temp_tail.prev };
            // println!("{}", temp_tail.pos);
            prev_size = self.size_without_buffer(temp_tail);
            is_last_buffer = false;
        }

        let n = &mut temp_tail.nodes[location - prev_size];
        if n.is_set.load(Acquire) == Empty {
            unsafe {
                n.set_data(data);
                n.set_state(Set);
            }
            if location - prev_size == 1 && is_last_buffer {
                let new_buffer_ptr = self.allocate_buffer(temp_tail);
                if temp_tail
                    .next
                    .compare_exchange(ptr::null_mut(), new_buffer_ptr, SeqCst, SeqCst)
                    .is_err()
                {
                    MpscQueue::drop_buffer(new_buffer_ptr);
                } else {
                    self.tail_of_queue.store(new_buffer_ptr, SeqCst);
                    // println!("storing new q");
                }
            }
        }

        Ok(())

        // loop {
        //     // The buffer in which we eventually insert into.
        //     let mut temp_tail = unsafe { &mut *self.tail_of_queue.load(Acquire) };
        //     // The number of items in the queue without the current buffer.
        //     let mut prev_size = self.size_without_buffer(temp_tail);
        //     // println!("prev_size={prev_size} pos={} head={} nodes={:?}", temp_tail.pos, temp_tail.head, temp_tail.nodes);
        //
        //     // The location is in a previous buffer. We need to track back to that one.
        //     while location < prev_size {
        //         is_last_buffer = false;
        //         temp_tail = unsafe { &mut *temp_tail.prev };
        //         prev_size -= self.buffer_size;
        //     }
        //
        //     // The current capacity of the queue.
        //     let global_size = self.buffer_size + prev_size;
        //
        //     if prev_size <= location && location < global_size {
        //         // We found the right buffer to insert.
        //         return self.insert(data, location - prev_size, temp_tail, is_last_buffer);
        //     }
        //
        //     // The location is in the next buffer. We need to allocate a new buffer
        //     // which becomes the new tail of the queue.
        //     if location >= global_size {
        //         let next = temp_tail.next.load(Ordering::Acquire);
        //
        //         if next.is_null() {
        //             // No next buffer, allocate a new one.
        //             let new_buffer_ptr = self.allocate_buffer(temp_tail);
        //             // Try setting the successor of the current buffer to point to our new buffer.
        //             if temp_tail
        //                 .next
        //                 .compare_exchange(
        //                     ptr::null_mut(),
        //                     new_buffer_ptr,
        //                     Ordering::SeqCst,
        //                     Ordering::SeqCst,
        //                 )
        //                 .is_ok()
        //             {
        //                 // Only one thread comes here and updates the next pointer.
        //                 temp_tail.next.store(new_buffer_ptr, Ordering::Release)
        //             } else {
        //                 // CAS was unsuccessful, we can drop our buffer.
        //                 // See the insert method for an optimization that
        //                 // reduces contention and wasteful allocations.
        //                 MpscQueue::drop_buffer(new_buffer_ptr)
        //             }
        //         } else {
        //             // If next is not null, we update the tail and proceed on the that buffer.
        //             self.tail_of_queue
        //                 .compare_exchange_weak(
        //                     temp_tail as *mut _,
        //                     next,
        //                     Ordering::SeqCst,
        //                     Ordering::SeqCst,
        //                 )
        //                 .ok();
        //         }
        //     }
        // }
    }

    pub fn dequeue(&self) -> Option<T> {
        // The buffer from which we eventually dequeue from.
        let curr_head = unsafe { &mut *self.head_of_queue.get() };
        let node = &mut curr_head.nodes[curr_head.head];
        while node.is_set.load(Acquire) == Handled {
            curr_head.head += 1;

        }

        let mut temp_tail;
        loop {
            temp_tail = unsafe { &mut *self.tail_of_queue.load(SeqCst) };
            // The number of items in the queue without the current buffer.
            let prev_size = self.size_without_buffer(temp_tail);

            let head = &mut unsafe { &mut *self.head_of_queue.get() }.head;
            let head_is_tail = self.head_of_queue.get() == temp_tail;
            let head_is_empty = *head == self.tail.load(Acquire) - prev_size;

            // The queue is empty.
            if head_is_tail && head_is_empty {
                break None;
            }

            // There are un-handled elements in the current buffer.
            if *head < self.buffer_size {
                let node = &mut unsafe { &mut *self.head_of_queue.get() }.nodes[*head];

                // Check if the node has already been dequeued.
                // If yes, we increment head and move on.
                if node.state() == Handled {
                    *head += 1;
                    continue;
                }

                // The current head points to a node that is not yet set or handled.
                // This means an enqueue thread is still ongoing and we need to find
                // the next set element (and potentially dequeue that one).
                if node.state() == Empty {
                    if let Some(data) = self.search(head, node) {
                        return Some(data);
                    }
                }

                // The current head points to a valid node and can be dequeued.
                if node.state() == Set {
                    // Increment head
                    unsafe { (*self.head_of_queue.get()).head += 1 };
                    let data = MpscQueue::read_data(node);
                    return Some(data);
                }
            }

            // The head buffer has been handled and can be removed.
            // The new head becomes the successor of the current head buffer.
            if *head >= self.buffer_size {
                if self.head_of_queue.get() == self.tail_of_queue.load(Acquire) {
                    // There is only one buffer.
                    return None;
                }

                let next = unsafe { &*self.head_of_queue.get() }.next.load(Acquire);
                if next.is_null() {
                    return None;
                }

                // Drop head buffer.
                MpscQueue::drop_buffer(self.head_of_queue.get());

                self.head_of_queue.set(next);
            }
        }
    }

    fn search(&self, head: &mut usize, node: &mut Node<T>) -> Option<T> {
        let mut temp_buffer = self.head_of_queue.get();
        let mut temp_head = *head;
        // Indicates if we need to continue the search in the next buffer.
        let mut search_next_buffer = false;
        // Indicates if all nodes in the current buffer are handled.
        let mut all_handled = true;

        while node.state() == Empty {
            // There are unhandled elements in the current buffer.
            if temp_head < self.buffer_size {
                // Move forward inside the current buffer.
                let mut temp_node = &mut unsafe { &mut *self.head_of_queue.get() }.nodes[temp_head];
                temp_head += 1;

                // We found a set node which becomes the new candidate for dequeue.
                if temp_node.state() == Set && node.state() == Empty {
                    // We scan from the head of the queue to the new candidate and
                    // check if there has been any node set in the meantime.
                    // If we find a node that is set, that node becomes the new
                    // dequeue candidate and we restart the scan process from the head.
                    // This process continues until there is no change found during scan.
                    // After scanning, `temp_node` stores the candidate node to dequeue.
                    self.scan(node, &mut temp_buffer, &mut temp_head, &mut temp_node);

                    // Check if the actual head has been set in between.
                    if node.state() == Set {
                        break;
                    }

                    // Dequeue the found candidate.
                    let data = MpscQueue::read_data(&mut temp_node);

                    if search_next_buffer && (temp_head - 1) == unsafe { (*temp_buffer).head } {
                        // If we moved to a new buffer, we need to move the head forward so
                        // in the end we can delete the buffer.
                        unsafe { (*temp_buffer).head += 1 };
                    }

                    return Some(data);
                }

                if temp_node.state() == Empty {
                    all_handled = false;
                }
            }

            // We reached the end of the current buffer, move to the next.
            // This also performs a cleanup operation called `fold` that
            // removes buffers in which all elements are handled.
            if temp_head >= self.buffer_size {
                if all_handled && search_next_buffer {
                    // If all nodes in the current buffer are handled, we try to fold the
                    // queue, i.e. remove the intermediate buffer.
                    if self.fold_buffer(&mut temp_buffer, &mut temp_head) {
                        all_handled = true;
                        search_next_buffer = true;
                    } else {
                        // We reached the last buffer in the queue.
                        return None;
                    }
                } else {
                    let next = unsafe { &*temp_buffer }.next.load(Acquire);
                    if next.is_null() {
                        // We reached the last buffer in the queue.
                        return None;
                    }
                    temp_buffer = next;
                    temp_head = unsafe { &*temp_buffer }.head;
                    all_handled = true;
                    search_next_buffer = true;
                }
            }
        }

        None
    }

    fn scan(
        &self,
        // The element at the head of the queue (which is not set yet).
        node: &Node<T>,
        temp_head_of_queue: &mut *mut BufferList<T>,
        temp_head: &mut usize,
        temp_node: &mut &mut Node<T>,
    ) {
        let mut scan_head_of_queue = self.head_of_queue.get();
        let mut scan_head = unsafe { &*scan_head_of_queue }.head;

        while node.state() == Empty && scan_head_of_queue != *temp_head_of_queue
            || scan_head < (*temp_head - 1)
        {
            if scan_head > self.buffer_size {
                // We reached the end of the current buffer and switch to the next.
                scan_head_of_queue = unsafe { (*scan_head_of_queue).next.load(Acquire) };
                scan_head = unsafe { (*scan_head_of_queue).head };
                continue;
            }

            // Move forward inside the current buffer.
            let scan_node = &mut unsafe { &mut *scan_head_of_queue }.nodes[scan_head];
            scan_head += 1;

            if scan_node.state() == Set {
                // We found a new candidate to dequeue and restart
                // the scan from the head of the queue to that element.
                *temp_head = scan_head;
                *temp_head_of_queue = scan_head_of_queue;
                *temp_node = scan_node;

                // re-scan from the beginning of the queue
                scan_head_of_queue = self.head_of_queue.get();
                scan_head = unsafe { &*scan_head_of_queue }.head;
            }
        }
    }

    fn fold_buffer(&self, buffer_ptr: &mut *mut BufferList<T>, buffer_head: &mut usize) -> bool {
        let buffer = unsafe { &**buffer_ptr };

        let next = buffer.next.load(Acquire);
        let prev = buffer.prev;

        if next.is_null() {
            // We reached the last buffer, which cannot be dropped.
            return false;
        }

        unsafe { &mut *next }.prev = prev;
        unsafe { &mut *prev }.next.store(next, Ordering::Release);

        // Drop current buffer
        MpscQueue::drop_buffer(*buffer_ptr);

        // Advance to the next buffer.
        *buffer_ptr = next;
        *buffer_head = unsafe { &mut **buffer_ptr }.head;

        true
    }

    fn size_without_buffer(&self, buffer: &BufferList<T>) -> usize {
        self.buffer_size * (buffer.pos - 1)
    }

    fn insert(
        &self,
        data: T,
        index: usize,
        buffer: &mut BufferList<T>,
        is_last_buffer: bool,
    ) -> Result<(), T> {
        // Insert the element at the right index. This also atomically
        // sets the state of the node to SET to make that change
        // visible to the dequeue thread.
        unsafe {
            buffer.nodes[index].set_data(data);
        }

        // Optimization to reduce contention on the tail of the queue.
        // If the inserted element is the second entry in the
        // current buffer, we already allocate a new buffer and
        // append it to the queue.
        if index == 1 && is_last_buffer {
            let new_buffer_ptr = self.allocate_buffer(buffer);
            if buffer
                .next
                .compare_exchange(
                    ptr::null_mut(),
                    new_buffer_ptr,
                    Ordering::SeqCst,
                    Ordering::SeqCst,
                )
                .is_err()
            {
                MpscQueue::drop_buffer(new_buffer_ptr);
            }
        }

        Ok(())
    }

    fn allocate_buffer(&self, buffer: &mut BufferList<T>) -> *mut BufferList<T> {
        let new_buffer = BufferList::with_prev(self.buffer_size, buffer.pos + 1, buffer as *mut _);

        Box::into_raw(Box::new(new_buffer))
    }

    fn drop_buffer(ptr: *mut BufferList<T>) {
        drop(unsafe { Box::from_raw(ptr) })
    }

    fn read_data(node: &mut Node<T>) -> T {
        // Read the data from the candidate node.
        let data = unsafe { node.data.as_ptr().read() };
        // Replace the data with MaybeUninit to avoid double free in the case
        // where the consumer drops T and the buffer is dropped afterwards.
        node.data = MaybeUninit::uninit();
        // Mark node as handled to avoid double-dequeue.
        node.set_state(Handled);

        data
    }
}

impl<T> Default for MpscQueue<T> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Sender<T> {
    queue: Arc<MpscQueue<T>>,
}

impl<T> Sender<T> {
    fn new(queue: Arc<MpscQueue<T>>) -> Self {
        Sender { queue }
    }

    pub fn send(&self, t: T) -> Result<(), T> {
        self.queue.enqueue(t)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Sender {
            queue: self.queue.clone(),
        }
    }
}

pub struct Receiver<T> {
    queue: Arc<MpscQueue<T>>,
}

impl<T> Receiver<T> {
    fn new(queue: Arc<MpscQueue<T>>) -> Self {
        Receiver { queue }
    }

    pub fn recv(&self) -> Result<Option<T>, Box<dyn Error>> {
        let head = self.queue.dequeue();
        Ok(head)
    }
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let queue = Arc::new(MpscQueue::new());
    (Sender::new(queue.clone()), Receiver::new(queue))
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::sync::Arc;
    use std::thread;

    use super::*;

    #[test]
    fn enqueue() {
        let q = MpscQueue::new();

        unsafe {
            assert_eq!(State::Empty, (*q.head_of_queue.get()).nodes[0].state());
        }
        unsafe {
            assert_eq!(State::Empty, (*q.head_of_queue.get()).nodes[1].state());
        }

        assert_eq!(Ok(()), q.enqueue(42));
        assert_eq!(Ok(()), q.enqueue(43));

        unsafe {
            assert_eq!(42, (*q.head_of_queue.get()).nodes[0].data.assume_init());
        }
        unsafe {
            assert_eq!(State::Set, (*q.head_of_queue.get()).nodes[0].state());
        }
        unsafe {
            assert_eq!(43, (*q.head_of_queue.get()).nodes[1].data.assume_init());
        }
        unsafe {
            assert_eq!(State::Set, (*q.head_of_queue.get()).nodes[1].state());
        }
    }

    #[test]
    fn enqueue_exceeds_buffer() {
        let q = MpscQueue::new();

        for i in 0..BUFFER_SIZE * 2 {
            let _ = q.enqueue(i);
        }

        for i in 0..BUFFER_SIZE * 2 {
            let buffer = i / BUFFER_SIZE;
            let index = i % BUFFER_SIZE;

            if buffer == 0 {
                unsafe {
                    assert_eq!(i, (*q.head_of_queue.get()).nodes[index].data.assume_init());
                }
            } else {
                unsafe {
                    assert_eq!(
                        i,
                        (*q.tail_of_queue.load(Ordering::SeqCst)).nodes[index]
                            .data
                            .assume_init()
                    );
                }
            }
        }
    }

    #[test]
    fn dequeue() {
        let q = MpscQueue::new();

        assert_eq!(None, q.dequeue());
        assert_eq!(Ok(()), q.enqueue(42));
        assert_eq!(Some(42), q.dequeue());
        assert_eq!(None, q.dequeue());
    }

    #[test]
    fn dequeue_exceeds_buffer() {
        let q = MpscQueue::new();

        let size = BUFFER_SIZE * 2.5 as usize;

        for i in 0..size {
            assert_eq!(q.enqueue(i), Ok(()));
        }

        for i in 0..size {
            assert_eq!(q.dequeue(), Some(i));
        }
    }

    #[test]
    fn multi_threaded_direct() {
        // inspired by sync/mpsc/mpsc_queue/tests.rs:14
        let nthreads = 8;
        let nmsgs = 1000;
        let q = MpscQueue::new();

        let q = Arc::new(q);

        let handles = (0..nthreads)
            .map(|_| {
                let q = q.clone();
                thread::spawn(move || {
                    for i in 0..nmsgs {
                        let _ = q.enqueue(i);
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let _ = handle.join();
        }

        let q = Arc::try_unwrap(q).unwrap();

        let mut i = 0;

        while let Some(data) = q.dequeue() {
            i += data;
        }

        let expected = (0..1000).sum::<i32>() * nthreads;

        assert_eq!(i, expected)
    }

    #[test]
    fn multi_threaded_channel() {
        let nthreads = 8;
        let nmsgs = 1000;

        let (tx, rx) = channel::<i32>();

        let handles = (0..nthreads)
            .map(|_| {
                let tx = tx.clone();
                thread::spawn(move || {
                    for i in 0..nmsgs {
                        let _ = tx.send(i).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            let _ = handle.join();
        }

        let mut i = 0;

        while let Some(data) = rx.recv().unwrap() {
            i += data;
        }

        let expected = (0..1000).sum::<i32>() * nthreads;

        assert_eq!(i, expected)
    }
}
