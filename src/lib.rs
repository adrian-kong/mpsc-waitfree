use std::collections::VecDeque;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ptr::null_mut;
use std::sync::atomic::Ordering::{Acquire, Relaxed, Release, SeqCst};
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU8, AtomicUsize, Ordering};

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum NodeStatus {
    Empty = 0,
    Set = 1,
    Handled = 2,
}

/// A Node in Jiffy, to be placed inside a buffer list
struct Node<T> {
    /// The actual data to be enqueued into queue.
    /// We will wrap this using MaybeUninitialized and use [`status`] field to capture whether
    /// this value has been initialized.
    data: MaybeUninit<T>,
    /// This field notifies when the data is available to be read.
    /// Original implementation uses is_set with 3 states: {0:empty, 1:set, 2:handled}
    status: AtomicU8,
}

impl<T> Node<T> {
    pub fn new() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            status: AtomicU8::default(),
        }
    }

    pub fn data(&mut self) -> T {
        // pass the data out of node
        let data = unsafe { self.data.as_ptr().read() };
        self.data = MaybeUninit::uninit();
        self.status.store(NodeStatus::Handled as u8, SeqCst);
        data
    }
}

/// Hiding nodes in cache friendly buffer list
struct BufferList<T, const N: usize> {
    /// The current array of nodes in this buffer list.
    curr_buffer: [Node<T>; N],
    /// Next buffer list
    next: AtomicPtr<BufferList<T, N>>,
    /// Previous buffer list
    prev: *mut BufferList<T, N>,
    /// Head index pointing to last place in this buffer that consumers read
    head: usize,
    /// Index to track position in queue, location of the BufferList in linked list.
    /// i.e. each increment in this index represents a new BufferList.
    queue_index: usize,
}

unsafe impl<T, const N: usize> Send for BufferList<T, N> {}

unsafe impl<T, const N: usize> Sync for BufferList<T, N> {}

impl<T, const N: usize> BufferList<T, N> {
    pub fn new(queue_index: usize, prev: *mut BufferList<T, N>) -> Self {
        // Initialize using stack allocated memory, reclaimable on thread termination
        let mut array: [Node<T>; N] = unsafe { MaybeUninit::uninit().assume_init() };
        for elem in array.iter_mut() {
            *elem = Node::new();
        }
        Self {
            curr_buffer: array,
            next: AtomicPtr::default(),
            prev,
            head: 0,
            queue_index,
        }
    }

    /// Place onto heap, and convert into raw pointer.
    /// This must be manually dropped.
    pub fn into_mut_ptr(self) -> *mut Self {
        Box::into_raw(Box::new(self))
    }
}

/// Implementation of Jiffy MPSC Queue
struct JiffyQueue<T, const N: usize> {
    /// Head of queue buffer list
    head: *mut BufferList<T, N>,
    /// Tail of queue buffer list
    tail: AtomicPtr<BufferList<T, N>>,
    /// The last place to be written to by producers in tail buffer list
    /// Once reaching to end (value equal to N), a new buffer list is created
    insert_index: AtomicUsize,

    /// Garbage collection, only mutated by single consumer during poll operation.
    gc: VecDeque<*mut BufferList<T, N>>,
}

unsafe impl<T, const N: usize> Send for JiffyQueue<T, N> {}

unsafe impl<T, const N: usize> Sync for JiffyQueue<T, N> {}

impl<T, const N: usize> JiffyQueue<T, N> {
    pub fn new() -> Self {
        let head = BufferList::<T, N>::new(0, null_mut()).into_mut_ptr();
        Self {
            head,
            tail: AtomicPtr::new(head),
            insert_index: Default::default(),
            gc: VecDeque::default(),
        }
    }

    /// Add tail buffer
    fn add_tail(&mut self, curr_tail: &mut BufferList<T, N>) {
        let new_tail = BufferList::new(curr_tail.queue_index + 1, curr_tail).into_mut_ptr();
        let next_tail = &curr_tail.next;
        match next_tail.compare_exchange(null_mut(), new_tail, SeqCst, SeqCst) {
            Ok(_prev) => {
                // store queue tail pointer, only one process should append the new buffer tail
                self.tail.store(new_tail, SeqCst);
            }
            Err(_prev) => {
                drop(unsafe { Box::from_raw(new_tail) });
            }
        }
    }

    pub fn push(&mut self, data: T) {
        // This reserves an element index in queue, we will use this to populate the buffer and
        // formally insert into the queue. insert_at represents the global index with respect to queue
        let insert_at = self.insert_index.fetch_add(1, SeqCst);
        // If this element i in the last buffer
        let mut is_last_buffer = true;

        // Find the current insertion buffer where we will insert the data
        // into the buffer list of the queue.
        let mut curr_buffer = unsafe { &mut *self.tail.load(Acquire) };
        let mut num_elements = N * curr_buffer.queue_index;

        // if the insertion index is beyond the current number of elements, due to data contention,
        // we need to insert using CAS primitive an additional buffer list at tail
        while insert_at >= num_elements {
            if curr_buffer.next.load(Acquire).is_null() {
                self.add_tail(curr_buffer);
            }
            // move to the end of the tail since insertion index is beyond number of elements allocated in queue.
            curr_buffer = unsafe { &mut *self.tail.load(Acquire) };
            num_elements = N * curr_buffer.queue_index;
        }

        // if we over shoot the location, iterate backwards and find the correct buffer entry index
        let mut lower_buffer_index = N * (curr_buffer.queue_index - 1);
        while insert_at < lower_buffer_index {
            curr_buffer = unsafe { &mut *curr_buffer.prev };
            lower_buffer_index = N * (curr_buffer.queue_index - 1);
            is_last_buffer = false;
        }

        // We are finally at the location in the buffer list to insert the element in.
        // Since [`insert_at`] corresponds to the global index in the queue, we need to subtract
        // from the lower bound global index of the buffer to get the relative insertion index
        // to place inside the buffer list.
        let relative_index = insert_at - lower_buffer_index;
        let node = &curr_buffer.curr_buffer[relative_index];
        if node.status.load(Acquire) == NodeStatus::Empty as u8 {
            node.status.store(NodeStatus::Set as u8, Relaxed);
            self.add_tail(curr_buffer);
        }
    }

    pub fn poll(&mut self) -> Option<T> {
        let mut head_buf = unsafe { &mut *self.head };
        let mut node = &mut head_buf.curr_buffer[head_buf.head];
        while node.status.load(Acquire) == NodeStatus::Handled as u8 {
            // advance node and head to next element, find the first non handled element.
            head_buf.head += 1;
            // if at the end of buffer list, move head to next buffer
            if !self.try_advance() {
                return None;
            }
            head_buf = unsafe { &mut *self.head };
            node = &mut head_buf.curr_buffer[head_buf.head];
        }
        if self.is_empty() {
            return None;
        }
        let status = node.status.load(Acquire);
        if status == NodeStatus::Set as u8 {
            head_buf.head += 1;
            self.try_advance();
            return Some(node.data());
        }
        if status == NodeStatus::Empty as u8 {
            head_buf = unsafe { &mut *self.head };
            let temp_node = &head_buf.curr_buffer[head_buf.head];
            // if !self.scan(head_buf, temp_node) {
            //     return None;
            // }
            self.rescan(head_buf, head_buf.head, node);
            let curr_head_buf = unsafe { &mut *self.head };
            if self.head == head_buf && curr_head_buf.head == head_buf.head {
                head_buf.head += 1;
                self.try_advance();
            }
            return Some(node.data());
        }
        None
    }

    fn fold(&mut self, temp_head: *mut BufferList<T, N>, temp_head_pos: usize) -> bool {
        if self.tail.load(Acquire) == temp_head {
            return false; // queue is empty
        }
        let temp_head_buf = unsafe { &mut *temp_head };
        let mut next = temp_head_buf.next.load(Acquire);
        let mut prev = temp_head_buf.prev;
        if next.is_null() {
            return false; // nowhere to move to
        }
        unsafe {
            (*next).prev = prev;
            (*prev).next.store(next, SeqCst);
        }
        self.gc.push_back(temp_head);
        true
    }

    /// Advance to next head buffer
    fn try_advance(&mut self) -> bool {
        let head_buf = unsafe { &mut *self.head };
        if self.head == self.tail.load(Acquire) {}
        if head_buf.head >= N {
            if self.head == self.tail.load(Acquire) {
                return false; // head is tail, there is no buffer left to advance to
            }
            let next = head_buf.next.load(Acquire);
            if next.is_null() {
                return false; // there is no next buffer to advance into
            }
            loop {
                let Some(&garbage_ptr) = self.gc.front() else {
                    break;
                };
                unsafe {
                    // if we moved past garbage pointer, we can safely deallocate
                    // since single consumer, this should be ordered and sequential
                    if (*garbage_ptr).queue_index < (*next).queue_index {
                        break;
                    }
                    drop(Box::from_raw(garbage_ptr));
                    self.gc.pop_front();
                }
            }
            drop(Box::from(head_buf));
            self.head = next;
        }
        true
    }

    fn scan(&mut self, temp_head: &mut BufferList<T, N>, node: &Node<T>) -> bool {
        let mut temp_head_pos = temp_head.head;
        let mut all_handled = true;
        let mut advanced_buffer = true;
        while node.status.load(Acquire) == NodeStatus::Set as u8 {
            temp_head_pos += 1;
            if node.status.load(Acquire) != NodeStatus::Handled as u8 {
                all_handled = false;
            }
            if temp_head_pos >= N {
                if all_handled && advanced_buffer {
                    if self.fold(temp_head, temp_head_pos) {
                        return false;
                    }
                } else {
                    let next = temp_head.next.load(Acquire);
                    if next.is_null() {
                        return false;
                    }
                    // temp_head = next;
                }
            }
        }
        true
    }

    fn rescan(&mut self, buf: &mut BufferList<T, N>, buf_head: usize, node: &Node<T>) {}

    pub fn is_empty(&self) -> bool {
        let head_buf = unsafe { &*self.head };
        let tail = unsafe { &mut *self.tail.load(Acquire) };
        let insert_index = self.insert_index.load(Acquire);
        (self.head == tail) && (head_buf.head == insert_index % N)
    }
}
