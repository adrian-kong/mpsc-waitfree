use std::cell::Cell;
use std::fmt::{Debug, Formatter};
use std::mem::MaybeUninit;
use std::ptr::null_mut;
use std::sync::atomic::Ordering::{Acquire, SeqCst};
use std::sync::atomic::{AtomicPtr, AtomicU8, AtomicUsize};
use std::sync::Arc;

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum NodeStatus {
    Empty = 0,
    Set = 1,
    Handled = 2,
}

impl From<u8> for NodeStatus {
    fn from(value: u8) -> Self {
        match value {
            0 => Self::Empty,
            1 => Self::Set,
            2 => Self::Handled,
            _ => panic!("node status not found"),
        }
    }
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

impl<T> Debug for Node<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", NodeStatus::from(self.status.load(Acquire))).unwrap();
        Ok(())
    }
}

impl<T> Node<T> {
    pub fn new() -> Self {
        Self {
            data: MaybeUninit::uninit(),
            status: AtomicU8::default(),
        }
    }

    fn set_data(&mut self, data: T) {
        unsafe { self.data.as_mut_ptr().write(data) }
        self.status.store(NodeStatus::Set as u8, SeqCst);
    }

    fn get_data(&mut self) -> T {
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
    nodes: [Node<T>; N],
    /// Next buffer list
    next: AtomicPtr<BufferList<T, N>>,
    /// Previous buffer list
    prev: *mut BufferList<T, N>,
    /// Index to track position in queue, location of the BufferList in linked list.
    /// i.e. each increment in this index represents a new BufferList.
    queue_index: usize,
}

unsafe impl<T, const N: usize> Send for BufferList<T, N> {}

unsafe impl<T, const N: usize> Sync for BufferList<T, N> {}

impl<T, const N: usize> BufferList<T, N> {
    #[allow(clippy::uninit_assumed_init)]
    pub fn new(queue_index: usize, prev: *mut BufferList<T, N>) -> Self {
        // Initialize using stack allocated memory, reclaimable on thread termination
        let mut array: [Node<T>; N] = unsafe { MaybeUninit::uninit().assume_init() };
        for elem in array.iter_mut() {
            *elem = Node::new();
        }
        Self {
            nodes: array,
            next: AtomicPtr::default(),
            prev,
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
pub struct JiffyQueue<T, const N: usize> {
    /// Head of queue buffer list only mutated by single consumer.
    head_ptr: Cell<*mut BufferList<T, N>>,
    /// Tail of queue buffer list
    tail_ptr: AtomicPtr<BufferList<T, N>>,
    /// The last place to be written to by producers in tail buffer list
    /// Once reaching to end (value equal to N), a new buffer list is created
    insert_index: AtomicUsize,
    /// Index in head ptr to be dequeued. This is bounded between 0 to N.
    /// To be only mutated inside [`poll()`], since single consumer queue.
    dequeue_index: Cell<usize>,
    // /// Garbage collection, only mutated by single consumer during poll operation.
    // gc: VecDeque<*mut BufferList<T, N>>,
}

unsafe impl<T, const N: usize> Send for JiffyQueue<T, N> {}

unsafe impl<T, const N: usize> Sync for JiffyQueue<T, N> {}

impl<T, const N: usize> JiffyQueue<T, N> {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let head = BufferList::<T, N>::new(0, null_mut()).into_mut_ptr();
        Self {
            head_ptr: Cell::new(head),
            tail_ptr: AtomicPtr::new(head),
            insert_index: AtomicUsize::default(),
            dequeue_index: Cell::new(0),
            // gc: VecDeque::default(),
        }
    }

    /// Add tail buffer
    fn add_tail(&self, curr_tail: &mut BufferList<T, N>) {
        let new_tail = BufferList::new(curr_tail.queue_index + 1, curr_tail).into_mut_ptr();
        let next_tail = &curr_tail.next;
        match next_tail.compare_exchange(null_mut(), new_tail, SeqCst, SeqCst) {
            Ok(_) => {
                // store queue tail pointer, only one process should append the new buffer tail
                self.tail_ptr.store(new_tail, SeqCst);
            }
            Err(_) => {
                drop(unsafe { Box::from_raw(new_tail) });
            }
        }
    }

    pub fn push(&self, data: T) {
        // This reserves an element index in queue, we will use this to populate the buffer and
        // formally insert into the queue. insert_at represents the global index with respect to queue
        let insert_at = self.insert_index.fetch_add(1, SeqCst);
        // If this element i in the last buffer
        let mut is_last_buffer = true;

        // Find the current insertion buffer where we will insert the data
        // into the buffer list of the queue.
        let mut curr_buffer = unsafe { &mut *self.tail_ptr.load(Acquire) };
        let mut num_elements = N * curr_buffer.queue_index;

        // if the insertion index is beyond the current number of elements, due to data contention,
        // we need to insert using CAS primitive an additional buffer list at tail
        while insert_at > num_elements {
            if curr_buffer.next.load(Acquire).is_null() {
                self.add_tail(curr_buffer);
            }
            // move to the end of the tail since insertion index is beyond number of elements allocated in queue.
            curr_buffer = unsafe { &mut *self.tail_ptr.load(Acquire) };
            num_elements = N * curr_buffer.queue_index;
        }

        // if we over shoot the location, iterate backwards and find the correct buffer entry index
        let mut lower_buffer_index = N * curr_buffer.queue_index;
        while insert_at < lower_buffer_index {
            curr_buffer = unsafe { &mut *curr_buffer.prev };
            lower_buffer_index = N * curr_buffer.queue_index;
            is_last_buffer = false;
        }

        // We are finally at the location in the buffer list to insert the element in.
        // Since [`insert_at`] corresponds to the global index in the queue, we need to subtract
        // from the lower bound global index of the buffer to get the relative insertion index
        // to place inside the buffer list.
        let relative_index = insert_at - lower_buffer_index;
        let node = &mut curr_buffer.nodes[relative_index];
        if node.status.load(Acquire) == NodeStatus::Empty as u8 {
            node.set_data(data);
            if relative_index == 1 && is_last_buffer {
                self.add_tail(curr_buffer);
            }
        }
    }

    pub fn poll(&self) -> Option<T> {
        let mut head_buf = unsafe { &mut *self.head_ptr.get() };
        // mutate this index after poll, since single threaded - can just make one cell call after.
        let mut i = self.dequeue_index.get();
        // advance to the first non handled item.
        let mut n = &head_buf.nodes[i];
        while n.status.load(Acquire) == NodeStatus::Handled as u8 {
            i += 1;
            // if we read the entire buffer and its all handled, we can safely drop and move on.
            if i >= N {
                let next_head = head_buf.next.load(Acquire);
                drop(unsafe { Box::from_raw(self.head_ptr.get()) });
                self.head_ptr.set(next_head);
                head_buf = unsafe { &mut *self.head_ptr.get() };
                i = 0;
            }
            n = &head_buf.nodes[i];
        }
        self.dequeue_index.set(i);
        if self.reached_end() {
            return None;
        }
        // eliminated handled case, now only have cases where head is empty or set.
        // let us search for the buffer and index inside the buffer where we should pop from
        let (mut found_buf, mut found_index) = match NodeStatus::from(n.status.load(Acquire)) {
            // search from start to end of queue for set element. remove handled buffers along the way.
            NodeStatus::Empty => match self.search(head_buf, i + 1) {
                Some(x) => x,
                None => return None,
            },
            NodeStatus::Set => {
                // if we are on a Set node, we know previous nodes are handled, can simply return this.
                // defers advancing and dropping buffer for next poll iteration for simplicity
                return Some(head_buf.nodes[i].get_data());
            }
            NodeStatus::Handled => panic!("this node should have been handled!"),
        };
        // scan from head to found, if there are any new SET changes will jump to that element and scan again.
        loop {
            (found_buf, found_index) =
                match self.scan(self.head_ptr.get(), i, found_buf, found_index) {
                    Some(x) => x,
                    None => break,
                }
        }
        // caveat in my implementation, even if scan loops enough times to have the node adjacent to head
        // become Set status, it does not handle the buffer replacing. we will defer that to next poll loop for simplicity
        let node = unsafe { &mut (*found_buf).nodes[found_index] };
        let data = node.get_data();
        Some(data)
    }

    /// Fold buffer in the middle of the queue,
    /// given Buffer B, with A - B - C neighbouring buffers,
    /// this becomes A - C and returns prev buffer of C, which is A.
    fn fold(&self, buf_ptr: *mut BufferList<T, N>) -> Option<*mut BufferList<T, N>> {
        let buf = unsafe { &*buf_ptr };
        let next_buf = buf.next.load(Acquire);
        let prev_buf = buf.prev;
        if next_buf.is_null() {
            return None;
        }
        unsafe {
            (*next_buf).prev = prev_buf;
            (*prev_buf).next.store(next_buf, SeqCst);
            drop(Box::from_raw(buf_ptr));
        }
        Some(buf_ptr)
    }

    /// Scan entire queue for nodes with status Set to dequeue between ranges start and end
    /// * `start_index` - Inclusive start index to begin search
    fn scan(
        &self,
        start: *mut BufferList<T, N>,
        start_index: usize,
        end: *mut BufferList<T, N>,
        end_index: usize,
    ) -> Option<(*mut BufferList<T, N>, usize)> {
        let mut curr_ptr = start;
        let mut curr_buf = unsafe { &*curr_ptr };
        let mut start = start_index;
        while curr_ptr != end {
            let mut handled = true;
            for i in start..N {
                match curr_buf.nodes[i].status.load(Acquire).into() {
                    NodeStatus::Set => return Some((curr_ptr, i)),
                    NodeStatus::Empty => handled = false,
                    _ => {}
                }
            }
            if handled {
                curr_ptr = self.fold(curr_ptr).expect("should be middle buffer");
                curr_buf = unsafe { &*curr_ptr };
            }
            curr_ptr = curr_buf.next.load(Acquire);
            curr_buf = unsafe { &*curr_ptr };
            start = 0;
        }
        for i in start_index..end_index {
            if curr_buf.nodes[i].status.load(Acquire) == NodeStatus::Set as u8 {
                return Some((curr_ptr, i));
            }
        }
        None
    }

    /// Search for [`Set`] nodes between start and tail.
    fn search(
        &self,
        start: *mut BufferList<T, N>,
        start_index: usize,
    ) -> Option<(*mut BufferList<T, N>, usize)> {
        let mut curr_ptr = start;
        let mut curr_buf = unsafe { &*curr_ptr };
        let mut start = start_index;
        loop {
            let tail_ptr = self.tail_ptr.load(Acquire);
            let mut handled = true;
            for i in start..N {
                match curr_buf.nodes[i].status.load(Acquire).into() {
                    NodeStatus::Set => return Some((curr_ptr, i)),
                    NodeStatus::Empty => handled = false,
                    _ => {}
                }
            }
            if handled {
                curr_ptr = self.fold(curr_ptr).expect("should be middle buffer");
                curr_buf = unsafe { &*curr_ptr };
            }
            curr_ptr = curr_buf.next.load(Acquire);
            curr_buf = unsafe { &*curr_ptr };
            if tail_ptr == curr_ptr {
                return None;
            }
            start = 0;
        }
    }

    /// naive implementation, we will just loop over entire queue and check how many elements are filled.
    pub fn len(&self) -> usize {
        let mut head_buf = unsafe { &*self.head_ptr.get() };
        let mut cnt = 0;
        loop {
            for i in &head_buf.nodes {
                if i.status.load(Acquire) == NodeStatus::Set as u8 {
                    cnt += 1;
                }
            }
            let head_buf_ptr = head_buf.next.load(Acquire);
            if head_buf_ptr.is_null() {
                break cnt;
            }
            head_buf = unsafe { &*head_buf_ptr };
        }
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Check whether head >= tail overlapping, there are nothing left to iterate on.
    fn reached_end(&self) -> bool {
        let tail_ptr = self.tail_ptr.load(Acquire);
        self.head_ptr.get() == tail_ptr
            && self.dequeue_index.get() >= (self.insert_index.load(Acquire) % N)
    }
}

const DEFAULT_BUFFER_SIZE: usize = 1024;

#[derive(Clone)]
pub struct Sender<T>(Arc<JiffyQueue<T, DEFAULT_BUFFER_SIZE>>);

impl<T> Sender<T> {
    pub fn send(&self, data: T) {
        self.0.push(data);
    }
}

#[derive(Clone)]
pub struct Receiver<T>(Arc<JiffyQueue<T, DEFAULT_BUFFER_SIZE>>);

impl<T> Receiver<T> {
    pub fn recv(&self) -> Result<Option<T>, Box<dyn std::error::Error>> {
        let value = self.0.poll();
        Ok(value)
    }
}

/// return queue just for verification purposes
pub fn channel<T>() -> (
    Sender<T>,
    Receiver<T>,
    Arc<JiffyQueue<T, DEFAULT_BUFFER_SIZE>>,
) {
    let queue = Arc::new(JiffyQueue::new());
    (
        Sender(queue.clone()),
        Receiver(queue.clone()),
        queue.clone(),
    )
}

#[cfg(test)]
mod tests {
    use crate::queue::JiffyQueue;
    use std::sync::atomic::Ordering::Acquire;

    #[test]
    pub fn push_single_buffer() {
        let queue = JiffyQueue::<u8, 4>::new();
        assert!(queue.is_empty());
        queue.push(1);
        assert_eq!(queue.dequeue_index.get(), 0);
        queue.push(2);
        queue.push(3);
        assert!(!queue.is_empty());
        unsafe {
            let buf = &(*queue.head_ptr.get());
            assert_eq!(buf.nodes[0].data.assume_init(), 1);
            assert_eq!(buf.nodes[1].data.assume_init(), 2);
            assert_eq!(buf.nodes[2].data.assume_init(), 3);
        }
    }

    #[test]
    pub fn pop_single_buffer() {
        let queue = JiffyQueue::<u8, 4>::new();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        for i in 0..4 {
            queue.push(i + 1);
            assert_eq!(queue.len(), i as usize + 1);
        }
        for i in 0..4 {
            assert_eq!(queue.poll(), Some(i + 1));
            assert_eq!(queue.len(), 3 - i as usize);
        }
        assert!(queue.is_empty());
    }

    #[test]
    pub fn pop_single_buffer_empty() {
        let queue = JiffyQueue::<u8, 4>::new();
        assert!(queue.is_empty());
        for i in 0..4 {
            queue.push(i + 1);
            assert_eq!(queue.len(), i as usize + 1);
        }
        for i in 0..4 {
            assert_eq!(queue.poll(), Some(i + 1));
            assert_eq!(queue.len(), 3 - i as usize);
        }
        assert!(queue.is_empty());
        assert_eq!(queue.poll(), None);
        assert!(queue.is_empty());
    }

    #[test]
    pub fn pop_multiple_buffers() {
        const BUFFER_SIZE: usize = 4;
        let queue = JiffyQueue::<u8, BUFFER_SIZE>::new();
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        for i in 0..7 {
            queue.push(i + 1);
            assert_eq!(queue.len(), i as usize + 1);
        }
        assert_eq!(queue.len(), 7);
        for i in 0..7 {
            assert_eq!(queue.poll(), Some(i + 1));
            assert_eq!(queue.len(), 7 - (i + 1) as usize);
        }
        assert!(queue.is_empty());
    }

    #[test]
    pub fn test_push() {
        let queue = JiffyQueue::<u8, 4>::new();

        // let head = queue.head;
        // assert!(!head.is_null(), "head is initialized on construction");
        // let head_buf = unsafe { &*head };
        //
        // println!(
        //     "headbuf head={} q_index={} {:?} next={}",
        //     head_buf.head,
        //     head_buf.queue_index,
        //     head_buf.nodes,
        //     head_buf.next.load(Acquire).is_null(),
        // );
        //
        // let tail_buf = unsafe { &*queue.tail.load(Acquire) };
        // println!(
        //     "tailbuf head={} q_index={} {:?} next={}",
        //     tail_buf.head,
        //     tail_buf.queue_index,
        //     tail_buf.nodes,
        //     tail_buf.next.load(Acquire).is_null(),
        // );

        for i in 0..7 {
            assert_eq!(
                queue.insert_index.load(Acquire),
                i as usize,
                "index is insertion order in single producer"
            );
            queue.push(i + 1);
            // unsafe {
            //     println!(
            //         "{:?}",
            //         (*queue.head_ptr).nodes[i as usize % 4].data.assume_init()
            //     );
            // }
            assert_eq!(
                queue.insert_index.load(Acquire),
                i as usize + 1,
                "insertion index should increase after queue push"
            );
            // println!(
            //     "headbuf head={} q_index={} {:?} next={}",
            //     head_buf.head,
            //     head_buf.queue_index,
            //     head_buf.nodes,
            //     !head_buf.next.load(Acquire).is_null(),
            // );
            // let tail_buf = unsafe { &*queue.tail.load(Acquire) };
            // println!(
            //     "tailbuf head={} q_index={} {:?} next={}",
            //     tail_buf.head,
            //     tail_buf.queue_index,
            //     tail_buf.nodes,
            //     tail_buf.next.load(Acquire).is_null(),
            // );
        }

        println!("---done--- listing tail...");
        // let mut tail_buf = unsafe { &*queue.tail.load(Acquire) };
        //
        // loop {
        //     println!(
        //         "tailbuf head={} q_index={} {:?} next={}",
        //         tail_buf.head,
        //         tail_buf.queue_index,
        //         tail_buf.nodes,
        //         tail_buf.next.load(Acquire).is_null(),
        //     );
        //     if tail_buf.prev.is_null() {
        //         break;
        //     }
        //     tail_buf = unsafe { &*tail_buf.prev };
        // }

        println!("---done--- listing head...");
        let mut head_buf = unsafe { &*queue.head_ptr.get() };

        loop {
            let next_head = head_buf.next.load(Acquire);
            println!(
                "headbuf head={} q_index={} {:?} next={}",
                queue.dequeue_index.get(),
                head_buf.queue_index,
                head_buf.nodes,
                !next_head.is_null(),
            );
            if next_head.is_null() {
                break;
            }
            head_buf = unsafe { &*next_head };
        }

        println!("{:?}", queue.poll());
        println!("{:?}", queue.poll());
        println!("{:?}", queue.poll());

        println!("---done--- listing head...");
        let mut head_buf = unsafe { &*queue.head_ptr.get() };

        loop {
            let next_head = head_buf.next.load(Acquire);
            println!(
                "headbuf head={} q_index={} {:?} next={}",
                queue.dequeue_index.get(),
                head_buf.queue_index,
                head_buf.nodes,
                !next_head.is_null(),
            );
            if next_head.is_null() {
                break;
            }
            head_buf = unsafe { &*next_head };
        }
    }
}
