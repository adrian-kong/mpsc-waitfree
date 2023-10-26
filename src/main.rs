mod benchmark;
mod lib;
mod testabc;

// use crate::benchmark::run_bench;
use crate::lib::{channel, JiffyQueue};
use std::sync::atomic::Ordering::Acquire;
use std::sync::mpsc;
use std::time::Instant;

fn main() {
    // let mut queue = JiffyQueue::<u8, 4>::new();
    // for i in 0..7 {
    //     queue.push(1);
    // }
    // println!("finished pushing");
    // println!("{:?}", queue.poll());
    // println!("{:?}", queue.poll());

    // let mut q = testabc::MpscQueue::new();
    // for i in 0..7 {
    //     q.enqueue(i).unwrap();
    // }
    // println!("finished pushing");
    // println!("{:?}", q.dequeue());
    // let mut head_buf = unsafe { &*q.head_of_queue.get() };
    //
    // loop {
    //     let next_head = head_buf.next.load(Acquire);
    //     println!(
    //         "headbuf head={} q_index={} {:?} next={}",
    //         head_buf.head,
    //         head_buf.pos,
    //         head_buf.nodes,
    //         !next_head.is_null(),
    //     );
    //     if next_head.is_null() {
    //         break;
    //     }
    //     head_buf = unsafe { &*next_head };
    // }
    // println!("{:?}", q.dequeue());
    // for i in 0..10 {
    //     q.enqueue(i).unwrap();
    //     println!("====enqueue finished====");
    // }
    // for i in 0..10 {
    //     println!("{:?}", q.dequeue());
    // }

    // let q = testabc::MpscQueue::new();
    //
    // for i in 0..5 * 2 {
    //     let _ = q.enqueue(i);
    // }
    //
    // for i in 0..5 * 2 {
    //     let buffer = i / 5;
    //     let index = i % 5;
    //
    //     if buffer == 0 {
    //         unsafe {
    //             println!(
    //                 "first-buffer {i} {}",
    //                 (*q.head_of_queue.get()).nodes[index].data.assume_init()
    //             );
    //             // assert_eq!(i, (*q.head_of_queue.get()).nodes[index].data.assume_init());
    //         }
    //     } else {
    //         unsafe {
    //             println!(
    //                 "{index} head={} pos={} nodes={}",
    //                 (*q.tail_of_queue.load(SeqCst)).head,
    //                 (*q.tail_of_queue.load(SeqCst)).pos,
    //                 (*q.tail_of_queue.load(SeqCst)).nodes[index]
    //                     .data
    //                     .assume_init()
    //             );
    //             // assert_eq!(
    //             //     i,
    //             //     (*q.tail_of_queue.load(SeqCst)).nodes[index]
    //             //         .data
    //             //         .assume_init()
    //             // );
    //         }
    //     }
    // }

    const SIZE: usize = 10_000_000;
    let now = Instant::now();
    let (tx, rx, queue) = lib::channel::<usize>();
    // let (tx, rx) = riffy::channel();
    let producer1 = tx.clone();
    let a = std::thread::spawn(move || {
        let mut i = 0;
        loop {
            producer1.send(i);
            i += 3;
            if i > SIZE {
                break;
            }
        }
    });
    let producer2 = tx.clone();
    let b = std::thread::spawn(move || {
        let mut i = 1;
        loop {
            producer2.send(i);
            i += 3;
            if i > SIZE {
                break;
            }
        }
    });
    let producer3 = tx.clone();
    let c = std::thread::spawn(move || {
        let mut i = 2;
        loop {
            producer3.send(i);
            i += 3;
            if i > SIZE {
                break;
            }
        }
    });
    a.join().unwrap();
    b.join().unwrap();
    c.join().unwrap();
    println!("{:?}", now.elapsed());
    println!("{:?}", rx.recv().unwrap());
    println!("{:?}", rx.recv().unwrap());
    println!("{:?}", rx.recv().unwrap());
    println!("{:?}", rx.recv().unwrap());
    // println!("{}", queue.len());

    // let mut results = [false; SIZE + 1];
    // let mut recv_value = 0;
    // loop {
    //     let x = rx.recv().unwrap();
    //     if let Some(i) = x {
    //         results[i] = true;
    //         recv_value += 1;
    //         // println!("{}", i);
    //         if recv_value >= SIZE {
    //             // println!("terminating");
    //             break;
    //         }
    //     } else {
    //         println!("received nothing");
    //     }
    // }
    // println!("{:?}", now.elapsed());
    // for i in 0..SIZE {
    //     if !results[i] {
    //         println!("missing {i}");
    //     }
    // }
    // run_bench();
}

// fn main() {
//     run_bench();
// }
