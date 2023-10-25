mod benchmark;
mod lib;
mod testabc;

use crate::benchmark::run_bench;

fn main() {
    // let a = JiffyQueue::<usize, 3>::new();
    // //     q.push(10);
    // //     q.push(20);
    // //     assert_eq!(q.pop(), Some(10));
    // //     assert_eq!(q.pop(), Some(20));
    // //     assert_eq!(q.pop(), None);

    let mut q = testabc::MpscQueue::new();

    for i in 0..10 {
        q.enqueue(i).unwrap();
        println!("====enqueue finished====");
    }
    for i in 0..10 {
        println!("{:?}", q.dequeue());
    }

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
    // run_bench();
}

// fn main() {
//     run_bench();
// }
