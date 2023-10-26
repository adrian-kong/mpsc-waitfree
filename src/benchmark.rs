// use std::sync::mpsc;
//
// use crate::testabc;
// use std::fmt;
//
// const LEN: usize = 1;
//
// #[derive(Clone, Copy)]
// pub struct Message(pub [usize; LEN]);
//
// impl fmt::Debug for Message {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.pad("Message")
//     }
// }
//
// #[inline]
// pub fn new_message(num: usize) -> Message {
//     Message([num; LEN])
// }
//
// const MESSAGES: usize = 5_000_000;
// const THREADS: usize = 4;
//
// pub fn shuffle<T>(v: &mut [T]) {
//     use std::cell::Cell;
//     use std::num::Wrapping;
//
//     let len = v.len();
//     if len <= 1 {
//         return;
//     }
//
//     thread_local! {
//         static RNG: Cell<Wrapping<u32>> = Cell::new(Wrapping(1));
//     }
//
//     RNG.with(|rng| {
//         for i in 1..len {
//             // This is the 32-bit variant of Xorshift.
//             // https://en.wikipedia.org/wiki/Xorshift
//             let mut x = rng.get();
//             x ^= x << 13;
//             x ^= x >> 17;
//             x ^= x << 5;
//             rng.set(x);
//
//             let x = x.0;
//             let n = i + 1;
//
//             // This is a fast alternative to `let j = x % n`.
//             // https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/
//             let j = ((x as u64).wrapping_mul(n as u64) >> 32) as u32 as usize;
//
//             v.swap(i, j);
//         }
//     });
// }
//
// fn seq_async() {
//     let (tx, rx) = crate::lib::channel();
//
//     for i in 0..MESSAGES {
//         tx.send(new_message(i));
//     }
//
//     for _ in 0..MESSAGES {
//         rx.recv().unwrap();
//     }
// }
//
// fn spsc_async() {
//     let (tx, rx) = crate::lib::channel();
//
//     crossbeam::scope(|scope| {
//         scope.spawn(move |_| {
//             for i in 0..MESSAGES {
//                 tx.send(new_message(i));
//             }
//         });
//
//         for _ in 0..MESSAGES {
//             rx.recv().unwrap();
//         }
//     })
//     .unwrap();
// }
//
// fn mpsc_async() {
//     let (tx, rx) = crate::lib::channel();
//
//     crossbeam::scope(|scope| {
//         for _ in 0..THREADS {
//             let tx = tx.clone();
//             scope.spawn(move |_| {
//                 for i in 0..MESSAGES / THREADS {
//                     tx.send(new_message(i));
//                 }
//             });
//         }
//
//         for _ in 0..MESSAGES {
//             rx.recv().unwrap();
//         }
//     })
//     .unwrap();
// }
//
// pub fn run_bench() {
//     macro_rules! run {
//         ($name:expr, $f:expr) => {
//             let now = ::std::time::Instant::now();
//             $f;
//             let elapsed = now.elapsed();
//             println!(
//                 "{:25} {:15} {:7.3} sec",
//                 $name,
//                 "Rust mpsc",
//                 elapsed.as_secs() as f64 + elapsed.subsec_nanos() as f64 / 1e9
//             );
//         };
//     }
//     run!("unbounded_mpsc", mpsc_async());
//     run!("unbounded_seq", seq_async());
//     run!("unbounded_spsc", spsc_async());
// }
