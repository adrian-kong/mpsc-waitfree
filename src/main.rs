mod queue;
use crate::queue::channel;
use std::time::Instant;

fn main() {
    const SIZE: usize = 10_000_000;
    let now = Instant::now();
    let (tx, rx, queue) = channel::<usize>();
    let producer1 = tx.clone();
    let a = std::thread::spawn(move || {
        let mut i = 0;
        loop {
            producer1.send(i);
            i += 3;
            if i >= SIZE {
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
            if i >= SIZE {
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
            if i >= SIZE {
                break;
            }
        }
    });
    a.join().unwrap();
    b.join().unwrap();
    c.join().unwrap();
    println!("{:?}", now.elapsed());
    println!("{:?}", queue.len());
    println!("{:?}", rx.recv().unwrap());
    println!("{:?}", rx.recv().unwrap());
    println!("{:?}", rx.recv().unwrap());
    println!("{:?}", rx.recv().unwrap());
}
