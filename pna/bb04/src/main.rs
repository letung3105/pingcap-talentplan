use bb04::ThreadPool;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let thread_pool = ThreadPool::new(4)?;
    let handles: Vec<_> = (0..100)
        .map(|i| {
            let pool = thread_pool.clone();
            std::thread::spawn(move || {
                pool.exec(move || println!("Hello world {}1", i));
                pool.exec(move || println!("Hello world {}2", i));
                pool.exec(move || println!("Hello world {}3", i));
                pool.join();
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }

    Ok(())
}
