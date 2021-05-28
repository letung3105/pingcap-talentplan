use bb04::ThreadPool;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let thread_pool = ThreadPool::new(4)?;
    thread_pool.exec(|| println!("Hello world 1"));
    thread_pool.exec(|| println!("Hello world 2"));
    thread_pool.exec(|| println!("Hello world 3"));
    std::thread::sleep(Duration::from_secs(5));
    Ok(())
}
