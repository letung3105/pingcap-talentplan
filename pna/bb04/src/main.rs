use bb04::ThreadPool;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let thread_pool = ThreadPool::new(4)?;
    thread_pool.exec(|| println!("Hello world"));
    Ok(())
}
