use crate::thread_pool::ThreadPool;
use crate::Result;

/// A thread spawner, that reuses no thread
#[allow(missing_debug_implementations)]
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        Ok(Self)
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        std::thread::spawn(f);
    }
}
