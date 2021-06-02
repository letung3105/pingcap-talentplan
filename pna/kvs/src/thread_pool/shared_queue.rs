use crate::thread_pool::ThreadPool;
use crate::Result;

/// A thread spawner, that reuses no thread
#[derive(Debug, Clone)]
pub struct SharedQueueThreadPool;

impl ThreadPool for SharedQueueThreadPool {
    fn new(_threads: u32) -> Result<Self> {
        todo!()
    }

    fn spawn<F>(&self, _f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        todo!()
    }
}

