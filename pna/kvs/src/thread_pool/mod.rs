//! Implementations of different type of threadpool to parallelize operation on the data store

mod naive;
pub use naive::NaiveThreadPool;

mod shared_queue;
pub use shared_queue::SharedQueueThreadPool;

mod rayon;
pub use rayon::RayonThreadPool;

use crate::Result;

/// Interface of a threads manager that queues threads and executes the queued threads when
/// possible
pub trait ThreadPool {
    /// Create a threadpool that is heap-allocated
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;
    // fn new(threads: u32) -> Result<Self>;
    /// Executes the given closure if possible, otherwise, queues the task for future execution
    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static;
}

/// Heap-allocated thread's closure
pub type Thunk<'a> = Box<dyn FnOnce() + Send + 'a>;
