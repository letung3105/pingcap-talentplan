//! Implementations of different type of threadpool to parallelize operation on the data store

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

/// A thread spawner, that reuses no thread
#[derive(Debug, Clone)]
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

/// A thread spawner, that reuses no thread
#[derive(Debug, Clone)]
pub struct RayonThreadPool;

impl ThreadPool for RayonThreadPool {
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
