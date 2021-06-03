use crate::thread_pool::{ThreadPool, Thunk};
use crate::Result;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// A thread spawner, that reuses no thread
#[derive(Clone)]
#[allow(missing_debug_implementations)]
pub struct SharedQueueThreadPool {
    job_tx: Sender<Thunk<'static>>,
    context: Arc<SharedQueueThreadPoolContext>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (job_tx, job_rx) = mpsc::channel();
        let context = Arc::new(SharedQueueThreadPoolContext::new(job_rx));
        for _ in 0..threads {
            Self::spawn_thread(context.clone());
        }
        Ok(Self { job_tx, context })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.job_tx.send(Box::new(f)).ok();
    }
}

impl SharedQueueThreadPool {
    fn spawn_thread(context: Arc<SharedQueueThreadPoolContext>) -> JoinHandle<()> {
        std::thread::spawn(move || {
            let mut sentinel = SharedQueueThreadPoolSentinel::new(&context);
            loop {
                let job = {
                    let job_rx = context.job_rx.lock().unwrap();
                    job_rx.recv()
                };

                match job {
                    // execute the queued job
                    Ok(job) => job(),
                    // stop the thread, the receive channel was closed
                    Err(_) => break,
                }
            }
            sentinel.stop();
        })
    }
}

/// Data structure holding the shared state between all threads in the pool
struct SharedQueueThreadPoolContext {
    job_rx: Mutex<Receiver<Thunk<'static>>>,
}

impl SharedQueueThreadPoolContext {
    fn new(job_rx: Receiver<Thunk<'static>>) -> Self {
        Self {
            job_rx: Mutex::new(job_rx),
        }
    }
}

/// Monitor the a thread's execution and see if a thread exits gracefully or it panics.
/// If a thread did not finish execution gracefully, spawn a new thread to replaced the
/// finished thread.
struct SharedQueueThreadPoolSentinel<'a> {
    context: &'a Arc<SharedQueueThreadPoolContext>,
    active: bool,
}

impl<'a> Drop for SharedQueueThreadPoolSentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            SharedQueueThreadPool::spawn_thread(self.context.clone());
        }
    }
}

impl<'a> SharedQueueThreadPoolSentinel<'a> {
    fn new(context: &'a Arc<SharedQueueThreadPoolContext>) -> Self {
        Self {
            context,
            active: true,
        }
    }

    fn stop(&mut self) {
        self.active = false;
    }
}
