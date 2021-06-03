use crate::thread_pool::{ThreadPool, Thunk};
use crate::Result;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;

/// A threadpool that spawns a fix number of threads on startup and maintains a fix number of
/// active threads when it is active. Jobs are shared between threads via a multiple producer
/// single receiver channel.
#[allow(missing_debug_implementations)]
pub struct SharedQueueThreadPool {
    job_tx: Sender<Thunk<'static>>,
    _context: Arc<Context>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self> {
        let (job_tx, job_rx) = mpsc::channel();
        let _context = Arc::new(Context::new(job_rx));
        for _ in 0..threads {
            spawn_in_pool(_context.clone());
        }
        Ok(Self { job_tx, _context })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.job_tx.send(Box::new(f)).ok();
    }
}

fn spawn_in_pool(context: Arc<Context>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut sentinel = Sentinel::new(&context);
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

/// Data structure holding the shared state between all threads in the pool
struct Context {
    job_rx: Mutex<Receiver<Thunk<'static>>>,
}

impl Context {
    fn new(job_rx: Receiver<Thunk<'static>>) -> Self {
        Self {
            job_rx: Mutex::new(job_rx),
        }
    }
}

/// Monitor the a thread's execution and see if a thread exits gracefully or it panics.
/// If a thread did not finish execution gracefully, spawn a new thread to replaced the
/// finished thread.
struct Sentinel<'a> {
    context: &'a Arc<Context>,
    active: bool,
}

impl<'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            spawn_in_pool(self.context.clone());
        }
    }
}

impl<'a> Sentinel<'a> {
    fn new(context: &'a Arc<Context>) -> Self {
        Self {
            context,
            active: true,
        }
    }

    fn stop(&mut self) {
        self.active = false;
    }
}
