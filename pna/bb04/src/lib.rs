use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

type Thunk<'a> = Box<dyn FnOnce() + Send + 'a>;

pub struct ThreadPool {
    jobs_tx: Sender<Thunk<'static>>,
    context: Arc<Context>,
}

impl ThreadPool {
    pub fn new(threads: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let (jobs_tx, jobs_rx) = mpsc::channel();
        let context = Arc::new(Context::new(jobs_rx));
        (0..threads).for_each(|_| {
            spawn(context.clone());
        });
        Ok(Self { jobs_tx, context })
    }

    pub fn exec<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.context.count_queued.fetch_add(1, Ordering::SeqCst);
        self.jobs_tx
            .send(Box::new(job))
            .expect("Could not queue job for execution.")
    }

    pub fn join(&self) {
        if !self.context.has_work() {
            return;
        }

        let join_generation = self.context.join_generation.load(Ordering::SeqCst);
        let mut join_lock = self.context.join_lock.lock().unwrap();
        while join_generation == self.context.join_generation.load(Ordering::Relaxed)
            && self.context.has_work()
        {
            join_lock = self.context.join_condvar.wait(join_lock).unwrap();
        }

        self.context
            .join_generation
            .compare_exchange(
                join_generation,
                join_generation.wrapping_add(1),
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .ok();
    }
}

impl Clone for ThreadPool {
    fn clone(&self) -> Self {
        Self {
            jobs_tx: self.jobs_tx.clone(),
            context: self.context.clone(),
        }
    }
}

pub struct Context {
    jobs_rx: Mutex<Receiver<Thunk<'static>>>,
    count_queued: AtomicUsize,
    count_active: AtomicUsize,
    join_generation: AtomicUsize,
    join_lock: Mutex<()>,
    join_condvar: Condvar,
}

impl Context {
    pub fn new(jobs_rx: Receiver<Thunk<'static>>) -> Self {
        Self {
            jobs_rx: Mutex::new(jobs_rx),
            count_queued: AtomicUsize::new(0),
            count_active: AtomicUsize::new(0),
            join_generation: AtomicUsize::new(0),
            join_lock: Mutex::new(()),
            join_condvar: Condvar::new(),
        }
    }
    pub fn has_work(&self) -> bool {
        self.count_queued.load(Ordering::SeqCst) > 0 || self.count_active.load(Ordering::SeqCst) > 0
    }

    pub fn notify_all_join(&self) {
        if !self.has_work() {
            *self
                .join_lock
                .lock()
                .expect("Unable to notify all joining threads.");
            self.join_condvar.notify_all();
        }
    }
}

struct Sentinel<'a> {
    context: &'a Arc<Context>,
    active: bool,
}

impl<'a> Sentinel<'a> {
    pub fn new(context: &'a Arc<Context>) -> Self {
        Self {
            context,
            active: true,
        }
    }

    pub fn stop(mut self) {
        self.active = false;
    }
}

impl<'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            self.context.count_active.fetch_sub(1, Ordering::SeqCst);
            self.context.notify_all_join();
            spawn(self.context.clone());
        }
    }
}

fn spawn(context: Arc<Context>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let sentinel = Sentinel::new(&context);
        loop {
            let job = {
                // drop MutexGuard as soon as a job is acquired
                let jobs_rx = context
                    .jobs_rx
                    .lock()
                    .expect("Could not get exclusive access to the jobs receive channel.");
                jobs_rx.recv()
            };

            let job = match job {
                Ok(job) => job,
                Err(_) => break,
            };

            context.count_active.fetch_add(1, Ordering::SeqCst);
            context.count_queued.fetch_sub(1, Ordering::SeqCst);
            job();
            context.count_active.fetch_sub(1, Ordering::SeqCst);
            context.notify_all_join();
        }
        sentinel.stop();
    })
}
