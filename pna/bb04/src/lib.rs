use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};

type Thunk<'a> = Box<dyn FnOnce() + Send + 'a>;

pub struct ThreadPool {
    jobs_tx: Sender<Thunk<'static>>,
    context: Arc<ThreadPoolContext>,
}

impl ThreadPool {
    pub fn new(threads: u32) -> Result<Self, Box<dyn std::error::Error>> {
        let (jobs_tx, jobs_rx) = mpsc::channel();
        let jobs_rx = Mutex::new(jobs_rx);

        let context = Arc::new(ThreadPoolContext { jobs_rx });
        (0..threads).for_each(|_| spawn(context.clone()));
        Ok(Self { jobs_tx, context })
    }

    pub fn exec<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.jobs_tx
            .send(Box::new(job))
            .expect("Could not queue job for execution.")
    }

    pub fn join(&self) {
        todo!()
    }
}

pub struct ThreadPoolContext {
    jobs_rx: Mutex<Receiver<Thunk<'static>>>,
}

fn spawn(context: Arc<ThreadPoolContext>) {
    std::thread::spawn(move || loop {
        let queued = {
            // drop MutexGuard as soon as a job is acquired
            let guarded_jobs_rx = context
                .jobs_rx
                .lock()
                .expect("Could not get exclusive access to the jobs receive channel.");
            guarded_jobs_rx.recv()
        };

        match queued {
            Ok(job) => job(),
            Err(_) => break,
        }
    });
}
