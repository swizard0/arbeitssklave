use std::{
    io,
    sync::{
        atomic,
        Arc,
        Mutex,
    },
    thread,
};

pub struct Freie<B, E> {
    inner: Arc<Inner<B, E>>,
}

pub struct Meister<B, E> {
    inner: Arc<Inner<B, E>>,
    join_handle: Option<Arc<thread::JoinHandle<()>>>,
}

impl<B, E> Clone for Meister<B, E> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            join_handle: self.join_handle.clone(),
        }
    }
}

pub struct Sklave<B, E> {
    inner: Arc<Inner<B, E>>,
    sched_worker: crossbeam::deque::Worker<B>,
}

struct Inner<B, E> {
    injector: crossbeam::deque::Injector<B>,
    is_waiting: atomic::AtomicBool,
    is_terminated: atomic::AtomicBool,
    maybe_error: Mutex<Option<E>>,
}

#[derive(Debug)]
pub enum Error {
    ThreadSpawn(io::Error),
    Terminated,
}

impl<B, E> Freie<B, E> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                injector: crossbeam::deque::Injector::new(),
                is_waiting: atomic::AtomicBool::new(false),
                is_terminated: atomic::AtomicBool::new(false),
                maybe_error: Mutex::new(None),
            }),
        }
    }

    pub fn versklaven<F>(self, sklave_job: F) -> Result<Meister<B, E>, E>
    where F: FnOnce(&Sklave<B, E>) -> Result<(), E> + Send + 'static,
          B: Send + 'static,
          E: From<Error> + Send + 'static,
    {
        self.versklaven_als("arbeitssklave::ewig::Sklave".to_string(), sklave_job)
    }

    pub fn versklaven_als<F>(self, thread_name: String, sklave_job: F) -> Result<Meister<B, E>, E>
    where F: FnOnce(&Sklave<B, E>) -> Result<(), E> + Send + 'static,
          B: Send + 'static,
          E: From<Error> + Send + 'static,
    {
        let sklave = Sklave {
            inner: self.inner.clone(),
            sched_worker: crossbeam::deque::Worker::new_lifo(),
        };
        let join_handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let _drop_bomb = DropBomp { is_terminated: &sklave.inner.is_terminated, };
                if let Err(error) = sklave_job(&sklave) {
                    if let Ok(mut locked_maybe_error) = sklave.inner.maybe_error.lock() {
                        *locked_maybe_error = Some(error);
                    }
                }

                struct DropBomp<'a> {
                    is_terminated: &'a atomic::AtomicBool,
                }

                impl<'a> Drop for DropBomp<'a> {
                    fn drop(&mut self) {
                        self.is_terminated.store(true, atomic::Ordering::SeqCst);
                    }
                }
            })
            .map_err(Error::ThreadSpawn)?;
        Ok(Meister {
            inner: self.inner,
            join_handle: Some(Arc::new(join_handle)),
        })
    }
}

impl<B, E> Drop for Meister<B, E> {
    fn drop(&mut self) {
        if let Some(join_handle_arc) = self.join_handle.take() {
            if let Ok(join_handle) = Arc::try_unwrap(join_handle_arc) {
                self.inner.is_terminated.store(true, atomic::Ordering::SeqCst);
                join_handle.thread().unpark();
                join_handle.join().ok();
            }
        }
    }
}

impl<B, E> Meister<B, E> where E: From<Error> {
    pub fn befehl(&self, order: B) -> Result<(), E> {
        self.befehle(std::iter::once(order))
    }

    pub fn befehle<I>(&self, orders: I) -> Result<(), E> where I: IntoIterator<Item = B> {
        if self.inner.is_terminated.load(atomic::Ordering::SeqCst) {
            return if let Ok(mut locked_maybe_error) = self.inner.maybe_error.lock() {
                if let Some(error) = locked_maybe_error.take() {
                    Err(error)
                } else {
                    Err(Error::Terminated.into())
                }
            } else {
                Err(Error::Terminated.into())
            }
        }

        for order in orders {
            self.inner.injector.push(order);
        }
        if let Some(join_handle) = self.join_handle.as_ref() {
            if self.inner.is_waiting.load(atomic::Ordering::SeqCst) {
                join_handle.thread().unpark();
            }
        }

        Ok(())
    }
}

impl<B, E> Sklave<B, E> where E: From<Error> {
    pub fn zu_ihren_diensten(&self) -> Result<impl Iterator<Item = B> + '_, E> {
        let backoff = crossbeam::utils::Backoff::new();
        let mut steal = crossbeam::deque::Steal::Retry;
        'outer: loop {
            if self.inner.is_terminated.load(atomic::Ordering::SeqCst) {
                return Err(Error::Terminated.into());
            }

            match steal {
                crossbeam::deque::Steal::Empty => {
                    // nothing to do, sleeping
                    if backoff.is_completed() {
                        self.inner.is_waiting.store(true, atomic::Ordering::SeqCst);
                        loop {
                            thread::park();
                            if !self.inner.is_waiting.load(atomic::Ordering::SeqCst) ||
                                self.inner.is_terminated.load(atomic::Ordering::SeqCst)
                            {
                                break;
                            }
                        }
                    } else {
                        backoff.snooze();
                    }
                    steal = crossbeam::deque::Steal::Retry;
                    continue 'outer;
                },
                crossbeam::deque::Steal::Success(job) =>
                    return Ok(std::iter::once(job)),
                crossbeam::deque::Steal::Retry =>
                    (),
            }

            // first try to acquire a job from the local queue
            if let Some(job) = self.sched_worker.pop() {
                steal = crossbeam::deque::Steal::Success(job);
                continue 'outer;
            }

            // finally try to steal a batch from the injector
            match self.inner.injector.steal_batch_and_pop(&self.sched_worker) {
                crossbeam::deque::Steal::Empty =>
                    (),
                crossbeam::deque::Steal::Success(job) => {
                    steal = crossbeam::deque::Steal::Success(job);
                    continue 'outer;
                }
                crossbeam::deque::Steal::Retry =>
                    steal = crossbeam::deque::Steal::Retry,
            }
        }
    }
}
