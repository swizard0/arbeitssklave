use std::{
    io,
    mem,
    sync::{
        Arc,
        Mutex,
        Condvar,
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
    taken_orders: Vec<B>,
}

struct Inner<B, E> {
    state: Mutex<InnerState<B, E>>,
    condvar: Condvar,
}

enum InnerState<B, E> {
    Active(InnerStateActive<B>),
    Terminated(Option<E>),
}

struct InnerStateActive<B> {
    orders: Vec<B>,
}

#[derive(Debug)]
pub enum Error {
    ThreadSpawn(io::Error),
    MutexIsPoisoned,
    Terminated,
}

impl<B, E> Freie<B, E> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(InnerState::Active(InnerStateActive {
                    orders: Vec::new(),
                })),
                condvar: Condvar::new(),
            }),
        }
    }

    pub fn versklaven<F>(self, sklave_job: F) -> Result<Meister<B, E>, E>
    where F: FnOnce(&mut Sklave<B, E>) -> Result<(), E> + Send + 'static,
          B: Send + 'static,
          E: From<Error> + Send + 'static,
    {
        self.versklaven_als("arbeitssklave::ewig::Sklave".to_string(), sklave_job)
    }

    pub fn versklaven_als<F>(self, thread_name: String, sklave_job: F) -> Result<Meister<B, E>, E>
    where F: FnOnce(&mut Sklave<B, E>) -> Result<(), E> + Send + 'static,
          B: Send + 'static,
          E: From<Error> + Send + 'static,
    {
        let mut sklave = Sklave {
            inner: self.inner.clone(),
            taken_orders: Vec::new(),
        };
        let join_handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                if let Err(error) = sklave_job(&mut sklave) {
                    if let Ok(mut locked_state) = sklave.inner.state.lock() {
                        *locked_state = InnerState::Terminated(Some(error));
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
                if let Ok(mut locked_state) = self.inner.state.lock() {
                    if let InnerState::Active(..) = &*locked_state {
                        *locked_state = InnerState::Terminated(None);
                        self.inner.condvar.notify_one();
                    }
                }
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
        match *self.inner.state.lock().map_err(|_| Error::MutexIsPoisoned)? {
            InnerState::Active(ref mut state) => {
                state.orders.extend(orders);
                self.inner.condvar.notify_one();
                Ok(())
            },
            InnerState::Terminated(ref mut maybe_error) =>
                if let Some(error) = maybe_error.take() {
                    Err(error)
                } else {
                    Err(Error::MutexIsPoisoned.into())
                },
        }
    }
}

impl<B, E> Sklave<B, E> where E: From<Error> {
    pub fn zu_ihren_diensten(&mut self) -> Result<impl Iterator<Item = B> + '_, E> {
        let mut locked_state = self.inner.state.lock()
            .map_err(|_| Error::MutexIsPoisoned)?;
        loop {
            match &mut *locked_state {
                InnerState::Active(InnerStateActive { orders, }) if !orders.is_empty() => {
                    mem::swap(orders, &mut self.taken_orders);
                    return Ok(self.taken_orders.drain(..));
                },
                InnerState::Active(..) => {
                    locked_state = self.inner.condvar.wait(locked_state)
                        .map_err(|_| Error::MutexIsPoisoned)?;
                },
                InnerState::Terminated(maybe_error) =>
                    return Err(match maybe_error.take() {
                        None =>
                            Error::Terminated.into(),
                        Some(error) =>
                            error,
                    }),
            }
        }
    }
}
