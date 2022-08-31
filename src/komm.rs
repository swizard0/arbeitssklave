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

use crate::{
    Freie,
    Meister,
    SklaveJob,
};

pub struct Sendegeraet<B> {
    inner: Arc<Inner<B>>,
    join_handle: Option<Arc<thread::JoinHandle<()>>>,
}

impl<B> Clone for Sendegeraet<B> {
    fn clone(&self) -> Self {
        Sendegeraet {
            inner: self.inner.clone(),
            join_handle: self.join_handle.clone(),
        }
    }
}

struct Inner<B> {
    state: Mutex<InnerState<B>>,
    condvar: Condvar,
}

enum InnerState<B> {
    Active(InnerStateActive<B>),
    Terminated(Option<Error>),
}

struct InnerStateActive<B> {
    orders: Vec<B>,
}

#[derive(Debug)]
pub enum Error {
    ThreadSpawn(io::Error),
    MutexIsPoisoned,
    Meister(super::Error),
}

pub struct Rueckkopplung<B, S> {
    sendegeraet: Sendegeraet<B>,
    stamp: S,
}

pub struct Umschlag<T, S> {
    pub payload: T,
    pub stamp: S,
}

impl<B> Sendegeraet<B> {
    pub fn spawn<W, P, J>(freie: &Freie<W, B>, thread_pool: P) -> Result<Self, Error>
    where P: edeltraud::ThreadPool<J> + Send + 'static,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
          W: Send + 'static,
          B: Send + 'static,
    {
        let inner = Arc::new(Inner {
            state: Mutex::new(InnerState::Active(InnerStateActive {
                orders: Vec::new(),
            })),
            condvar: Condvar::new(),
        });
        let inner_clone = inner.clone();
        let meister = Meister { inner: freie.inner.clone(), };

        let join_handle = thread::Builder::new()
            .name("arbeitssklave::komm::Sendegeraet".to_string())
            .spawn(move || sendegeraet_loop(&meister, &inner_clone, &thread_pool))
            .map_err(Error::ThreadSpawn)?;

        Ok(Sendegeraet {
            inner,
            join_handle: Some(Arc::new(join_handle)),
        })
    }

    pub fn rueckkopplung<S>(&self, stamp: S) -> Rueckkopplung<B, S> {
        Rueckkopplung { sendegeraet: self.clone(), stamp, }
    }

    pub fn order(&self, order: B) -> Result<(), Error> {
        self.orders(std::iter::once(order))
    }

    pub fn orders<I>(&self, orders: I) -> Result<(), Error> where I: IntoIterator<Item = B> {
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
                    Err(Error::MutexIsPoisoned)
                },
        }
    }
}

impl<B> Drop for Sendegeraet<B> {
    fn drop(&mut self) {
        if let Some(join_handle_arc) = self.join_handle.take() {
            if let Ok(join_handle) = Arc::try_unwrap(join_handle_arc) {
                if let Ok(mut locked_state) = self.inner.state.lock() {
                    if let InnerState::Active(..) = &*locked_state {
                        *locked_state = InnerState::Terminated(None);
                    }
                }
                join_handle.join().ok();
            }
        }
    }
}

impl<B, S> Rueckkopplung<B, S> {
    pub fn commit<T>(self, payload: T) -> Result<(), Error> where B: From<Umschlag<T, S>> {
        let umschlag = Umschlag { payload, stamp: self.stamp, };
        let order = umschlag.into();
        self.sendegeraet.order(order)
    }
}

fn sendegeraet_loop<W, B, P, J>(
    meister: &Meister<W, B>,
    inner: &Inner<B>,
    thread_pool: &P,
)
where P: edeltraud::ThreadPool<J> + Send + 'static,
      J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
      W: Send + 'static,
      B: Send + 'static,
{
    let mut taken_orders = Vec::new();
    let mut maybe_maybe_error = None;
    loop {
        if let Some(maybe_error) = maybe_maybe_error.take() {
            if let Ok(mut locked_state) = inner.state.lock() {
                *locked_state = InnerState::Terminated(maybe_error);
            }
            break;
        }

        if let Err(maybe_error) = acquire_orders(inner, &mut taken_orders) {
            maybe_maybe_error = Some(maybe_error);
            continue;
        }
        assert!(!taken_orders.is_empty());

        if let Err(error) = meister.orders(taken_orders.drain(..), thread_pool).map_err(Error::Meister) {
            maybe_maybe_error = Some(Some(error));
        }
    }
}

fn acquire_orders<B>(inner: &Inner<B>, taken_orders: &mut Vec<B>) -> Result<(), Option<Error>> {
    let mut locked_state = inner.state.lock()
        .map_err(|_| None)?;
    loop {
        match &mut *locked_state {
            InnerState::Active(InnerStateActive { orders, }) if !orders.is_empty() => {
                mem::swap(orders, taken_orders);
                return Ok(());
            },
            InnerState::Active(..) => {
                locked_state = inner.condvar.wait(locked_state)
                    .map_err(|_| None)?;
            },
            InnerState::Terminated(maybe_error) =>
                return Err(maybe_error.take()),
        }
    }
}
