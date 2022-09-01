#![forbid(unsafe_code)]

use std::{
    mem,
    ops::{
        Deref,
        DerefMut,
    },
    sync::{
        Arc,
        Mutex,
    },
};

pub mod ewig;
pub mod komm;

#[cfg(test)]
mod tests;

pub struct Freie<W, B> {
    inner: Arc<Inner<W, B>>,
}

pub struct Meister<W, B> {
    inner: Arc<Inner<W, B>>,
}

impl<W, B> Clone for Meister<W, B> {
    fn clone(&self) -> Self {
        Meister {
            inner: self.inner.clone(),
        }
    }
}

pub struct Sklave<W, B> {
    maybe_inner: Option<Arc<Inner<W, B>>>,
}

pub struct SklaveJob<W, B> {
    pub sklave: Sklave<W, B>,
    pub sklavenwelt: Sklavenwelt<W>,
}

struct Inner<W, B> {
    state: Mutex<InnerState<W, B>>,
}

enum InnerState<W, B> {
    Active(InnerStateActive<W, B>),
    Terminated,
}

struct InnerStateActive<W, B> {
    orders: Vec<B>,
    activity: Activity<W>,
}

enum Activity<W> {
    Work,
    Rest(Sklavenwelt<W>),
}

pub enum Gehorsam<W, B> {
    Machen {
        befehl: B,
        sklavenwelt: Sklavenwelt<W>,
    },
    Rasten,
}

pub struct Sklavenwelt<W>(W);

#[derive(Debug)]
pub enum Error {
    Edeltraud(edeltraud::SpawnError),
    Terminated,
    MutexIsPoisoned,
}

impl<W, B> Freie<W, B> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(InnerState::Active(InnerStateActive {
                    orders: Vec::new(),
                    activity: Activity::Work,
                })),
            }),
        }
    }

    pub fn meister(&self) -> Meister<W, B> {
        Meister { inner: self.inner.clone(), }
    }

    pub fn versklaven<P, J>(self, sklavenwelt: W, thread_pool: &P) -> Result<Meister<W, B>, Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
    {
        let meister = Meister { inner: self.inner, };
        meister.whip(Sklavenwelt(sklavenwelt), thread_pool)?;
        Ok(meister)
    }
}

impl<W, B> Meister<W, B> {
    pub fn befehl<P, J>(&self, order: B, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
    {
        self.befehle(std::iter::once(order), thread_pool)
    }

    pub fn befehle<P, J, I>(&self, orders: I, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
          I: IntoIterator<Item = B>,
    {
        let prev_activity =
            match *self.inner.state.lock().map_err(|_| Error::MutexIsPoisoned)? {
                InnerState::Active(ref mut state) => {
                    state.orders.extend(orders);
                    mem::replace(&mut state.activity, Activity::Work)
                },
                InnerState::Terminated =>
                    return Err(Error::Terminated),
            };
        match prev_activity {
            Activity::Work =>
                Ok(()),
            Activity::Rest(sklavenwelt) =>
                self.whip(sklavenwelt, thread_pool)
        }
    }

    fn whip<P, J>(&self, sklavenwelt: Sklavenwelt<W>, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
    {
        edeltraud::job(
            thread_pool,
            SklaveJob {
                sklave: Sklave { maybe_inner: Some(self.inner.clone()), },
                sklavenwelt,
            },
        )
            .map_err(Error::Edeltraud)
    }
}

impl<W, B> Sklave<W, B> {
    pub fn zu_ihren_diensten(&mut self, sklavenwelt: Sklavenwelt<W>) -> Result<Gehorsam<W, B>, Error> {
        let inner = self.maybe_inner.take()
            .ok_or(Error::Terminated)?;
        let befehl = match *inner.state.lock().map_err(|_| Error::MutexIsPoisoned)? {
            InnerState::Active(ref mut state) =>
                match state.orders.pop() {
                    Some(order) =>
                        order,
                    None => {
                        assert!(matches!(state.activity, Activity::Work));
                        state.activity = Activity::Rest(sklavenwelt);
                        return Ok(Gehorsam::Rasten);
                    },
                },
            InnerState::Terminated =>
                return Err(Error::Terminated),
        };
        self.maybe_inner = Some(inner);
        Ok(Gehorsam::Machen { befehl, sklavenwelt, })
    }

    pub fn meister(&self) -> Result<Meister<W, B>, Error> {
        let inner = self.maybe_inner.as_ref()
            .ok_or(Error::Terminated)?;
        Ok(Meister {
            inner: inner.clone(),
        })
    }
}

impl<W, B> Drop for Sklave<W, B> {
    fn drop(&mut self) {
        if let Some(inner) = self.maybe_inner.take() {
            if let Ok(mut state) = inner.state.lock() {
                *state = InnerState::Terminated;
            }
        }
    }
}

impl<W> Deref for Sklavenwelt<W> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<W> DerefMut for Sklavenwelt<W> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
