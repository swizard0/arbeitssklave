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
    pub sklavenwelt: Sklavenwelt<W, B>,
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
    activity: Activity<W, B>,
}

enum Activity<W, B> {
    Work,
    Rest(Sklavenwelt<W, B>),
}

pub enum Gehorsam<W, B> {
    Machen {
        befehle: SklavenBefehle<W, B>,
    },
    Rasten,
}

pub struct Sklavenwelt<W, B> {
    sklavenwelt: W,
    taken_orders: Vec<B>,
}

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
        meister.whip(Sklavenwelt { sklavenwelt, taken_orders: Vec::new(), }, thread_pool)?;
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

    fn whip<P, J>(&self, sklavenwelt: Sklavenwelt<W, B>, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
    {
        let sklave_job = SklaveJob {
            sklave: Sklave {
                maybe_inner: Some(self.inner.clone()),
            },
            sklavenwelt,
        };
        edeltraud::job(thread_pool, sklave_job)
            .map_err(Error::Edeltraud)
    }
}

impl<W, B> Sklave<W, B> {
    pub fn zu_ihren_diensten(&mut self, mut sklavenwelt: Sklavenwelt<W, B>) -> Result<Gehorsam<W, B>, Error> {
        loop {
            if sklavenwelt.taken_orders.is_empty() {
                let inner = self.maybe_inner.take()
                    .ok_or(Error::Terminated)?;
                match &mut *inner.state.lock().map_err(|_| Error::MutexIsPoisoned)? {
                    InnerState::Active(state) if state.orders.is_empty() => {
                        assert!(matches!(state.activity, Activity::Work));
                        state.activity = Activity::Rest(sklavenwelt);
                        return Ok(Gehorsam::Rasten);
                    },
                    InnerState::Active(InnerStateActive { orders, .. }) => {
                        mem::swap(orders, &mut sklavenwelt.taken_orders);
                    },
                    InnerState::Terminated =>
                        return Err(Error::Terminated),
                }
                self.maybe_inner = Some(inner);
            } else {
                return Ok(Gehorsam::Machen {
                    befehle: SklavenBefehle { sklavenwelt, },
                });
            }
        }
    }

    pub fn meister(&self) -> Result<Meister<W, B>, Error> {
        let inner = self.maybe_inner.as_ref()
            .ok_or(Error::Terminated)?;
        Ok(Meister {
            inner: inner.clone(),
        })
    }
}

pub struct SklavenBefehle<W, B> {
    sklavenwelt: Sklavenwelt<W, B>,
}

pub enum SklavenBefehl<W, B> {
    Mehr { befehl: B, mehr_befehle: SklavenBefehle<W, B>, },
    Ende { sklavenwelt: Sklavenwelt<W, B>, },
}

impl<W, B> SklavenBefehle<W, B> {
    pub fn befehl(mut self) -> SklavenBefehl<W, B> {
        match self.sklavenwelt.taken_orders.pop() {
            Some(befehl) =>
                SklavenBefehl::Mehr { befehl, mehr_befehle: self, },
            None =>
                SklavenBefehl::Ende { sklavenwelt: self.sklavenwelt, },
        }
    }

    pub fn sklavenwelt(&self) -> &Sklavenwelt<W, B> {
        &self.sklavenwelt
    }

    pub fn sklavenwelt_mut(&mut self) -> &mut Sklavenwelt<W, B> {
        &mut self.sklavenwelt
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

impl<W, B> Deref for Sklavenwelt<W, B> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        &self.sklavenwelt
    }
}

impl<W, B> DerefMut for Sklavenwelt<W, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sklavenwelt
    }
}
