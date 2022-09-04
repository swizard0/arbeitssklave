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

pub struct SklaveJob<W, B> {
    maybe_sklave_job_inner: Option<SklaveJobInner<W, B>>,
}

struct SklaveJobInner<W, B> {
    inner: Arc<Inner<W, B>>,
    welt: Sklavenwelt<W, B>,
}

struct Sklavenwelt<W, B> {
    sklavenwelt: W,
    taken_orders: Vec<B>,
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
          J: edeltraud::Job + From<SklaveJob<W, B>>,
    {
        let meister = Meister { inner: self.inner, };
        meister.whip(Sklavenwelt { sklavenwelt, taken_orders: Vec::new(), }, thread_pool)?;
        Ok(meister)
    }
}

impl<W, B> Meister<W, B> {
    pub fn befehl<P, J>(&self, order: B, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
    {
        self.befehle(std::iter::once(order), thread_pool)
    }

    pub fn befehle<P, J, I>(&self, orders: I, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
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
            Activity::Rest(welt) =>
                self.whip(welt, thread_pool),
        }
    }

    fn whip<P, J>(&self, welt: Sklavenwelt<W, B>, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
    {
        let sklave_job = SklaveJob {
            maybe_sklave_job_inner: Some(SklaveJobInner {
                inner: self.inner.clone(),
                welt,
            }),
        };
        edeltraud::job(thread_pool, sklave_job)
            .map_err(Error::Edeltraud)
    }
}

impl<W, B> SklaveJob<W, B> {
    pub fn zu_ihren_diensten(mut self) -> Result<Gehorsam<W, B>, Error> {
        let mut sklave_job_inner = self.maybe_sklave_job_inner
            .take()
            .ok_or(Error::Terminated)?;
        loop {
            if sklave_job_inner.welt.taken_orders.is_empty() {
                match &mut *sklave_job_inner.inner.state.lock().map_err(|_| Error::MutexIsPoisoned)? {
                    InnerState::Active(state) if state.orders.is_empty() => {
                        assert!(matches!(state.activity, Activity::Work));
                        state.activity = Activity::Rest(sklave_job_inner.welt);
                        return Ok(Gehorsam::Rasten);
                    },
                    InnerState::Active(InnerStateActive { orders, .. }) => {
                        mem::swap(orders, &mut sklave_job_inner.welt.taken_orders);
                    },
                    InnerState::Terminated =>
                        return Err(Error::Terminated),
                }
            } else {
                self.maybe_sklave_job_inner =
                    Some(sklave_job_inner);
                return Ok(Gehorsam::Machen {
                    befehle: SklavenBefehle { sklave_job: self, },
                });
            };
        }
    }

    pub fn meister(&self) -> Meister<W, B> {
        let sklave_job_inner = self.maybe_sklave_job_inner.as_ref()
            .unwrap();
        Meister {
            inner: sklave_job_inner.inner.clone(),
        }
    }

    pub fn sklavenwelt(&self) -> &W {
        let sklave_job_inner = self.maybe_sklave_job_inner.as_ref()
            .unwrap();
        &sklave_job_inner.welt.sklavenwelt
    }

    pub fn sklavenwelt_mut(&mut self) -> &mut W {
        let sklave_job_inner = self.maybe_sklave_job_inner.as_mut()
            .unwrap();
        &mut sklave_job_inner.welt.sklavenwelt
    }
}

pub struct SklavenBefehle<W, B> {
    sklave_job: SklaveJob<W, B>,
}

pub enum SklavenBefehl<W, B> {
    Mehr { befehl: B, mehr_befehle: SklavenBefehle<W, B>, },
    Ende { sklave_job: SklaveJob<W, B>, },
}

impl<W, B> SklavenBefehle<W, B> {
    pub fn befehl(mut self) -> SklavenBefehl<W, B> {
        let inner = self.sklave_job
            .maybe_sklave_job_inner
            .as_mut()
            .unwrap();
        match inner.welt.taken_orders.pop() {
            Some(befehl) =>
                SklavenBefehl::Mehr { befehl, mehr_befehle: self, },
            None =>
                SklavenBefehl::Ende {
                    sklave_job: self.sklave_job,
                },
        }
    }
}

impl<W, B> Deref for SklavenBefehle<W, B> {
    type Target = SklaveJob<W, B>;

    fn deref(&self) -> &Self::Target {
        &self.sklave_job
    }
}

impl<W, B> DerefMut for SklavenBefehle<W, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sklave_job
    }
}

impl<W, B> Drop for SklaveJob<W, B> {
    fn drop(&mut self) {
        if let Some(sklave_job_inner) = self.maybe_sklave_job_inner.take() {
            if let Ok(mut state) = sklave_job_inner.inner.state.lock() {
                *state = InnerState::Terminated;
            }
        }
    }
}
