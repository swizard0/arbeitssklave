#![forbid(unsafe_code)]

use std::{
    mem,
    ops::{
        Deref,
        DerefMut,
    },
    sync::{
        atomic,
        Arc,
        Mutex,
        TryLockError,
    },
    collections::{
        VecDeque,
    },
};

pub mod ewig;
pub mod komm;
pub mod utils;

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
    welt: Box<Sklavenwelt<W, B>>,
}

struct Sklavenwelt<W, B> {
    sklavenwelt: W,
    taken_orders: VecDeque<B>,
}

struct Inner<W, B> {
    orders: crossbeam::queue::SegQueue<B>,
    touch_tag: TouchTag,
    activity: Mutex<Activity<W, B>>,
}

enum Activity<W, B> {
    Work,
    Rest(Box<Sklavenwelt<W, B>>),
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

struct TouchTag {
    tag: atomic::AtomicU64,
}

impl Default for TouchTag {
    fn default() -> TouchTag {
        TouchTag {
            tag: atomic::AtomicU64::new(0),
        }
    }
}

impl TouchTag {
    const ORDERS_COUNT_MASK: u64 = u32::MAX as u64;
    const RESTING_BIT: u64 = Self::ORDERS_COUNT_MASK.wrapping_add(1);
    const TERMINATED_BIT: u64 = Self::RESTING_BIT.wrapping_shl(1);

    fn load(&self) -> u64 {
        self.tag.load(atomic::Ordering::SeqCst)
    }

    fn try_set(&self, prev_tag: u64, new_tag: u64) -> bool {
        self.tag
            .compare_exchange(
                prev_tag,
                new_tag,
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .is_ok()
    }

    fn set_terminated(&self) {
        self.tag.fetch_or(Self::TERMINATED_BIT, atomic::Ordering::SeqCst);
    }

    fn decompose(tag: u64) -> (bool, bool, usize) {
        (
            tag & Self::TERMINATED_BIT != 0,
            tag & Self::RESTING_BIT != 0,
            (tag & Self::ORDERS_COUNT_MASK) as usize,
        )
    }

    fn compose(is_terminated: bool, is_resting: bool, orders_count: usize) -> u64 {
        let mut tag = orders_count as u64;
        if is_resting {
            tag |= Self::RESTING_BIT;
        }
        if is_terminated {
            tag |= Self::TERMINATED_BIT;
        }
        tag
    }
}

impl<W, B> Default for Freie<W, B> {
    fn default() -> Self {
        Self::new()
    }
}

impl<W, B> Freie<W, B> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                orders: crossbeam::queue::SegQueue::new(),
                touch_tag: TouchTag::default(),
                activity: Mutex::new(Activity::Work),
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
        meister.whip(
            Box::new(Sklavenwelt { sklavenwelt, taken_orders: VecDeque::new(), }),
            thread_pool,
        )?;
        Ok(meister)
    }
}

impl<W, B> Meister<W, B> {
    pub fn befehl<P, J>(&self, order: B, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
    {
        loop {
            let prev_tag = self.inner.touch_tag.load();
            let (is_terminated, is_resting, orders_count) = TouchTag::decompose(prev_tag);
            if is_terminated {
                return Err(Error::Terminated);
            }
            let new_tag = TouchTag::compose(is_terminated, false, orders_count + 1);
            if !self.inner.touch_tag.try_set(prev_tag, new_tag) {
                continue;
            }
            if is_resting {
                let prev_activity = {
                    let mut activity_lock = self.inner.activity.lock()
                        .map_err(|_| Error::MutexIsPoisoned)?;
                    mem::replace(&mut *activity_lock, Activity::Work)
                };
                match prev_activity {
                    Activity::Rest(welt) =>
                        self.whip(welt, thread_pool)?,
                    Activity::Work =>
                        unreachable!("totally unexpected Activity::Work after taking `is_resting` flag"),
                }
            }
            self.inner.orders.push(order);
            return Ok(());
        }
    }

    pub fn befehle<P, J, I>(&self, orders: I, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
          I: IntoIterator<Item = B>,
    {
        for order in orders {
            self.befehl(order, thread_pool)?;
        }
        Ok(())
    }

    fn whip<P, J>(&self, welt: Box<Sklavenwelt<W, B>>, thread_pool: &P) -> Result<(), Error>
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
        let backoff = crossbeam::utils::Backoff::new();
        'outer: loop {
            let prev_tag = sklave_job_inner.inner.touch_tag.load();
            let (is_terminated, is_resting, orders_count) = TouchTag::decompose(prev_tag);
            assert!(!is_resting);
            if is_terminated {
                return Err(Error::Terminated);
            }
            if orders_count == 0 {
                if !sklave_job_inner.welt.taken_orders.is_empty() {
                    self.maybe_sklave_job_inner = Some(sklave_job_inner);
                    return Ok(Gehorsam::Machen {
                        befehle: SklavenBefehle { sklave_job: self, },
                    });
                }

                let new_tag = TouchTag::compose(is_terminated, true, 0);
                match sklave_job_inner.inner.activity.try_lock() {
                    Ok(mut activity_lock) => {
                        if !sklave_job_inner.inner.touch_tag.try_set(prev_tag, new_tag) {
                            backoff.snooze();
                            continue 'outer;
                        }
                        *activity_lock = Activity::Rest(sklave_job_inner.welt);
                        return Ok(Gehorsam::Rasten);
                    },
                    Err(TryLockError::WouldBlock) => {
                        backoff.snooze();
                        continue 'outer;
                    },
                    Err(TryLockError::Poisoned(..)) =>
                        return Err(Error::MutexIsPoisoned),
                }
            } else {
                let new_tag = TouchTag::compose(is_terminated, false, orders_count - 1);
                if !sklave_job_inner.inner.touch_tag.try_set(prev_tag, new_tag) {
                    continue 'outer;
                }
                loop {
                    if let Some(order) = sklave_job_inner.inner.orders.pop() {
                        sklave_job_inner.welt.taken_orders.push_back(order);
                        continue 'outer;
                    }
                    backoff.snooze();
                }
            }
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
        match inner.welt.taken_orders.pop_front() {
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
            sklave_job_inner.inner.touch_tag.set_terminated();
        }
    }
}
