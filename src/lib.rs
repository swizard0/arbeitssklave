use std::{
    ops::{
        Deref,
        DerefMut,
    },
    cell::{
        UnsafeCell,
    },
    sync::{
        atomic,
        Arc,
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
    inner: Arc<Inner<W, B>>,
    rasten_mark: bool,
}

impl<W, B> SklaveJob<W, B> {
    fn new(inner: Arc<Inner<W, B>>) -> Self {
        Self { inner, rasten_mark: false, }
    }
}

struct Sklavenwelt<W, B> {
    sklavenwelt: W,
    taken_orders: VecDeque<B>,
}

struct Inner<W, B> {
    orders: crossbeam::queue::SegQueue<B>,
    touch_tag: TouchTag,
    sklavenwelt: UnsafeCell<Option<Sklavenwelt<W, B>>>,
}

unsafe impl<W, B> Sync for Inner<W, B> { }

fn reach_sklavenwelt<W, B>(inner: &Arc<Inner<W, B>>) -> &Option<Sklavenwelt<W, B>> {
    unsafe { &*inner.sklavenwelt.get() }
}

fn reach_sklavenwelt_mut<W, B>(inner: &mut Arc<Inner<W, B>>) -> &mut Option<Sklavenwelt<W, B>> {
    unsafe { &mut *inner.sklavenwelt.get() }
}

#[derive(Debug)]
pub enum Error {
    Edeltraud(edeltraud::SpawnError),
    Terminated,
    SklavenweltDropped,
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

struct TouchTagDecoded {
    is_terminated: bool,
    is_ready: bool,
    orders_count: usize,
}

impl TouchTag {
    const ORDERS_COUNT_MASK: u64 = u32::MAX as u64;
    const READY_BIT: u64 = Self::ORDERS_COUNT_MASK.wrapping_add(1);
    const TERMINATED_BIT: u64 = Self::READY_BIT.wrapping_shl(1);

    fn load(&self) -> u64 {
        self.tag.load(atomic::Ordering::SeqCst)
    }

    fn try_set(&self, prev_tag: u64, new_tag: u64) -> Result<(), u64> {
        self.tag
            .compare_exchange_weak(
                prev_tag,
                new_tag,
                atomic::Ordering::Acquire,
                atomic::Ordering::Relaxed,
            )
            .map(|_| ())
    }

    fn decompose(tag: u64) -> TouchTagDecoded {
        TouchTagDecoded {
            is_terminated: tag & Self::TERMINATED_BIT != 0,
            is_ready: tag & Self::READY_BIT != 0,
            orders_count: (tag & Self::ORDERS_COUNT_MASK) as usize,
        }
    }

    fn compose(decoded: TouchTagDecoded) -> u64 {
        let mut tag = decoded.orders_count as u64;
        if decoded.is_ready {
            tag |= Self::READY_BIT;
        }
        if decoded.is_terminated {
            tag |= Self::TERMINATED_BIT;
        }
        tag
    }
}

impl<W, B> Freie<W, B> {
    pub fn new(sklavenwelt: W) -> Self {
        Self {
            inner: Arc::new(Inner {
                orders: crossbeam::queue::SegQueue::new(),
                touch_tag: TouchTag::default(),
                sklavenwelt: UnsafeCell::new(Some(Sklavenwelt {
                    sklavenwelt,
                    taken_orders: VecDeque::new(),
                })),
            }),
        }
    }

    pub fn versklaven<P, J>(self, thread_pool: &P) -> Result<Meister<W, B>, Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
    {
        let meister = Meister { inner: self.inner, };
        meister.whip(thread_pool)?;
        Ok(meister)
    }
}

impl<W, B> Meister<W, B> {
    pub fn befehl<P, J>(&self, order: B, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
    {
        self.befehl_common(order, || self.whip(thread_pool))
    }

    fn befehl_common<F>(&self, order: B, whip: F) -> Result<(), Error>
    where F: FnOnce() -> Result<(), Error>,
    {
        let mut prev_tag = self.inner.touch_tag.load();
        loop {
            let decoded = TouchTag::decompose(prev_tag);
            if decoded.is_terminated {
                return Err(Error::Terminated);
            }

            let new_tag = TouchTag::compose(TouchTagDecoded {
                is_ready: false,
                orders_count: decoded.orders_count + 1,
                ..decoded
            });
            if let Err(changed_tag) = self.inner.touch_tag.try_set(prev_tag, new_tag) {
                prev_tag = changed_tag;
                continue;
            }
            if decoded.is_ready {
                whip()?;
            }

            self.inner.orders.push(order);
            return Ok(());
        }
    }

    fn whip<P, J>(&self, thread_pool: &P) -> Result<(), Error>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
    {
        let sklave_job = SklaveJob::new(self.inner.clone());
        edeltraud::job(thread_pool, sklave_job)
            .map_err(Error::Edeltraud)
    }
}


impl<W, B> SklaveJob<W, B> {
    pub fn zu_ihren_diensten(mut self) -> Result<Gehorsam<SklaveJob<W, B>>, Error> {
        let mut prev_tag = self.inner.touch_tag.load();
        loop {
            let decoded = TouchTag::decompose(prev_tag);
            assert!(!decoded.is_ready);
            if decoded.is_terminated {
                return Err(Error::Terminated);
            }
            if decoded.orders_count == 0 {
                let sklavenwelt = reach_sklavenwelt(&self.inner)
                    .as_ref()
                    .ok_or(Error::SklavenweltDropped)?;
                if !sklavenwelt.taken_orders.is_empty() {
                    return Ok(Gehorsam::Machen {
                        befehle: SklavenBefehle { sklave_job: self, },
                    });
                }

                let new_tag = TouchTag::compose(TouchTagDecoded {
                    is_ready: true,
                    orders_count: 0,
                    ..decoded
                });
                if let Err(changed_tag) = self.inner.touch_tag.try_set(prev_tag, new_tag) {
                    prev_tag = changed_tag;
                    continue;
                }

                self.rasten_mark = true;
                return Ok(Gehorsam::Rasten);
            } else {
                let new_tag = TouchTag::compose(TouchTagDecoded {
                    orders_count: decoded.orders_count - 1,
                    ..decoded
                });
                if let Err(changed_tag) = self.inner.touch_tag.try_set(prev_tag, new_tag) {
                    prev_tag = changed_tag;
                    continue;
                }
                let backoff = crossbeam::utils::Backoff::new();
                loop {
                    if let Some(order) = self.inner.orders.pop() {
                        let sklavenwelt_mut = reach_sklavenwelt_mut(&mut self.inner)
                            .as_mut()
                            .ok_or(Error::SklavenweltDropped)?;
                        sklavenwelt_mut.taken_orders.push_back(order);
                        break;
                    }
                    backoff.snooze();
                }
            }
        }
    }

    pub fn meister(&self) -> Meister<W, B> {
        Meister {
            inner: self.inner.clone(),
        }
    }
}

pub enum Gehorsam<S> {
    Machen {
        befehle: SklavenBefehle<S>,
    },
    Rasten,
}

pub struct SklavenBefehle<S> {
    sklave_job: S,
}

pub enum SklavenBefehl<S, B> {
    Mehr { befehl: B, mehr_befehle: SklavenBefehle<S>, },
    Ende { sklave_job: S, },
}

impl<W, B> SklavenBefehle<SklaveJob<W, B>> {
    pub fn befehl(mut self) -> SklavenBefehl<SklaveJob<W, B>, B> {
        let sklavenwelt_mut = reach_sklavenwelt_mut(&mut self.sklave_job.inner)
            .as_mut()
            .unwrap();
        match sklavenwelt_mut.taken_orders.pop_front() {
            Some(befehl) =>
                SklavenBefehl::Mehr { befehl, mehr_befehle: self, },
            None =>
                SklavenBefehl::Ende {
                    sklave_job: self.sklave_job,
                },
        }
    }
}

impl<W, B> Deref for SklaveJob<W, B> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        reach_sklavenwelt(&self.inner)
            .as_ref()
            .map(|s| &s.sklavenwelt)
            .unwrap()
    }
}

impl<W, B> DerefMut for SklaveJob<W, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        reach_sklavenwelt_mut(&mut self.inner)
            .as_mut()
            .map(|s| &mut s.sklavenwelt)
            .unwrap()
    }
}

impl<S> Deref for SklavenBefehle<S> {
    type Target = S;

    fn deref(&self) -> &Self::Target {
        &self.sklave_job
    }
}

impl<S> DerefMut for SklavenBefehle<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.sklave_job
    }
}

impl<W, B> Drop for SklaveJob<W, B> {
    fn drop(&mut self) {
        if !self.rasten_mark {
            let mut prev_tag = self.inner.touch_tag.load();
            loop {
                let decoded = TouchTag::decompose(prev_tag);
                if decoded.is_terminated {
                    break;
                }
                let new_tag = TouchTag::compose(TouchTagDecoded {
                    is_terminated: true,
                    ..decoded
                });
                if let Err(changed_tag) = self.inner.touch_tag.try_set(prev_tag, new_tag) {
                    prev_tag = changed_tag;
                    continue;
                }
                break;
            }

            // drop sklavenwelt
            let _sklavenwelt =
                reach_sklavenwelt_mut(&mut self.inner).take();
        }
    }
}

impl<W, B> Deref for Freie<W, B> {
    type Target = W;

    fn deref(&self) -> &Self::Target {
        reach_sklavenwelt(&self.inner)
            .as_ref()
            .map(|s| &s.sklavenwelt)
            .unwrap()
    }
}

impl<W, B> DerefMut for Freie<W, B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        reach_sklavenwelt_mut(&mut self.inner)
            .as_mut()
            .map(|s| &mut s.sklavenwelt)
            .unwrap()
    }
}
