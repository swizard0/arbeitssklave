use std::{
    fmt,
    sync::{
        Arc,
        atomic::{
            Ordering,
            AtomicBool,
            AtomicUsize,
        },
    },
};

use crate::{
    Error,
    Meister,
    SklaveJob,
};

// Umschlag

#[derive(Debug)]
pub struct Umschlag<I, S> {
    pub inhalt: I,
    pub stamp: S,
}

// UmschlagAbbrechen

#[derive(Debug)]
pub struct UmschlagAbbrechen<S> {
    pub stamp: S,
}

// Rueckkopplung

pub struct Rueckkopplung<W, B, S, J> where B: From<UmschlagAbbrechen<S>>, J: From<SklaveJob<W, B>> {
    maybe_stamp: Option<S>,
    meister: Meister<W, B>,
    thread_pool: edeltraud::Handle<J>,
}

impl<W, B, S, J> Rueckkopplung<W, B, S, J> where B: From<UmschlagAbbrechen<S>>, J: From<SklaveJob<W, B>> {
    pub fn new(stamp: S, meister: Meister<W, B>, thread_pool: &edeltraud::Handle<J>) -> Self {
        Self {
            maybe_stamp: Some(stamp),
            meister,
            thread_pool: thread_pool.clone(),
        }
    }

    pub fn commit<I>(mut self, inhalt: I) -> Result<(), Error> where B: From<Umschlag<I, S>> {
        let stamp = self.maybe_stamp.take().unwrap();
        let umschlag = Umschlag { inhalt, stamp, };
        let order = umschlag.into();
        self.meister.befehl(order, &self.thread_pool)
    }
}

impl<W, B, S, J> Drop for Rueckkopplung<W, B, S, J> where B: From<UmschlagAbbrechen<S>>, J: From<SklaveJob<W, B>>, {
    fn drop(&mut self) {
        if let Some(stamp) = self.maybe_stamp.take() {
            let umschlag_abbrechen = UmschlagAbbrechen { stamp, };
            let order = umschlag_abbrechen.into();
            self.meister.befehl(order, &self.thread_pool).ok();
        }
    }
}

// Echo

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EchoError;

pub trait Echo<I> {
    fn commit_echo(self, inhalt: I) -> Result<(), EchoError>;
}

impl<W, B, I, S, J> Echo<I> for Rueckkopplung<W, B, S, J>
where B: From<UmschlagAbbrechen<S>>,
      B: From<Umschlag<I, S>>,
      J: From<SklaveJob<W, B>>,
{
    fn commit_echo(self, inhalt: I) -> Result<(), EchoError> {
        self.commit(inhalt)
            .map_err(|_error| EchoError)
    }
}

// Stream

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct StreamId {
    id: usize,
}

#[derive(Debug)]
pub struct StreamzeugNichtMehr {
    stream_id: StreamId,
}


impl StreamzeugNichtMehr {
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }
}

#[derive(Debug)]
pub struct StreamzeugMehr {
    token: StreamToken,
}

impl StreamzeugMehr {
    pub fn stream_id(&self) -> &StreamId {
        self.token.stream_id()
    }
}

impl From<StreamzeugMehr> for StreamToken {
    fn from(mehr: StreamzeugMehr) -> StreamToken {
        mehr.token
    }
}

#[derive(Debug)]
pub enum Streamzeug<Z> {
    NichtMehr(StreamzeugNichtMehr),
    Zeug {
        zeug: Z,
        mehr: StreamzeugMehr,
    },
}

pub struct StreamToken {
    stream_id: StreamId,
    cancellable: Arc<AtomicBool>,
}

impl StreamToken {
    pub(crate) fn new(stream_id: StreamId, cancellable: Arc<AtomicBool>) -> StreamToken {
        StreamToken { stream_id, cancellable, }
    }

    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn streamzeug_nicht_mehr<Z>(self) -> Streamzeug<Z> {
        self.cancellable.store(false, Ordering::SeqCst);
        Streamzeug::NichtMehr(StreamzeugNichtMehr { stream_id: self.stream_id, })
    }

    pub fn streamzeug_zeug<Z>(self, zeug: Z) -> Streamzeug<Z> {
        Streamzeug::Zeug { zeug, mehr: StreamzeugMehr { token: self, }, }
    }
}

#[derive(Debug)]
pub struct StreamStarten<I> {
    pub inhalt: I,
    pub stream_token: StreamToken,
}

#[derive(Debug)]
pub struct StreamMehr<I> {
    pub inhalt: I,
    pub stream_token: StreamToken,
}

#[derive(Debug)]
pub struct StreamAbbrechen {
    pub stream_id: StreamId,
}

pub struct Stream<W, B, J> where B: From<StreamAbbrechen>, J: From<SklaveJob<W, B>> {
    stream_id: StreamId,
    cancellable: Arc<AtomicBool>,
    meister: Meister<W, B>,
    thread_pool: edeltraud::Handle<J>,
}

impl<W, B, J> Stream<W, B, J> where B: From<StreamAbbrechen>, J: From<SklaveJob<W, B>> {
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn mehr<I>(&self, inhalt: I, stream_token: StreamToken) -> Result<(), Error> where B: From<StreamMehr<I>> {
        self.meister.befehl(StreamMehr { inhalt, stream_token, }.into(), &self.thread_pool)
    }
}

impl<W, B, J> Drop for Stream<W, B, J> where B: From<StreamAbbrechen>, J: From<SklaveJob<W, B>>  {
    fn drop(&mut self) {
        if self.cancellable.load(Ordering::SeqCst) {
            self.meister
                .befehl(
                    StreamAbbrechen { stream_id: self.stream_id.clone(), }.into(),
                    &self.thread_pool,
                )
                .ok();
        }
    }
}

#[derive(Default)]
pub struct StreamErbauer {
    stream_counter: Arc<AtomicUsize>,
}

impl StreamErbauer {
    pub fn stream_starten<I, W, B, J>(
        &self,
        inhalt: I,
        meister: Meister<W, B>,
        thread_pool: edeltraud::Handle<J>,
    )
        -> Result<Stream<W, B, J>, Error>
    where B: From<StreamStarten<I>>,
          B: From<StreamAbbrechen>,
          J: From<SklaveJob<W, B>>,
    {
        let id = self.stream_counter.fetch_add(1, Ordering::Relaxed);
        let stream_id = StreamId { id, };
        let cancellable = Arc::new(AtomicBool::new(true));
        let stream_token = StreamToken::new(
            stream_id.clone(),
            cancellable.clone(),
        );
        meister.befehl(
            StreamStarten { inhalt, stream_token, }.into(),
            &thread_pool,
        )?;

        Ok(Stream { stream_id, cancellable, meister, thread_pool, })
    }
}


// misc

impl<W, B, S, J> fmt::Debug for Rueckkopplung<W, B, S, J>
where B: From<UmschlagAbbrechen<S>>,
      S: fmt::Debug,
      J: From<SklaveJob<W, B>>,
{
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Rueckkopplung")
            .field("maybe_stamp", &self.maybe_stamp)
            .field("<hidden>", &"..")
            .finish()
    }
}

impl fmt::Debug for StreamToken {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("StreamToken")
            .field("stream_id", &self.stream_id)
            .finish()
    }
}
