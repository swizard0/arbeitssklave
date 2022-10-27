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
    marker::{
        PhantomData,
    },
};

use crate::{
    ewig,
    Freie,
    Meister,
    SklaveJob,
    Error as ArbeitssklaveError,
};

#[derive(Debug)]
pub enum Error {
    Ewig(ewig::Error),
    Meister(ArbeitssklaveError),
    Edeltraud(edeltraud::SpawnError),
}

impl From<ewig::Error> for Error {
    fn from(error: ewig::Error) -> Error {
        Error::Ewig(error)
    }
}

// Sendegeraet

pub struct Sendegeraet<B> {
    meister: Arc<dyn SendegeraetMeister<B>>,
    stream_counter: Arc<AtomicUsize>,
}

trait SendegeraetMeister<B> where Self: Send + Sync + 'static {
    fn befehl(&self, order: B) -> Result<(), Error>;
}

struct SendegeraetInner<W, B, P, J> {
    meister: Meister<W, B>,
    thread_pool: P,
    _marker: PhantomData<J>,
}

impl<W, B, P, J> SendegeraetMeister<B> for SendegeraetInner<W, B, P, J>
where P: edeltraud::ThreadPool<J> + Send + Sync + 'static,
      J: edeltraud::Job + From<SklaveJob<W, B>> + Sync,
      W: Send + 'static,
      B: Send + 'static,
{
    fn befehl(&self, order: B) -> Result<(), Error> {
        self.meister.befehl(order, &self.thread_pool)
            .map_err(Error::Meister)
    }
}

impl<B> Sendegeraet<B> where B: Send + 'static {
    pub fn starten<W, P, J>(freie: &Freie<W, B>, thread_pool: P) -> Result<Self, Error>
    where P: edeltraud::ThreadPool<J> + Send + Sync + 'static,
          J: edeltraud::Job + From<SklaveJob<W, B>> + Sync,
          W: Send + 'static,
    {
        let meister = Meister { inner: freie.inner.clone(), };
        let stream_counter = Arc::new(AtomicUsize::new(0));
        let inner = SendegeraetInner {
            meister,
            thread_pool,
            _marker: PhantomData,
        };
        Ok(Sendegeraet {
            meister: Arc::new(inner),
            stream_counter,
        })
    }

    pub fn befehl(&self, order: B) -> Result<(), Error> {
        self.meister.befehl(order)
    }

    pub fn rueckkopplung<S>(&self, stamp: S) -> Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
        Rueckkopplung {
            sendegeraet: self.clone(),
            maybe_stamp: Some(stamp),
        }
    }

    pub fn stream_starten<I>(&self, inhalt: I) -> Result<Stream<B>, Error>
    where B: From<StreamStarten<I>>,
          B: From<StreamAbbrechen>,
    {
        let id = self.stream_counter.fetch_add(1, Ordering::Relaxed);
        let stream_id = StreamId { id, };
        let cancellable = Arc::new(AtomicBool::new(true));
        let stream_token = StreamToken::new(
            stream_id.clone(),
            cancellable.clone(),
        );
        let order = StreamStarten { inhalt, stream_token, }.into();
        self.befehl(order)?;

        Ok(Stream {
            sendegeraet: self.clone(),
            stream_id,
            cancellable,
        })
    }
}

impl<B> Clone for Sendegeraet<B> {
    fn clone(&self) -> Self {
        Self {
            meister: self.meister.clone(),
            stream_counter: self.stream_counter.clone(),
        }
    }
}

// Rueckkopplung

#[derive(Debug)]
pub struct Umschlag<I, S> {
    pub inhalt: I,
    pub stamp: S,
}

#[derive(Debug)]
pub struct UmschlagAbbrechen<S> {
    pub stamp: S,
}

pub struct Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> + Send + 'static {
    sendegeraet: Sendegeraet<B>,
    maybe_stamp: Option<S>,
}

impl<B, S> Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> + Send + 'static {
    pub fn commit<I>(mut self, inhalt: I) -> Result<(), Error> where B: From<Umschlag<I, S>> {
        let stamp = self.maybe_stamp.take().unwrap();
        let umschlag = Umschlag { inhalt, stamp, };
        let order = umschlag.into();
        self.sendegeraet.befehl(order)
    }
}

impl<B, S> Drop for Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> + Send + 'static {
    fn drop(&mut self) {
        if let Some(stamp) = self.maybe_stamp.take() {
            let umschlag_abbrechen = UmschlagAbbrechen { stamp, };
            let order = umschlag_abbrechen.into();
            self.sendegeraet.befehl(order).ok();
        }
    }
}

// Echo

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EchoError;

pub trait Echo<I> {
    fn commit_echo(self, inhalt: I) -> Result<(), EchoError>;
}

impl<B, I, S> Echo<I> for Rueckkopplung<B, S>
where B: From<UmschlagAbbrechen<S>>,
      B: From<Umschlag<I, S>>,
      B: Send + 'static,
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

pub struct StreamzeugNichtMehr {
    stream_id: StreamId,
}


impl StreamzeugNichtMehr {
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }
}

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

pub struct Stream<B> where B: From<StreamAbbrechen> + Send + 'static {
    sendegeraet: Sendegeraet<B>,
    stream_id: StreamId,
    cancellable: Arc<AtomicBool>,
}

impl<B> Stream<B> where B: From<StreamAbbrechen> + Send + 'static {
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn mehr<I>(&self, inhalt: I, stream_token: StreamToken) -> Result<(), Error> where B: From<StreamMehr<I>> {
        self.sendegeraet.befehl(StreamMehr { inhalt, stream_token, }.into())
    }
}

impl<B> Drop for Stream<B> where B: From<StreamAbbrechen> + Send + 'static {
    fn drop(&mut self) {
        if self.cancellable.load(Ordering::SeqCst) {
            self.sendegeraet.befehl(StreamAbbrechen { stream_id: self.stream_id.clone(), }.into()).ok();
        }
    }
}

// misc

impl<B, S> fmt::Debug for Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> + Send + 'static, S: fmt::Debug {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Rueckkopplung")
            .field("sendegeraet", &"<Sendegeraet>")
            .field("maybe_stamp", &self.maybe_stamp)
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
