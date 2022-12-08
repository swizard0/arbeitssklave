use std::{
    fmt,
    sync::{
        Arc,
        Weak,
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
    Inner as ArbeitssklaveInner,
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

// Echo

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EchoError;

pub trait Echo<I> {
    fn commit_echo(self, inhalt: I) -> Result<(), EchoError>;
}

impl<B, I, S> Echo<I> for Rueckkopplung<B, S>
where B: From<UmschlagAbbrechen<S>>,
      B: From<Umschlag<I, S>>,
{
    fn commit_echo(self, inhalt: I) -> Result<(), EchoError> {
        self.commit(inhalt)
            .map_err(|_error| EchoError)
    }
}

// Rueckkopplung

pub struct Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> + 'static {
    maybe_stamp: Option<S>,
    sendegeraet: Sendegeraet<B>,
}

impl<B, S> Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
    pub fn commit<I>(mut self, inhalt: I) -> Result<(), Error> where B: From<Umschlag<I, S>> {
        let stamp = self.maybe_stamp.take().unwrap();
        let umschlag = Umschlag { inhalt, stamp, };
        let order = umschlag.into();
        self.sendegeraet.meister.befehl(order)
    }
}

impl<B, S> Drop for Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
    fn drop(&mut self) {
        if let Some(stamp) = self.maybe_stamp.take() {
            let umschlag_abbrechen = UmschlagAbbrechen { stamp, };
            let order = umschlag_abbrechen.into();
            self.sendegeraet.meister.befehl(order).ok();
        }
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

pub struct Stream<B> where B: From<StreamAbbrechen> + 'static {
    stream_id: StreamId,
    cancellable: Arc<AtomicBool>,
    sendegeraet: Sendegeraet<B>,
}

impl<B> Stream<B> where B: From<StreamAbbrechen> {
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn mehr<I>(&self, inhalt: I, stream_token: StreamToken) -> Result<(), Error> where B: From<StreamMehr<I>> {
        self.sendegeraet.meister.befehl(StreamMehr { inhalt, stream_token, }.into())
    }
}

impl<B> Drop for Stream<B> where B: From<StreamAbbrechen> {
    fn drop(&mut self) {
        if self.cancellable.load(Ordering::SeqCst) {
            self.sendegeraet
                .meister
                .befehl(StreamAbbrechen { stream_id: self.stream_id.clone(), }.into())
                .ok();
        }
    }
}

// StreamErbauer

#[derive(Clone, Default)]
pub struct StreamErbauer {
    stream_counter: Arc<AtomicUsize>,
}

// SendegeraetMeister

trait SendegeraetMeister<B> where Self: Send + Sync + 'static {
    fn befehl(&self, order: B) -> Result<(), Error>;
}

// SendegeraetInner

struct SendegeraetInner<W, B, J> {
    schwach_meister: SchwachMeister<W, B>,
    thread_pool: edeltraud::Handle<J>,
}

impl<W, B, J> SendegeraetMeister<B> for SendegeraetInner<W, B, J>
where J: From<SklaveJob<W, B>> + Send + 'static,
      W: Send + 'static,
      B: Send + 'static,
{
    fn befehl(&self, order: B) -> Result<(), Error> {
        self.schwach_meister.befehl(order, &self.thread_pool)
    }
}

// Sendegeraet

pub struct Sendegeraet<B> {
    meister: Arc<dyn SendegeraetMeister<B>>,
}

impl<B> Sendegeraet<B> where B: Send + 'static {
    pub fn starten<W, J>(
        meister: &Meister<W, B>,
        thread_pool: edeltraud::Handle<J>,
    )
        -> Self
    where J: From<SklaveJob<W, B>> + Send + 'static,
          W: Send + 'static,
    {
        let inner =
            SendegeraetInner {
                schwach_meister: SchwachMeister {
                    maybe_inner: Arc::downgrade(&meister.inner),
                },
                thread_pool,
            };
        Sendegeraet {
            meister: Arc::new(inner),
        }
    }

    pub fn rueckkopplung<S>(&self, stamp: S) -> Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
        Rueckkopplung {
            sendegeraet: self.clone(),
            maybe_stamp: Some(stamp),
        }
    }

    pub fn stream_starten<I>(&self, stream_erbauer: &StreamErbauer, inhalt: I) -> Result<Stream<B>, Error>
    where B: From<StreamStarten<I>>,
          B: From<StreamAbbrechen>,
    {
        let id = stream_erbauer
            .stream_counter
            .fetch_add(1, Ordering::Relaxed);
        let stream_id = StreamId { id, };
        let cancellable = Arc::new(AtomicBool::new(true));
        let stream_token = StreamToken::new(
            stream_id.clone(),
            cancellable.clone(),
        );
        let order = StreamStarten { inhalt, stream_token, }.into();
        self.meister.befehl(order)?;

        Ok(Stream { sendegeraet: self.clone(), stream_id, cancellable, })
    }
}

impl<B> Clone for Sendegeraet<B> {
    fn clone(&self) -> Self {
        Self {
            meister: self.meister.clone(),
        }
    }
}

// WeakMeister

struct SchwachMeister<W, B> {
    maybe_inner: Weak<ArbeitssklaveInner<W, B>>,
}

impl<W, B> SchwachMeister<W, B> {
    fn befehl<J>(&self, order: B, thread_pool: &edeltraud::Handle<J>) -> Result<(), Error> where J: From<SklaveJob<W, B>> {
        let inner = self.maybe_inner.upgrade()
            .ok_or(Error::Terminated)?;
        inner.befehl(order, thread_pool)
    }
}

// misc

impl<B, S> fmt::Debug for Rueckkopplung<B, S>
where B: From<UmschlagAbbrechen<S>>,
      S: fmt::Debug,
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
