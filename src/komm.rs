use std::{
    fmt,
    ops::{
        Deref,
    },
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

pub struct Sendegeraet<B> {
    ewig_meister: ewig::Meister<B, Error>,
    stream_counter: Arc<AtomicUsize>,
}

impl<B> Clone for Sendegeraet<B> {
    fn clone(&self) -> Self {
        Self {
            ewig_meister: self.ewig_meister.clone(),
            stream_counter: self.stream_counter.clone(),
        }
    }
}

impl<B> Deref for Sendegeraet<B> {
    type Target = ewig::Meister<B, Error>;

    fn deref(&self) -> &Self::Target {
        &self.ewig_meister
    }
}

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

impl<B> Sendegeraet<B> {
    pub fn starten<W, P, J>(freie: &Freie<W, B>, thread_pool: P) -> Result<Self, Error>
    where P: edeltraud::ThreadPool<J> + Send + 'static,
          J: edeltraud::Job + From<SklaveJob<W, B>>,
          W: Send + 'static,
          B: Send + 'static,
    {
        let meister = Meister { inner: freie.inner.clone(), };
        let ewig_meister = ewig::Freie::new()
            .versklaven_als(
                "arbeitssklave::komm::Sendegeraet".to_string(),
                move |sklave| sendegeraet_loop(sklave, &meister, &thread_pool),
            )?;
        let stream_counter = Arc::new(AtomicUsize::new(0));
        Ok(Sendegeraet { ewig_meister, stream_counter, })
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

pub struct Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
    sendegeraet: Sendegeraet<B>,
    maybe_stamp: Option<S>,
}

impl<B, S> Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
    pub fn commit<I>(mut self, inhalt: I) -> Result<(), Error> where B: From<Umschlag<I, S>> {
        let stamp = self.maybe_stamp.take().unwrap();
        let umschlag = Umschlag { inhalt, stamp, };
        let order = umschlag.into();
        self.sendegeraet.befehl(order)
    }
}

impl<B, S> Drop for Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
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

impl<B, I, S> Echo<I> for Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>>, B: From<Umschlag<I, S>> {
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
    _marker: PhantomData<()>,
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
        Streamzeug::NichtMehr(StreamzeugNichtMehr { _marker: PhantomData, })
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

pub struct Stream<B> where B: From<StreamAbbrechen> {
    sendegeraet: Sendegeraet<B>,
    stream_id: StreamId,
    cancellable: Arc<AtomicBool>,
}

impl<B> Stream<B> where B: From<StreamAbbrechen> {
    pub fn stream_id(&self) -> &StreamId {
        &self.stream_id
    }

    pub fn mehr<I>(&self, inhalt: I, stream_token: StreamToken) -> Result<(), Error> where B: From<StreamMehr<I>> {
        self.sendegeraet.befehl(StreamMehr { inhalt, stream_token, }.into())
    }
}

impl<B> Drop for Stream<B> where B: From<StreamAbbrechen> {
    fn drop(&mut self) {
        if self.cancellable.load(Ordering::SeqCst) {
            self.sendegeraet.befehl(StreamAbbrechen { stream_id: self.stream_id.clone(), }.into()).ok();
        }
    }
}

// sendegeraet_loop

fn sendegeraet_loop<W, B, P, J>(
    sklave: &ewig::Sklave<B, Error>,
    meister: &Meister<W, B>,
    thread_pool: &P,
)
    -> Result<(), Error>
where P: edeltraud::ThreadPool<J> + Send + 'static,
      J: edeltraud::Job + From<SklaveJob<W, B>>,
      W: Send + 'static,
      B: Send + 'static,
{
    loop {
        let orders = sklave.zu_ihren_diensten()?;
        meister.befehle(orders, thread_pool)
            .map_err(Error::Meister)?;
    }
}

impl<B, S> fmt::Debug for Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>>, S: fmt::Debug {
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
