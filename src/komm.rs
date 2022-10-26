use std::{
    fmt,
    ops::{
        Deref,
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
}

impl<B> Clone for Sendegeraet<B> {
    fn clone(&self) -> Self {
        Self { ewig_meister: self.ewig_meister.clone(), }
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

#[derive(Debug)]
pub struct Umschlag<I, S> {
    pub inhalt: I,
    pub stamp: S,
}

#[derive(Debug)]
pub struct UmschlagAbbrechen<S> {
    pub stamp: S,
}

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
        Ok(Sendegeraet { ewig_meister, })
    }

    pub fn rueckkopplung<S>(&self, stamp: S) -> Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
        Rueckkopplung {
            sendegeraet: self.clone(),
            maybe_stamp: Some(stamp),
        }
    }
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct EchoError;

pub struct EchoInhalt<T>(pub T);

pub trait TunInhalt<T> {
    fn tun(self) -> T;
}

impl<T> TunInhalt<T> for EchoInhalt<T> {
    fn tun(self) -> T {
        self.0
    }
}

pub trait Echo<I> {
    fn commit_echo<T>(self, inhalt: T) -> Result<(), EchoError> where EchoInhalt<T>: TunInhalt<I>;
}

impl<B, I, S> Echo<I> for Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>>, B: From<Umschlag<I, S>> {
    fn commit_echo<T>(self, inhalt: T) -> Result<(), EchoError> where EchoInhalt<T>: TunInhalt<I> {
        self.commit(EchoInhalt(inhalt).tun())
            .map_err(|_error| EchoError)
    }
}

pub enum Streamzeug<Z, ME> {
    NichtMehr,
    Zeug {
        zeug: Z,
        mehr_stream: ME,
    },
}

pub trait Stream<Z, ME>: Echo<Streamzeug<Z, ME>> + Sized where ME: Echo<Self> { }

impl<B, Z, S, ME> Stream<Z, ME> for Rueckkopplung<B, S>
where B: From<UmschlagAbbrechen<S>>,
      B: From<Umschlag<Streamzeug<Z, ME>, S>>,
      ME: Echo<Self>,
{
}

pub trait RueckkopplungWeg
where Self::Befehl: From<UmschlagAbbrechen<Self::Stamp>>,
      Self::Befehl: From<Umschlag<Self::Inhalt, Self::Stamp>>,
{
    type Stamp;
    type Inhalt;
    type Befehl;
}

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
