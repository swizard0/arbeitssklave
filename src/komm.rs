use std::{
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
}

impl From<ewig::Error> for Error {
    fn from(error: ewig::Error) -> Error {
        Error::Ewig(error)
    }
}

pub struct Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
    sendegeraet: Sendegeraet<B>,
    maybe_stamp: Option<S>,
}

pub struct Umschlag<T, S> {
    pub payload: T,
    pub stamp: S,
}

pub struct UmschlagAbbrechen<S> {
    pub stamp: S,
}

impl<B> Sendegeraet<B> {
    pub fn starten<W, P, J>(freie: &Freie<W, B>, thread_pool: P) -> Result<Self, Error>
    where P: edeltraud::ThreadPool<J> + Send + 'static,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
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

impl<B, S> Rueckkopplung<B, S> where B: From<UmschlagAbbrechen<S>> {
    pub fn commit<T>(mut self, payload: T) -> Result<(), Error> where B: From<Umschlag<T, S>> {
        let stamp = self.maybe_stamp.take().unwrap();
        let umschlag = Umschlag { payload, stamp, };
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

fn sendegeraet_loop<W, B, P, J>(
    sklave: &mut ewig::Sklave<B, Error>,
    meister: &Meister<W, B>,
    thread_pool: &P,
)
    -> Result<(), Error>
where P: edeltraud::ThreadPool<J> + Send + 'static,
      J: edeltraud::Job<Output = ()> + From<SklaveJob<W, B>>,
      W: Send + 'static,
      B: Send + 'static,
{
    loop {
        let orders = sklave.zu_ihren_diensten()?;
        meister.befehle(orders, thread_pool)
            .map_err(Error::Meister)?;
    }
}
