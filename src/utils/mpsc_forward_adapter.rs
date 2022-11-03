use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    ewig,
    Freie,
    Meister,
    Gehorsam,
    SklaveJob,
    SklavenBefehl,
};

#[derive(Debug)]
pub enum Error {
    Arbeitssklave(crate::Error),
    Ewig(ewig::Error),
    Disconnected,
}

impl From<ewig::Error> for Error {
    fn from(error: ewig::Error) -> Error {
        Error::Ewig(error)
    }
}

pub fn into<B, P>(
    freie: Freie<Welt<B>, B>,
    sync_sender: mpsc::SyncSender<B>,
    thread_pool: &P,
)
    -> Result<Meister<Welt<B>, B>, Error>
where P: edeltraud::ThreadPool<Job<B>>,
      B: Send + 'static,
{
    let ewig_freie = ewig::Freie::new();
    let ewig_meister =
        ewig_freie.versklaven(move |sklave| forward(sklave, &sync_sender))?;
    let meister = freie
        .versklaven(Welt { ewig_meister, }, thread_pool)
        .map_err(Error::Arbeitssklave)?;
    Ok(meister)
}

pub struct Welt<B> {
    ewig_meister: ewig::Meister<B, Error>,
}

fn forward<B>(sklave: &ewig::Sklave<B, Error>, sender: &mpsc::SyncSender<B>) -> Result<(), Error> {
    loop {
        for befehl in sklave.zu_ihren_diensten()? {
            if let Err(_send_error) = sender.send(befehl) {
                return Err(Error::Disconnected);
            }
        }
    }
}

pub enum Job<B> {
    Sklave(SklaveJob<Welt<B>, B>),
}

impl<B> From<SklaveJob<Welt<B>, B>> for Job<B> {
    fn from(job: SklaveJob<Welt<B>, B>) -> Job<B> {
        Job::Sklave(job)
    }
}

impl<B> edeltraud::Job for Job<B> where B: Send + 'static {
    fn run<P>(self, _thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::Sklave(mut sklave_job) =>
                loop {
                    match sklave_job.zu_ihren_diensten().unwrap() {
                        Gehorsam::Rasten =>
                            break,
                        Gehorsam::Machen { mut befehle, } =>
                            loop {
                                match befehle.befehl() {
                                    SklavenBefehl::Mehr { befehl, mehr_befehle, } => {
                                        befehle = mehr_befehle;
                                        let sklavenwelt = befehle.sklavenwelt();
                                        if let Err(send_error) = sklavenwelt.ewig_meister.befehl(befehl) {
                                            log::debug!("befehl forward failed: {send_error:?}");
                                            return;
                                        }
                                    },
                                    SklavenBefehl::Ende {
                                        sklave_job: next_sklave_job,
                                    } => {
                                        sklave_job = next_sklave_job;
                                        break;
                                    },
                                }
                            },
                    }
                },
        }
    }
}
