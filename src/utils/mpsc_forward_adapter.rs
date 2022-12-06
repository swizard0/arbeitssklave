use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    ewig,
    Meister,
    Gehorsam,
    SklaveJob,
    SklavenBefehl,
};

#[derive(Debug)]
pub enum Error {
    Versklaven(crate::Error),
    Ewig(ewig::Error),
    Disconnected,
}

impl From<ewig::Error> for Error {
    fn from(error: ewig::Error) -> Error {
        Error::Ewig(error)
    }
}

pub struct Adapter<B> {
    pub sklave_meister: Meister<Welt<B>, B>,
}

impl<B> Adapter<B> {
    pub fn versklaven<J>(
        sync_tx: mpsc::SyncSender<B>,
        thread_pool: &edeltraud::Handle<J>,
    )
        -> Result<Adapter<B>, Error>
    where J: From<SklaveJob<Welt<B>, B>>,
          B: Send + 'static,
    {
        let ewig_freie = ewig::Freie::new();
        let ewig_meister =
            ewig_freie.versklaven(move |sklave| {
                forward(sklave, &sync_tx)
            })?;
        let sklave_freie = crate::Freie::new();
        let sklave_meister = sklave_freie
            .versklaven(Welt { ewig_meister, }, thread_pool)
            .map_err(Error::Versklaven)?;
        Ok(Adapter { sklave_meister, })
    }
}

pub struct Welt<B> {
    ewig_meister: ewig::Meister<B, Error>,
}

fn forward<B>(sklave: &mut ewig::Sklave<B, Error>, sync_tx: &mpsc::SyncSender<B>) -> Result<(), Error> {
    loop {
        for befehl in sklave.zu_ihren_diensten()? {
            if let Err(_send_error) = sync_tx.send(befehl) {
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

pub struct JobUnit<B, J>(edeltraud::JobUnit<J, Job<B>>);

impl<B, J> From<edeltraud::JobUnit<J, Job<B>>> for JobUnit<B, J> {
    fn from(job_unit: edeltraud::JobUnit<J, Job<B>>) -> Self {
        Self(job_unit)
    }
}

impl<B, J> edeltraud::Job for JobUnit<B, J> {
    fn run(self) {
        match self.0.job {
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
                                        if let Err(send_error) = befehle.ewig_meister.befehl(befehl) {
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
