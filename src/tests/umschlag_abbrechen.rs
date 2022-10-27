use std::{
    sync::{
        mpsc,
        Mutex,
    },
};

use crate::{
    komm::{
        Sendegeraet,
        UmschlagAbbrechen,
    },
    Freie,
    Gehorsam,
    SklaveJob,
    SklavenBefehl,
};

#[test]
fn basic() {
    #[derive(PartialEq, Eq, Debug)]
    struct Canceled;

    struct LocalStamp;

    struct LocalOrder(UmschlagAbbrechen<LocalStamp>);

    impl From<UmschlagAbbrechen<LocalStamp>> for LocalOrder {
        fn from(umschlag_abbrechen: UmschlagAbbrechen<LocalStamp>) -> LocalOrder {
            LocalOrder(umschlag_abbrechen)
        }
    }

    struct LocalJob(SklaveJob<Mutex<mpsc::Sender<Canceled>>, LocalOrder>);

    impl From<SklaveJob<Mutex<mpsc::Sender<Canceled>>, LocalOrder>> for LocalJob {
        fn from(sklave_job: SklaveJob<Mutex<mpsc::Sender<Canceled>>, LocalOrder>) -> LocalJob {
            LocalJob(sklave_job)
        }
    }

    impl edeltraud::Job for LocalJob {
        fn run<P>(self, _thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
            let LocalJob(mut sklave_job) = self;
            loop {
                match sklave_job.zu_ihren_diensten().unwrap() {
                    Gehorsam::Rasten =>
                        break,
                    Gehorsam::Machen { mut befehle, } =>
                        loop {
                            match befehle.befehl() {
                                SklavenBefehl::Mehr {
                                    befehl: LocalOrder(UmschlagAbbrechen { stamp: LocalStamp, }),
                                    mehr_befehle,
                                } => {
                                    befehle = mehr_befehle;
                                    let tx_lock = befehle
                                        .sklavenwelt()
                                        .lock()
                                        .unwrap();
                                    tx_lock.send(Canceled)
                                        .ok();
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
            }
        }
    }

    let thread_pool: edeltraud::Edeltraud<LocalJob> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let driver_freie = Freie::new();
    let sendegeraet = Sendegeraet::starten(&driver_freie, thread_pool.clone()).unwrap();

    let (tx, rx) = mpsc::channel();
    let _driver_meister =
        driver_freie.versklaven(Mutex::new(tx), &thread_pool).unwrap();

    let rueckkopplung = sendegeraet.rueckkopplung(LocalStamp);
    drop(rueckkopplung);

    assert_eq!(rx.recv_timeout(std::time::Duration::from_millis(100)), Ok(Canceled));
}
