use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    komm,
    utils,
    Freie,
    Gehorsam,
    SklaveJob,
    SklavenBefehl,
};

#[test]
fn drop_bomb_with_umschlag_abbrechen() {
    struct RecvOrderBoom;

    struct Stamp;

    impl From<komm::UmschlagAbbrechen<Stamp>> for RecvOrderBoom {
        fn from(_umschlag_abbrechen: komm::UmschlagAbbrechen<Stamp>) -> RecvOrderBoom {
            RecvOrderBoom
        }
    }

    struct BombOrderTerminate;

    struct BombWelt {
        _drop_bomb: komm::Rueckkopplung<RecvOrderBoom, Stamp>,
    }

    enum Job {
        Bomb(SklaveJob<BombWelt, BombOrderTerminate>),
        Recv(utils::mpsc_forward_adapter::Job<RecvOrderBoom>),
    }

    impl From<SklaveJob<BombWelt, BombOrderTerminate>> for Job {
        fn from(job: SklaveJob<BombWelt, BombOrderTerminate>) -> Job {
            Job::Bomb(job)
        }
    }

    impl From<utils::mpsc_forward_adapter::Job<RecvOrderBoom>> for Job {
        fn from(job: utils::mpsc_forward_adapter::Job<RecvOrderBoom>) -> Job {
            Job::Recv(job)
        }
    }

    impl edeltraud::Job for Job {
        fn run<P>(self, thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
            match self {
                Job::Recv(job) =>
                    job.run(&edeltraud::ThreadPoolMap::new(thread_pool)),
                Job::Bomb(mut sklave_job) =>
                    loop {
                        match sklave_job.zu_ihren_diensten() {
                            Ok(Gehorsam::Rasten) =>
                                return,
                            Ok(Gehorsam::Machen { befehle, }) => {
                                #[allow(clippy::never_loop)]
                                loop {
                                    match befehle.befehl() {
                                        SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                            sklave_job = next_sklave_job;
                                            break;
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: BombOrderTerminate,
                                            ..
                                        } => {
                                            // bomb should trigger here
                                            return;
                                        },
                                    }
                                }
                            },
                            Err(error) => {
                                log::error!("zu_ihren_diensten terminated with {error:?}");
                                return;
                            },
                        }
                    },
            }
        }
    }

    let edeltraud: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build()
        .unwrap();
    let thread_pool = edeltraud.handle();

    let (sync_tx, sync_rx) = mpsc::sync_channel(0);
    let adapter =
        utils::mpsc_forward_adapter::Adapter::versklaven(
            sync_tx,
            &edeltraud::ThreadPoolMap::new(thread_pool.clone()),
        )
        .unwrap();
    let rueckkopplung = adapter
        .sklave_meister
        .sendegeraet()
        .rueckkopplung(Stamp);

    let bomb_meister =
        Freie::new(
            BombWelt { _drop_bomb: rueckkopplung, },
        )
        .versklaven(&thread_pool)
        .unwrap();

    bomb_meister.befehl(BombOrderTerminate, &thread_pool).unwrap();

    assert!(matches!(
        sync_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Ok(RecvOrderBoom),
    ));
}
