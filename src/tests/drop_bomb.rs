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
        fn from(job: SklaveJob<BombWelt, BombOrderTerminate>) -> Self {
            Self::Bomb(job)
        }
    }

    impl From<utils::mpsc_forward_adapter::Job<RecvOrderBoom>> for Job {
        fn from(job: utils::mpsc_forward_adapter::Job<RecvOrderBoom>) -> Self {
            Self::Recv(job)
        }
    }

    impl From<SklaveJob<utils::mpsc_forward_adapter::Welt<RecvOrderBoom>, RecvOrderBoom>> for Job {
        fn from(job: SklaveJob<utils::mpsc_forward_adapter::Welt<RecvOrderBoom>, RecvOrderBoom>) -> Self {
            Self::Recv(job.into())
        }
    }

    struct JobUnit<J>(edeltraud::JobUnit<J, Job>);

    impl<J> From<edeltraud::JobUnit<J, Job>> for JobUnit<J> {
        fn from(job_unit: edeltraud::JobUnit<J, Job>) -> Self {
            Self(job_unit)
        }
    }

    impl<J> edeltraud::Job for JobUnit<J> {
        fn run(self) {
            match self.0.job {
                Job::Recv(job) => {
                    let job_unit = utils::mpsc_forward_adapter::JobUnit::from(
                        edeltraud::JobUnit {
                            handle: self.0.handle,
                            job,
                        },
                    );
                    job_unit.run();
                },
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
        .build::<_, JobUnit<_>>()
        .unwrap();
    let thread_pool = edeltraud.handle();

    let (sync_tx, sync_rx) = mpsc::sync_channel(0);
    let adapter =
        utils::mpsc_forward_adapter::Adapter::versklaven(
            sync_tx,
            &thread_pool,
        )
        .unwrap();
    let sendegeraet = komm::Sendegeraet::starten(
        &adapter.sklave_meister,
        thread_pool.clone(),
    );
    let rueckkopplung = sendegeraet
        .rueckkopplung(Stamp);

    let bomb_meister = Freie::new()
        .versklaven(BombWelt { _drop_bomb: rueckkopplung, }, &thread_pool)
        .unwrap();

    bomb_meister.befehl(BombOrderTerminate, &thread_pool).unwrap();

    assert!(matches!(
        sync_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Ok(RecvOrderBoom),
    ));
}
