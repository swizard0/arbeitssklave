
use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    Freie,
    Meister,
    Gehorsam,
    SklaveJob,
    SklavenBefehl,
};

#[test]
fn many_to_one() {
    const INCS_COUNT: usize = 131072;
    const JOBS_COUNT: usize = 8;

    enum ConsumerOrder {
        Register,
        Add(usize),
        Unregister,
    }

    enum Job {
        Feeder(FeederJob),
        Consumer(SklaveJob<ConsumerWelt, ConsumerOrder>),
    }

    impl From<FeederJob> for Job {
        fn from(job: FeederJob) -> Self {
            Self::Feeder(job)
        }
    }

    impl From<SklaveJob<ConsumerWelt, ConsumerOrder>> for Job {
        fn from(job: SklaveJob<ConsumerWelt, ConsumerOrder>) -> Self {
            Self::Consumer(job)
        }
    }

    struct FeederJob {
        consumer_meister: Meister<ConsumerWelt, ConsumerOrder>,
    }

    struct JobUnit<J>(edeltraud::JobUnit<J, Job>);

    impl<J> From<edeltraud::JobUnit<J, Job>> for JobUnit<J> {
        fn from(job_unit: edeltraud::JobUnit<J, Job>) -> Self {
            Self(job_unit)
        }
    }

    impl<J> edeltraud::Job for JobUnit<J> where J: From<SklaveJob<ConsumerWelt, ConsumerOrder>>, {
        fn run(self) {
            match self.0.job {
                Job::Feeder(FeederJob { consumer_meister, }) => {
                    fn job_loop<J>(
                        consumer_meister: &Meister<ConsumerWelt, ConsumerOrder>,
                        thread_pool: &edeltraud::Handle<J>,
                    )
                        -> Result<(), crate::Error>
                    where J: From<SklaveJob<ConsumerWelt, ConsumerOrder>>,
                    {
                        consumer_meister.befehl(ConsumerOrder::Register, thread_pool)?;
                        for _ in 0 .. INCS_COUNT {
                            consumer_meister.befehl(ConsumerOrder::Add(1), thread_pool)?;
                        }
                        consumer_meister.befehl(ConsumerOrder::Unregister, thread_pool)?;
                        Ok(())
                    }

                    job_loop(&consumer_meister, &self.0.handle).unwrap();
                },
                Job::Consumer(mut sklave_job) =>
                    loop {
                        let gehorsam = sklave_job.zu_ihren_diensten().unwrap();
                        match gehorsam {
                            Gehorsam::Rasten =>
                                break,
                            Gehorsam::Machen { mut befehle, } =>
                                loop {
                                    match befehle.befehl() {
                                        SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                            sklave_job = next_sklave_job;
                                            break;
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: ConsumerOrder::Register,
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = &mut *befehle;
                                            sklavenwelt.total_orders += 1;
                                            sklavenwelt.feeders_regs += 1;
                                            sklavenwelt.feeders_count += 1;
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: ConsumerOrder::Unregister,
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = &mut *befehle;
                                            sklavenwelt.total_orders += 1;
                                            assert!(sklavenwelt.feeders_count > 0);
                                            sklavenwelt.feeders_count -= 1;
                                            let all_orders_received = sklavenwelt.total_orders >= JOBS_COUNT * INCS_COUNT + (JOBS_COUNT * 2);
                                            let all_feeders_registered = sklavenwelt.feeders_regs >= JOBS_COUNT;
                                            let all_feeders_unregistered = sklavenwelt.feeders_count == 0;
                                            if all_orders_received && all_feeders_registered && all_feeders_unregistered {
                                                sklavenwelt.done_tx.send(sklavenwelt.local_counter).unwrap();
                                                return;
                                            }
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: ConsumerOrder::Add(value),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = &mut *befehle;
                                            sklavenwelt.total_orders += 1;
                                            sklavenwelt.local_counter += value;
                                        },
                                    }
                                },
                        }
                    },
            }
        }
    }

    struct ConsumerWelt {
        total_orders: usize,
        feeders_regs: usize,
        feeders_count: usize,
        local_counter: usize,
        done_tx: mpsc::Sender<usize>,
    }

    let edeltraud: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build::<_, JobUnit<_>>()
        .unwrap();
    let thread_pool = edeltraud.handle();

    let (done_tx, done_rx) = mpsc::channel();
    let consumer_meister = Freie::new()
        .versklaven(
            ConsumerWelt {
                total_orders: 0,
                feeders_regs: 0,
                feeders_count: 0,
                local_counter: 0,
                done_tx,
            },
            &thread_pool,
        )
        .unwrap();

    for _ in 0 .. JOBS_COUNT {
        edeltraud::job(&thread_pool, Job::Feeder(FeederJob { consumer_meister: consumer_meister.clone(), }))
            .unwrap();
    }

    assert_eq!(done_rx.recv(), Ok(JOBS_COUNT * INCS_COUNT));
}
