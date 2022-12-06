use std::{
    sync::{
        atomic,
        Arc,
        Barrier,
    },
};

use crate::{
    ewig,
};

#[test]
fn stress_8() {
    const INCS_COUNT: usize = 131072;
    const JOBS_COUNT: usize = 8;

    enum ConsumerOrder {
        Register,
        Add(usize),
        Unregister,
    }

    #[derive(Debug)]
    struct ConsumerError(ewig::Error);

    impl From<ewig::Error> for ConsumerError {
        fn from(error: ewig::Error) -> Self {
            Self(error)
        }
    }

    struct FeederJob {
        consumer_meister: ewig::Meister<ConsumerOrder, ConsumerError>,
    }

    struct JobUnit<J>(edeltraud::JobUnit<J, FeederJob>);

    impl<J> From<edeltraud::JobUnit<J, FeederJob>> for JobUnit<J> {
        fn from(job_unit: edeltraud::JobUnit<J, FeederJob>) -> Self {
            Self(job_unit)
        }
    }

    impl<J> edeltraud::Job for JobUnit<J> {
        fn run(self) {
            fn job_loop(consumer_meister: &ewig::Meister<ConsumerOrder, ConsumerError>) -> Result<(), ConsumerError> {
                consumer_meister.befehl(ConsumerOrder::Register)?;
                for _ in 0 .. INCS_COUNT {
                    consumer_meister.befehl(ConsumerOrder::Add(1))?;
                }
                consumer_meister.befehl(ConsumerOrder::Unregister)?;
                Ok(())
            }

            job_loop(&self.0.job.consumer_meister).unwrap();
        }
    }

    let global_counter = Arc::new(atomic::AtomicUsize::new(0));
    let ewig_counter = global_counter.clone();
    let global_barrier = Arc::new(Barrier::new(2));
    let ewig_barrier = global_barrier.clone();

    let consumer_meister = ewig::Freie::new()
        .versklaven(move |sklave| {
            let mut feeders_count = 0;
            loop {
                let orders = sklave.zu_ihren_diensten()?;
                for order in orders {
                    match order {
                        ConsumerOrder::Register =>
                            feeders_count += 1,
                        ConsumerOrder::Unregister if feeders_count < 2 => {
                            ewig_barrier.wait();
                            return Ok(());
                        },
                        ConsumerOrder::Unregister =>
                            feeders_count -= 1,
                        ConsumerOrder::Add(value) => {
                            ewig_counter.fetch_add(value, atomic::Ordering::Relaxed);
                        },
                    }
                }
            }
        })
        .unwrap();

    let edeltraud = edeltraud::Builder::new()
        .build::<_, JobUnit<_>>()
        .unwrap();
    let thread_pool = edeltraud.handle();

    for _ in 0 .. JOBS_COUNT {
        edeltraud::job(&thread_pool, FeederJob { consumer_meister: consumer_meister.clone(), }).unwrap();
    }

    global_barrier.wait();
    assert_eq!(global_counter.load(atomic::Ordering::SeqCst), JOBS_COUNT * INCS_COUNT);
}
