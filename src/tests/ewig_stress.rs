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
    const INCS_COUNT: usize = 131072 * 2;
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

    impl edeltraud::Job for FeederJob {
        fn run<P>(self, _thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
            fn job_loop(consumer_meister: &ewig::Meister<ConsumerOrder, ConsumerError>) -> Result<(), ConsumerError> {
                consumer_meister.befehl(ConsumerOrder::Register)?;
                for _ in 0 .. INCS_COUNT {
                    consumer_meister.befehl(ConsumerOrder::Add(1))?;
                }
                consumer_meister.befehl(ConsumerOrder::Unregister)?;
                Ok(())
            }

            job_loop(&self.consumer_meister).unwrap();
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

    let thread_pool: edeltraud::Edeltraud<FeederJob> = edeltraud::Builder::new()
        .build()
        .unwrap();

    for _ in 0 .. JOBS_COUNT {
        edeltraud::ThreadPool::spawn(&thread_pool, FeederJob { consumer_meister: consumer_meister.clone(), }).unwrap();
    }

    global_barrier.wait();
    assert_eq!(global_counter.load(atomic::Ordering::SeqCst), JOBS_COUNT * INCS_COUNT);
}
