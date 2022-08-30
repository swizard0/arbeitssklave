use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    Obey,
    Meister,
    Umschlag,
    SklaveJob,
    Rueckkopplung,
};

#[test]
fn mutual_recursive() {
    let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build()
        .unwrap();
    let odd_meister = even::start(&edeltraud::EdeltraudJobMap::<_, _, odd::Job>::new(&thread_pool));
    let even_meister = even::start(&edeltraud::EdeltraudJobMap::<_, _, even::Job>::new(&thread_pool));

    let driver_meister = Meister::start(Welt { odd_meister, even_meister, }, thread_pool)
        .unwrap();

    let (reply_tx, reply_rx) = mpsc::channel();
    driver_meister.order(Order::Calc { value: 13, reply_tx, }, &thread_pool).unwrap();
    let result = reply_rx.recv().unwrap();
    assert_eq!(result, ValueType::Odd);
}

enum Job {
    Odd(odd::Job<Welt, Order, Stamp>),
    Even(even::Job<Welt, Order, Stamp>),
    Driver(SklaveJob<Welt, Order>),
}

impl From<SklaveJob<Welt, Order>> for Job {
    fn from(job: SklaveJob<Welt, Order>) -> Job {
        Job::Driver(job)
    }
}

impl From<odd::Job<Welt, Order, Stamp>> for Job {
    fn from(job: odd::Job<Welt, Order, Stamp>) -> Job {
        Job::Odd(job)
    }
}

impl From<even::Job<Welt, Order, Stamp>> for Job {
    fn from(job: even::Job<Welt, Order, Stamp>) -> Job {
        Job::Even(job)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum ValueType {
    Even,
    Odd,
}

impl ValueType {
    fn neg(self) -> ValueType {
        match self {
            ValueType::Even =>
                ValueType::Odd,
            ValueType::Odd =>
                ValueType::Even,
        }
    }
}

struct Welt {
    odd_meister: Meister<odd::Welt, odd::Order<Welt, Order, Stamp>>,
    even_meister: Meister<even::Welt, even::Order<Welt, Order, Stamp>>,
}

enum Order {
    Calc {
        value: isize,
        reply_tx: mpsc::Sender<ValueType>,
    },
    OddUmschlag(Umschlag<odd::Outcome, Stamp>),
    EvenUmschlag(Umschlag<even::Outcome, Stamp>),
}

impl From<Umschlag<odd::Outcome, Stamp>> for Order {
    fn from(umschlag: Umschlag<odd::Outcome, Stamp>) -> Order {
        Order::OddUmschlag(umschlag)
    }
}

impl From<Umschlag<even::Outcome, Stamp>> for Order {
    fn from(umschlag: Umschlag<even::Outcome, Stamp>) -> Order {
        Order::EvenUmschlag(umschlag)
    }
}

struct Stamp {
    current_value: isize,
    current_guess: ValueType,
    reply_tx: mpsc::Sender<ValueType>,
}

impl edeltraud::Job for Job {
    type Output = ();

    fn run<P>(self, thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::Odd(job) => {
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
            Job::Even(job) => {
                job.run(&edeltraud::EdeltraudJobMap::new(thread_pool));
            },
            Job::Driver(SklaveJob { mut sklave, mut sklavenwelt, }) =>
                loop {
                    match sklave.obey(sklavenwelt).unwrap() {
                        Obey::Order { order: Order::Calc { value, reply_tx, }, sklavenwelt: next_sklavenwelt, } => {
                            sklavenwelt = next_sklavenwelt;
                            sklavenwelt.even_meister.order(
                                even::Order::Is {
                                    value,
                                    rueckkopplung: Rueckkopplung::new(
                                        sklave.meister().unwrap(),
                                        Stamp {
                                            current_value: value,
                                            current_guess: ValueType::Even,
                                            reply_tx,
                                        },
                                    ),
                                },
                                &edeltraud::EdeltraudJobMap::<_, _, even::Job<_, _, _>>::new(thread_pool),
                            ).unwrap();
                        },
                        Obey::Order {
                            order: Order::OddUmschlag(Umschlag { value: odd::Outcome::False, stamp: Stamp { current_guess, reply_tx, .. }, }),
                            sklavenwelt: next_sklavenwelt,
                        } => {
                            sklavenwelt = next_sklavenwelt;
                            reply_tx.send(current_guess.neg()).unwrap();
                        },
                        Obey::Order {
                            order: Order::OddUmschlag(Umschlag {
                                value: odd::Outcome::NotSure,
                                stamp: Stamp {
                                    current_value,
                                    current_guess,
                                    reply_tx,
                                },
                            }),
                            sklavenwelt: next_sklavenwelt,
                        } => {
                            sklavenwelt = next_sklavenwelt;
                            sklavenwelt.even_meister.order(
                                even::Order::Is {
                                    value: current_value - 1,
                                    rueckkopplung: Rueckkopplung::new(
                                        sklave.meister().unwrap(),
                                        Stamp {
                                            current_value: current_value - 1,
                                            current_guess: current_guess.neg(),
                                            reply_tx,
                                        },
                                    ),
                                },
                                &edeltraud::EdeltraudJobMap::<_, _, even::Job<_, _, _>>::new(thread_pool),
                            ).unwrap();
                        },
                        Obey::Order {
                            order: Order::EvenUmschlag(Umschlag { value: even::Outcome::True, stamp: Stamp { current_guess, reply_tx, .. }, }),
                            sklavenwelt: next_sklavenwelt,
                        } => {
                            sklavenwelt = next_sklavenwelt;
                            reply_tx.send(current_guess).unwrap();
                        },
                        Obey::Order {
                            order: Order::EvenUmschlag(Umschlag {
                                value: even::Outcome::NotSure,
                                stamp: Stamp {
                                    current_value,
                                    current_guess,
                                    reply_tx,
                                },
                            }),
                            sklavenwelt: next_sklavenwelt,
                        } => {
                            sklavenwelt = next_sklavenwelt;
                            sklavenwelt.odd_meister.order(
                                odd::Order::Is {
                                    value: current_value - 1,
                                    rueckkopplung: Rueckkopplung::new(
                                        sklave.meister().unwrap(),
                                        Stamp {
                                            current_value: current_value - 1,
                                            current_guess: current_guess.neg(),
                                            reply_tx,
                                        },
                                    ),
                                },
                                &edeltraud::EdeltraudJobMap::<_, _, odd::Job<_, _, _>>::new(thread_pool),
                            ).unwrap();
                        },

                        Obey::Rest =>
                            break,
                    }
                },
        }
    }
}

mod odd {
    use crate::{
        Obey,
        Meister,
        Umschlag,
        SklaveJob,
        Rueckkopplung,
    };

    pub enum Outcome {
        False,
        NotSure,
    }

    pub struct Welt;

    pub enum Order<W, B, S> {
        Is {
            value: isize,
            rueckkopplung: Rueckkopplung<W, B, S>,
        },
    }

    pub enum Job<W, B, S> {
        Sklave(SklaveJob<Welt, Order<W, B, S>>),
        Stolen(SklaveJob<W, B>),
    }

    impl<W, B, S> From<SklaveJob<Welt, Order<W, B, S>>> for Job<W, B, S> {
        fn from(sklave_job: SklaveJob<Welt, Order<W, B, S>>) -> Job<W, B, S> {
            Job::Sklave(sklave_job)
        }
    }

    impl<W, B, S> From<SklaveJob<W, B>> for Job<W, B, S> {
        fn from(sklave_job: SklaveJob<W, B>) -> Job<W, B, S> {
            Job::Stolen(sklave_job)
        }
    }

    impl<W, B, S> edeltraud::Job for Job<W, B, S>
    where W: Send + 'static,
          B: From<Umschlag<Outcome, S>> + Send + 'static,
          S: Send + 'static,
    {
        type Output = ();

        fn run<P>(self, thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
            match self {
                Job::Sklave(SklaveJob { mut sklave, sklavenwelt: _, }) => {
                    loop {
                        match sklave.obey(Welt).unwrap() {
                            Obey::Order { order: Order::Is { value: 0, rueckkopplung, }, .. } =>
                                rueckkopplung.commit(Outcome::False, thread_pool).unwrap(),
                            Obey::Order { order: Order::Is { rueckkopplung, .. }, .. } =>
                                rueckkopplung.commit(Outcome::NotSure, thread_pool).unwrap(),
                            Obey::Rest =>
                                break,
                        }
                    }
                },
                Job::Stolen(job) => {
                    job.run(thread_pool);
                },
            }
        }
    }

    pub fn start<P, J, W, B, S>(thread_pool: &P) -> Meister<Welt, Order<W, B, S>>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<Welt, Order<W, B, S>>>,
    {
        Meister::start(Welt, thread_pool)
            .unwrap()
    }
}

mod even {
    use crate::{
        Obey,
        Meister,
        SklaveJob,
        Rueckkopplung,
    };

    pub enum Outcome {
        True,
        NotSure,
    }

    pub struct Welt;

    pub enum Order<W, B, S> {
        Is {
            value: isize,
            rueckkopplung: Rueckkopplung<W, B, S>,
        },
    }

    pub enum Job<W, B, S> {
        Sklave(SklaveJob<Welt, Order<W, B, S>>),
    }

    impl<W, B, S> From<SklaveJob<Welt, Order<W, B, S>>> for Job<W, B, S> {
        fn from(sklave_job: SklaveJob<Welt, Order<W, B, S>>) -> Job<W, B, S> {
            Job::Sklave(sklave_job)
        }
    }

    impl<W, B, S> edeltraud::Job for Job<W, B, S>
    where W: Send + 'static,
          B: Send + 'static,
          S: Send + 'static,
    {
        type Output = ();

        fn run<P>(self, thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
            match self {
                Job::Sklave(SklaveJob { mut sklave, sklavenwelt: _, }) => {
                    loop {
                        match sklave.obey(Welt).unwrap() {
                            Obey::Order { order: Order::Is { value: 0, rueckkopplung, }, .. } =>
                                rueckkopplung.commit(Outcome::True, thread_pool).unwrap(),
                            Obey::Order { order: Order::Is { rueckkopplung, .. }, .. } =>
                                rueckkopplung.commit(Outcome::NotSure, thread_pool).unwrap(),
                            Obey::Rest =>
                                break,
                        }
                    }
                },
            }
        }
    }

    pub fn start<P, J, W, B, S>(thread_pool: &P) -> Meister<Welt, Order<W, B, S>>
    where P: edeltraud::ThreadPool<J>,
          J: edeltraud::Job<Output = ()> + From<SklaveJob<Welt, Order<W, B, S>>>,
    {
        Meister::start(Welt, thread_pool)
            .unwrap()
    }
}
