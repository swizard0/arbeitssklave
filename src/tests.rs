use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    komm::{
        Umschlag,
        Sendegeraet,
        UmschlagAbbrechen,
    },
    Obey,
    Freie,
    Meister,
    SklaveJob,
};

#[test]
fn umschlag_abbrechen() {
    #[derive(PartialEq, Eq, Debug)]
    struct Canceled;

    struct LocalStamp;

    struct LocalOrder(UmschlagAbbrechen<LocalStamp>);

    impl From<UmschlagAbbrechen<LocalStamp>> for LocalOrder {
        fn from(umschlag_abbrechen: UmschlagAbbrechen<LocalStamp>) -> LocalOrder {
            LocalOrder(umschlag_abbrechen)
        }
    }

    struct LocalJob(SklaveJob<mpsc::Sender<Canceled>, LocalOrder>);

    impl From<SklaveJob<mpsc::Sender<Canceled>, LocalOrder>> for LocalJob {
        fn from(sklave_job: SklaveJob<mpsc::Sender<Canceled>, LocalOrder>) -> LocalJob {
            LocalJob(sklave_job)
        }
    }

    impl edeltraud::Job for LocalJob {
        type Output = ();

        fn run<P>(self, _thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
            let LocalJob(SklaveJob { mut sklave, sklavenwelt: mut tx, }) = self;
            loop {
                match sklave.obey(tx).unwrap() {
                    Obey::Order { order: LocalOrder(UmschlagAbbrechen { stamp: LocalStamp, }), sklavenwelt: next_tx, } => {
                        tx = next_tx;
                        tx.send(Canceled).ok();
                    },
                    Obey::Rest =>
                        break,
                }
            }
        }
    }

    let thread_pool: edeltraud::Edeltraud<LocalJob> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let driver_freie = Freie::new();
    let sendegeraet = Sendegeraet::spawn(&driver_freie, thread_pool.clone()).unwrap();

    let (tx, rx) = mpsc::channel();
    let _driver_meister = driver_freie.versklaven(tx, &thread_pool).unwrap();

    let rueckkopplung = sendegeraet.rueckkopplung(LocalStamp);
    drop(rueckkopplung);

    assert_eq!(rx.recv_timeout(std::time::Duration::from_millis(100)), Ok(Canceled));
}

#[test]
fn even_odd_recursive() {
    let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let odd_meister = odd::start(&edeltraud::EdeltraudJobMap::new(&thread_pool));
    let even_meister = even::start(&edeltraud::EdeltraudJobMap::new(&thread_pool));

    let driver_freie = Freie::new();
    let sendegeraet = Sendegeraet::spawn(&driver_freie, thread_pool.clone()).unwrap();
    let driver_meister =
        driver_freie.versklaven(
            Welt {
                odd_meister,
                even_meister,
                sendegeraet,
            },
            &thread_pool,
        )
        .unwrap();

    let mut outcomes = Vec::new();
    for value in [13, 8, 1024, 1, 0] {
        let (reply_tx, reply_rx) = mpsc::channel();
        driver_meister.order(Order::Calc { value, reply_tx, }, &thread_pool).unwrap();
        let result = reply_rx.recv().unwrap();
        outcomes.push(result);
    }

    assert_eq!(outcomes, vec![ValueType::Odd, ValueType::Even, ValueType::Even, ValueType::Odd, ValueType::Even]);
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

enum Order {
    Calc {
        value: usize,
        reply_tx: mpsc::Sender<ValueType>,
    },
    OddUmschlag(Umschlag<odd::Outcome, Stamp>),
    EvenUmschlag(Umschlag<even::Outcome, Stamp>),
    Abbrechen(UmschlagAbbrechen<Stamp>),
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

impl From<UmschlagAbbrechen<Stamp>> for Order {
    fn from(umschlag_abbrechen: UmschlagAbbrechen<Stamp>) -> Order {
        Order::Abbrechen(umschlag_abbrechen)
    }
}

struct Welt {
    odd_meister: Meister<odd::Welt, odd::Order<Order, Stamp>>,
    even_meister: Meister<even::Welt, even::Order<Order, Stamp>>,
    sendegeraet: Sendegeraet<Order>,
}

enum Job {
    Odd(odd::Job<Order, Stamp>),
    Even(even::Job<Order, Stamp>),
    Driver(SklaveJob<Welt, Order>),
}

impl From<SklaveJob<Welt, Order>> for Job {
    fn from(job: SklaveJob<Welt, Order>) -> Job {
        Job::Driver(job)
    }
}

impl From<odd::Job<Order, Stamp>> for Job {
    fn from(job: odd::Job<Order, Stamp>) -> Job {
        Job::Odd(job)
    }
}

impl From<even::Job<Order, Stamp>> for Job {
    fn from(job: even::Job<Order, Stamp>) -> Job {
        Job::Even(job)
    }
}

struct Stamp {
    current_value: usize,
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
                                    rueckkopplung: sklavenwelt
                                        .sendegeraet
                                        .rueckkopplung(Stamp {
                                            current_value: value,
                                            current_guess: ValueType::Even,
                                            reply_tx,
                                        }),
                                },
                                &edeltraud::EdeltraudJobMap::<_, _, even::Job<_, _>>::new(thread_pool),
                            ).unwrap();
                        },
                        Obey::Order {
                            order: Order::OddUmschlag(Umschlag { payload: odd::Outcome::False, stamp: Stamp { current_guess, reply_tx, .. }, }),
                            sklavenwelt: next_sklavenwelt,
                        } => {
                            sklavenwelt = next_sklavenwelt;
                            reply_tx.send(current_guess).unwrap();
                        },
                        Obey::Order {
                            order: Order::OddUmschlag(Umschlag {
                                payload: odd::Outcome::NotSure,
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
                                    rueckkopplung: sklavenwelt
                                        .sendegeraet
                                        .rueckkopplung(Stamp {
                                            current_value: current_value - 1,
                                            current_guess: current_guess.neg(),
                                            reply_tx,
                                        }),
                                },
                                &edeltraud::EdeltraudJobMap::<_, _, even::Job<_, _>>::new(thread_pool),
                            ).unwrap();
                        },
                        Obey::Order {
                            order: Order::EvenUmschlag(Umschlag { payload: even::Outcome::True, stamp: Stamp { current_guess, reply_tx, .. }, }),
                            sklavenwelt: next_sklavenwelt,
                        } => {
                            sklavenwelt = next_sklavenwelt;
                            reply_tx.send(current_guess).unwrap();
                        },
                        Obey::Order {
                            order: Order::EvenUmschlag(Umschlag {
                                payload: even::Outcome::NotSure,
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
                                    rueckkopplung: sklavenwelt
                                        .sendegeraet
                                        .rueckkopplung(Stamp {
                                            current_value: current_value - 1,
                                            current_guess: current_guess.neg(),
                                            reply_tx,
                                        }),
                                },
                                &edeltraud::EdeltraudJobMap::<_, _, odd::Job<_, _>>::new(thread_pool),
                            ).unwrap();
                        },
                        Obey::Order { order: Order::Abbrechen(UmschlagAbbrechen { stamp: Stamp { current_value, current_guess, .. }, }), .. } =>
                            panic!("unexpected UmschlagAbbrechen for current_value = {current_value:?} current_guess = {current_guess:?}"),

                        Obey::Rest =>
                            break,
                    }
                },
        }
    }
}

mod odd {
    use crate::{
        komm::{
            Umschlag,
            Rueckkopplung,
            UmschlagAbbrechen,
        },
        Obey,
        Freie,
        Meister,
        SklaveJob,
    };

    pub enum Outcome {
        False,
        NotSure,
    }

    pub struct Welt;

    pub enum Order<B, S> where B: From<UmschlagAbbrechen<S>> {
        Is {
            value: usize,
            rueckkopplung: Rueckkopplung<B, S>,
        },
    }

    pub enum Job<B, S> where B: From<UmschlagAbbrechen<S>> {
        Sklave(SklaveJob<Welt, Order<B, S>>),
    }

    impl<B, S> From<SklaveJob<Welt, Order<B, S>>> for Job<B, S> where B: From<UmschlagAbbrechen<S>> {
        fn from(sklave_job: SklaveJob<Welt, Order<B, S>>) -> Job<B, S> {
            Job::Sklave(sklave_job)
        }
    }

    impl<B, S> edeltraud::Job for Job<B, S>
    where B: From<Umschlag<Outcome, S>> + From<UmschlagAbbrechen<S>> + Send + 'static,
          S: Send + 'static,
    {
        type Output = ();

        fn run<P>(self, _thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
            match self {
                Job::Sklave(SklaveJob { mut sklave, sklavenwelt: _, }) => {
                    loop {
                        match sklave.obey(Welt).unwrap() {
                            Obey::Order { order: Order::Is { value: 0, rueckkopplung, }, .. } => {
                                rueckkopplung.commit(Outcome::False).unwrap();
                            },
                            Obey::Order { order: Order::Is { rueckkopplung, .. }, .. } => {
                                rueckkopplung.commit(Outcome::NotSure).unwrap();
                            },
                            Obey::Rest =>
                                break,
                        }
                    }
                },
            }
        }
    }

    pub fn start<P, B, S>(thread_pool: &P) -> Meister<Welt, Order<B, S>>
    where P: edeltraud::ThreadPool<Job<B, S>>,
          B: From<Umschlag<Outcome, S>> + From<UmschlagAbbrechen<S>> + Send + 'static,
          S: Send + 'static,
    {
        let freie = Freie::new();
        freie.versklaven(Welt, thread_pool).unwrap()
    }
}

mod even {
    use crate::{
        komm::{
            Umschlag,
            Rueckkopplung,
            UmschlagAbbrechen,
        },
        Obey,
        Freie,
        Meister,
        SklaveJob,
    };

    pub enum Outcome {
        True,
        NotSure,
    }

    pub struct Welt;

    pub enum Order<B, S> where B: From<UmschlagAbbrechen<S>> {
        Is {
            value: usize,
            rueckkopplung: Rueckkopplung<B, S>,
        },
    }

    pub enum Job<B, S> where B: From<UmschlagAbbrechen<S>> {
        Sklave(SklaveJob<Welt, Order<B, S>>),
    }

    impl<B, S> From<SklaveJob<Welt, Order<B, S>>> for Job<B, S> where B: From<UmschlagAbbrechen<S>> {
        fn from(sklave_job: SklaveJob<Welt, Order<B, S>>) -> Job<B, S> {
            Job::Sklave(sklave_job)
        }
    }

    impl<B, S> edeltraud::Job for Job<B, S>
    where B: From<Umschlag<Outcome, S>> + From<UmschlagAbbrechen<S>> + Send + 'static,
          S: Send + 'static
    {
        type Output = ();

        fn run<P>(self, _thread_pool: &P) -> Self::Output where P: edeltraud::ThreadPool<Self> {
            match self {
                Job::Sklave(SklaveJob { mut sklave, sklavenwelt: _, }) => {
                    loop {
                        match sklave.obey(Welt).unwrap() {
                            Obey::Order { order: Order::Is { value: 0, rueckkopplung, }, .. } => {
                                rueckkopplung.commit(Outcome::True).unwrap();
                            },
                            Obey::Order { order: Order::Is { rueckkopplung, .. }, .. } => {
                                rueckkopplung.commit(Outcome::NotSure).unwrap();
                            },
                            Obey::Rest =>
                                break,
                        }
                    }
                },
            }
        }
    }

    pub fn start<P, B, S>(thread_pool: &P) -> Meister<Welt, Order<B, S>>
    where P: edeltraud::ThreadPool<Job<B, S>>,
          B: From<Umschlag<Outcome, S>> + From<UmschlagAbbrechen<S>> + Send + 'static,
          S: Send + 'static,
    {
        let freie = Freie::new();
        freie.versklaven(Welt, thread_pool).unwrap()
    }
}
