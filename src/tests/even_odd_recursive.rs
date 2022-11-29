use std::{
    sync::{
        mpsc,
        Mutex,
    },
};

use crate::{
    komm,
    Freie,
    Meister,
    Gehorsam,
    SklavenBefehl,
};

#[test]
fn basic() {
    let thread_pool: edeltraud::Edeltraud<Job> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let odd_meister = odd::start(&edeltraud::ThreadPoolMap::new(&thread_pool));
    let even_meister = even::start(&edeltraud::ThreadPoolMap::new(&thread_pool));

    let driver_freie = Freie::new(Welt {
        odd_meister,
        even_meister,
    });
    let driver_meister =
        driver_freie.versklaven_komm(&thread_pool)
        .unwrap();

    let mut outcomes = Vec::new();
    for value in [13, 8, 1024, 1, 0, 65535] {
        let (reply_tx, reply_rx) = mpsc::channel();
        driver_meister.befehl(Order::Calc { value, reply_tx: Mutex::new(reply_tx), }, &thread_pool).unwrap();
        let result = reply_rx.recv().unwrap();
        outcomes.push(result);
    }

    assert_eq!(outcomes, vec![ValueType::Odd, ValueType::Even, ValueType::Even, ValueType::Odd, ValueType::Even, ValueType::Odd]);
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
        reply_tx: Mutex<mpsc::Sender<ValueType>>,
    },
    OddUmschlag(komm::Umschlag<odd::Outcome, Stamp>),
    EvenUmschlag(komm::Umschlag<even::Outcome, Stamp>),
    Abbrechen(komm::UmschlagAbbrechen<Stamp>),
}

impl From<komm::Umschlag<odd::Outcome, Stamp>> for Order {
    fn from(umschlag: komm::Umschlag<odd::Outcome, Stamp>) -> Order {
        Order::OddUmschlag(umschlag)
    }
}

impl From<komm::Umschlag<even::Outcome, Stamp>> for Order {
    fn from(umschlag: komm::Umschlag<even::Outcome, Stamp>) -> Order {
        Order::EvenUmschlag(umschlag)
    }
}

impl From<komm::UmschlagAbbrechen<Stamp>> for Order {
    fn from(umschlag_abbrechen: komm::UmschlagAbbrechen<Stamp>) -> Order {
        Order::Abbrechen(umschlag_abbrechen)
    }
}

type OddJob = odd::Job<komm::Rueckkopplung<Order, Stamp>>;
type OddOrder = odd::Order<komm::Rueckkopplung<Order, Stamp>>;
type EvenJob = even::Job<komm::Rueckkopplung<Order, Stamp>>;
type EvenOrder = even::Order<komm::Rueckkopplung<Order, Stamp>>;

struct Welt {
    odd_meister: Meister<odd::Welt, OddOrder>,
    even_meister: Meister<even::Welt, EvenOrder>,
}

enum Job {
    Odd(OddJob),
    Even(EvenJob),
    Driver(komm::SklaveJob<Welt, Order>),
}

impl From<komm::SklaveJob<Welt, Order>> for Job {
    fn from(job: komm::SklaveJob<Welt, Order>) -> Job {
        Job::Driver(job)
    }
}

impl From<OddJob> for Job {
    fn from(job: OddJob) -> Job {
        Job::Odd(job)
    }
}

impl From<EvenJob> for Job {
    fn from(job: EvenJob) -> Job {
        Job::Even(job)
    }
}

struct Stamp {
    current_value: usize,
    current_guess: ValueType,
    reply_tx: Mutex<mpsc::Sender<ValueType>>,
}

impl edeltraud::Job for Job {
    fn run<P>(self, thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
        match self {
            Job::Odd(job) => {
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::Even(job) => {
                job.run(&edeltraud::ThreadPoolMap::new(thread_pool));
            },
            Job::Driver(mut sklave_job) =>
                loop {
                    match sklave_job.zu_ihren_diensten().unwrap() {
                        Gehorsam::Rasten =>
                            break,
                        Gehorsam::Machen { mut befehle, } =>
                            loop {
                                match befehle.befehl() {
                                    SklavenBefehl::Mehr {
                                        befehl: Order::Calc { value, reply_tx, },
                                        mehr_befehle,
                                    } => {
                                        mehr_befehle
                                            .even_meister
                                            .befehl(
                                                even::Order::Is {
                                                    value,
                                                    echo: mehr_befehle
                                                        .sendegeraet()
                                                        .rueckkopplung(Stamp {
                                                            current_value: value,
                                                            current_guess: ValueType::Even,
                                                            reply_tx,
                                                        }),
                                                },
                                                &edeltraud::ThreadPoolMap::<_, _, even::Job<_>>::new(thread_pool),
                                            )
                                            .unwrap();
                                        befehle = mehr_befehle;
                                    },
                                    SklavenBefehl::Mehr {
                                        befehl: Order::OddUmschlag(komm::Umschlag {
                                            inhalt: odd::Outcome::False,
                                            stamp: Stamp { current_guess, reply_tx, .. },
                                        }),
                                        mehr_befehle,
                                    } => {
                                        let tx_lock = reply_tx.lock().unwrap();
                                        tx_lock.send(current_guess).unwrap();
                                        befehle = mehr_befehle;
                                    },
                                    SklavenBefehl::Mehr {
                                        befehl: Order::OddUmschlag(komm::Umschlag {
                                            inhalt: odd::Outcome::NotSure,
                                            stamp: Stamp { current_value, current_guess, reply_tx, },
                                        }),
                                        mehr_befehle,
                                    } => {
                                        mehr_befehle
                                            .even_meister
                                            .befehl(
                                                even::Order::Is {
                                                    value: current_value - 1,
                                                    echo: mehr_befehle
                                                        .sendegeraet()
                                                        .rueckkopplung(Stamp {
                                                            current_value: current_value - 1,
                                                            current_guess: current_guess.neg(),
                                                            reply_tx,
                                                        }),
                                                },
                                                &edeltraud::ThreadPoolMap::<_, _, even::Job<_>>::new(thread_pool),
                                            )
                                            .unwrap();
                                        befehle = mehr_befehle;
                                    },
                                    SklavenBefehl::Mehr {
                                        befehl: Order::EvenUmschlag(komm::Umschlag {
                                            inhalt: even::Outcome::True,
                                            stamp: Stamp { current_guess, reply_tx, .. },
                                        }),
                                        mehr_befehle,
                                    } => {
                                        let tx_lock = reply_tx.lock().unwrap();
                                        tx_lock.send(current_guess).unwrap();
                                        befehle = mehr_befehle;
                                    },
                                    SklavenBefehl::Mehr {
                                        befehl: Order::EvenUmschlag(komm::Umschlag {
                                            inhalt: even::Outcome::NotSure,
                                            stamp: Stamp {
                                                current_value,
                                                current_guess,
                                                reply_tx,
                                            },
                                        }),
                                        mehr_befehle,
                                    } => {
                                        mehr_befehle
                                            .odd_meister
                                            .befehl(
                                                odd::Order::Is {
                                                    value: current_value - 1,
                                                    echo: mehr_befehle
                                                        .sendegeraet()
                                                        .rueckkopplung(Stamp {
                                                            current_value: current_value - 1,
                                                            current_guess: current_guess.neg(),
                                                            reply_tx,
                                                        }),
                                                },
                                                &edeltraud::ThreadPoolMap::<_, _, odd::Job<_>>::new(thread_pool),
                                            )
                                            .unwrap();
                                        befehle = mehr_befehle;
                                    },
                                    SklavenBefehl::Mehr {
                                        befehl: Order::Abbrechen(komm::UmschlagAbbrechen {
                                            stamp: Stamp { current_value, current_guess, .. },
                                        }),
                                        ..
                                    } =>
                                        panic!("unexpected UmschlagAbbrechen for current_value = {current_value:?} current_guess = {current_guess:?}"),
                                    SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
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

mod odd {
    use crate::{
        komm,
        Freie,
        Meister,
        Gehorsam,
        SklaveJob,
        SklavenBefehl,
    };

    pub enum Outcome {
        False,
        NotSure,
    }

    pub struct Welt;

    pub enum Order<E> where E: komm::Echo<Outcome> {
        Is {
            value: usize,
            echo: E,
        },
    }

    pub enum Job<E> where E: komm::Echo<Outcome> {
        Sklave(SklaveJob<Welt, Order<E>>),
    }

    impl<E> From<SklaveJob<Welt, Order<E>>> for Job<E> where E: komm::Echo<Outcome> {
        fn from(sklave_job: SklaveJob<Welt, Order<E>>) -> Job<E> {
            Job::Sklave(sklave_job)
        }
    }

    impl<E> edeltraud::Job for Job<E> where E: komm::Echo<Outcome> + Send + 'static {
        fn run<P>(self, _thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
            match self {
                Job::Sklave(mut sklave_job) => {
                    loop {
                        match sklave_job.zu_ihren_diensten().unwrap() {
                            Gehorsam::Rasten =>
                                break,
                            Gehorsam::Machen { mut befehle, } =>
                                loop {
                                    match befehle.befehl() {
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Is { value: 0, echo, },
                                            mehr_befehle,
                                        } => {
                                            echo.commit_echo(Outcome::False).unwrap();
                                            befehle = mehr_befehle;
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Is { echo, .. },
                                            mehr_befehle,
                                        } => {
                                            echo.commit_echo(Outcome::NotSure).unwrap();
                                            befehle = mehr_befehle;
                                        },
                                        SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                            sklave_job = next_sklave_job;
                                            break;
                                        },
                                    }
                                },
                        }
                    }
                },
            }
        }
    }

    pub fn start<P, E>(thread_pool: &P) -> Meister<Welt, Order<E>>
    where P: edeltraud::ThreadPool<Job<E>>,
          E: komm::Echo<Outcome> + Send + 'static,
    {
        let freie = Freie::new(Welt);
        freie.versklaven(thread_pool).unwrap()
    }
}

mod even {
    use crate::{
        komm,
        Freie,
        Meister,
        Gehorsam,
        SklaveJob,
        SklavenBefehl,
    };

    pub enum Outcome {
        True,
        NotSure,
    }

    pub struct Welt;

    pub enum Order<E> where E: komm::Echo<Outcome> {
        Is {
            value: usize,
            echo: E,
        },
    }

    pub enum Job<E> where E: komm::Echo<Outcome> {
        Sklave(SklaveJob<Welt, Order<E>>),
    }

    impl<E> From<SklaveJob<Welt, Order<E>>> for Job<E> where E: komm::Echo<Outcome> {
        fn from(sklave_job: SklaveJob<Welt, Order<E>>) -> Job<E> {
            Job::Sklave(sklave_job)
        }
    }

    impl<E> edeltraud::Job for Job<E> where E: komm::Echo<Outcome> + Send + 'static {
        fn run<P>(self, _thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
            match self {
                Job::Sklave(mut sklave_job) => {
                    loop {
                        match sklave_job.zu_ihren_diensten().unwrap() {
                            Gehorsam::Rasten =>
                                break,
                            Gehorsam::Machen { mut befehle, } =>
                                loop {
                                    match befehle.befehl() {
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Is { value: 0, echo, },
                                            mehr_befehle,
                                        } => {
                                            echo.commit_echo(Outcome::True).unwrap();
                                            befehle = mehr_befehle;
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Is { echo, .. },
                                            mehr_befehle,
                                        } => {
                                            echo.commit_echo(Outcome::NotSure).unwrap();
                                            befehle = mehr_befehle;
                                        },
                                        SklavenBefehl::Ende { sklave_job: next_sklave_job, } => {
                                            sklave_job = next_sklave_job;
                                            break;
                                        },
                                    }
                                },
                        }
                    }
                },
            }
        }
    }

    pub fn start<P, E>(thread_pool: &P) -> Meister<Welt, Order<E>>
    where P: edeltraud::ThreadPool<Job<E>>,
          E: komm::Echo<Outcome> + Send + 'static,
    {
        let freie = Freie::new(Welt);
        freie.versklaven(thread_pool).unwrap()
    }
}
