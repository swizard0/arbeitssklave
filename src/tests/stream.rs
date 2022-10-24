use std::{
    mem,
    sync::{
        mpsc,
    },
};

use crate::{
    komm::{
        Echo,
        Umschlag,
        Streamzeug,
        Sendegeraet,
        Rueckkopplung,
        UmschlagAbbrechen,
    },
    Freie,
    Meister,
    Gehorsam,
    SklaveJob,
    SklavenBefehl,
};

#[test]
fn basic() {
    enum LocalOrder {
        Start { start: isize, end: isize, },
        GotAnItem(Umschlag<Streamzeug<isize, stream::StreamNext<Stream>>, LocalStamp>),
        StreamCancel(UmschlagAbbrechen<LocalStamp>),
    }

    impl From<Umschlag<Streamzeug<isize, stream::StreamNext<Stream>>, LocalStamp>> for LocalOrder {
        fn from(umschlag: Umschlag<Streamzeug<isize, stream::StreamNext<Stream>>, LocalStamp>) -> LocalOrder {
            LocalOrder::GotAnItem(umschlag)
        }
    }

    impl From<UmschlagAbbrechen<LocalStamp>> for LocalOrder {
        fn from(umschlag_abbrechen: UmschlagAbbrechen<LocalStamp>) -> LocalOrder {
            LocalOrder::StreamCancel(umschlag_abbrechen)
        }
    }

    struct LocalStamp;

    struct Welt {
        tx: mpsc::Sender<Vec<isize>>,
        current: Vec<isize>,
        sendegeraet: Sendegeraet<LocalOrder>,
        stream_meister: Meister<stream::Welt<Stream>, stream::Order<Stream>>,
    }

    type Stream = Rueckkopplung<LocalOrder, LocalStamp>;

    enum LocalJob {
        Sklave(SklaveJob<Welt, LocalOrder>),
        Stream(stream::Job<Stream>),
    }

    impl From<SklaveJob<Welt, LocalOrder>> for LocalJob {
        fn from(job: SklaveJob<Welt, LocalOrder>) -> LocalJob {
            LocalJob::Sklave(job)
        }
    }

    impl From<stream::Job<Stream>> for LocalJob {
        fn from(job: stream::Job<Stream>) -> LocalJob {
            LocalJob::Stream(job)
        }
    }

    impl edeltraud::Job for LocalJob {
        fn run<P>(self, thread_pool: &P) where P: edeltraud::ThreadPool<Self> {
            match self {
                LocalJob::Stream(job) =>
                    job.run(&edeltraud::ThreadPoolMap::new(thread_pool)),
                LocalJob::Sklave(mut sklave_job) => {
                    loop {
                        match sklave_job.zu_ihren_diensten().unwrap() {
                            Gehorsam::Rasten =>
                                break,
                            Gehorsam::Machen { mut befehle, } =>
                                loop {
                                    match befehle.befehl() {
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::Start { start, end, },
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            sklavenwelt
                                                .stream_meister
                                                .befehl(
                                                    stream::Order::Start(stream::OrderStreamStart {
                                                        start,
                                                        end,
                                                        stream: sklavenwelt
                                                            .sendegeraet
                                                            .rueckkopplung(LocalStamp),
                                                    }),
                                                    &edeltraud::ThreadPoolMap::<_, _, stream::Job<_>>::new(thread_pool),
                                                )
                                                .unwrap();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::GotAnItem(Umschlag {
                                                inhalt: Streamzeug::NichtMehr,
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            let current = mem::take(&mut sklavenwelt.current);
                                            sklavenwelt.tx.send(current).ok();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::GotAnItem(Umschlag {
                                                inhalt: Streamzeug::Zeug {
                                                    zeug,
                                                    mehr_stream ,
                                                },
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            sklavenwelt.current.push(zeug);
                                            let echo = sklavenwelt
                                                .sendegeraet
                                                .rueckkopplung(LocalStamp);
                                            mehr_stream.commit_echo(echo).unwrap();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::StreamCancel(UmschlagAbbrechen {
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            let current = mem::take(&mut sklavenwelt.current);
                                            sklavenwelt.tx.send(current).ok();
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
                },
            }
        }
    }

    let thread_pool: edeltraud::Edeltraud<LocalJob> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let (tx, rx) = mpsc::channel();

    let stream_meister =
        stream::start(&edeltraud::ThreadPoolMap::new(thread_pool.clone()));
    let freie = Freie::new();
    let sendegeraet = Sendegeraet::starten(&freie, thread_pool.clone()).unwrap();
    let meister = freie
        .versklaven(
            Welt {
                tx,
                current: Vec::new(),
                sendegeraet,
                stream_meister,
            },
            &thread_pool,
        )
        .unwrap();

    meister.befehl(LocalOrder::Start { start: 3, end: 6, }, &thread_pool).unwrap();
    assert_eq!(rx.recv(), Ok(vec![3, 4, 5]));
    meister.befehl(LocalOrder::Start { start: -1, end: 1, }, &thread_pool).unwrap();
    assert_eq!(rx.recv(), Ok(vec![-1, 0]));
    meister.befehl(LocalOrder::Start { start: 9, end: 10, }, &thread_pool).unwrap();
    assert_eq!(rx.recv(), Ok(vec![9]));
    meister.befehl(LocalOrder::Start { start: -3, end: 7, }, &thread_pool).unwrap();
    assert_eq!(rx.recv(), Ok(vec![-3, -2, -1, 0, 1, 2, 3, 4, 5, 6]));
}

#[allow(clippy::module_inception)]
mod stream {
    use crate::{
        komm,
        Freie,
        Meister,
        Gehorsam,
        SklaveJob,
        SklavenBefehl,
    };

    pub enum Order<S> {
        Start(OrderStreamStart<S>),
        Next(komm::Umschlag<S, Stamp>),
        Cancel(komm::UmschlagAbbrechen<Stamp>),
    }

    pub struct OrderStreamStart<S> {
        pub start: isize,
        pub end: isize,
        pub stream: S,
    }

    impl<S> From<komm::UmschlagAbbrechen<Stamp>> for Order<S> {
        fn from(umschlag_abbrechen: komm::UmschlagAbbrechen<Stamp>) -> Order<S> {
            Order::Cancel(umschlag_abbrechen)
        }
    }

    impl<S> From<komm::Umschlag<S, Stamp>> for Order<S> {
        fn from(umschlag: komm::Umschlag<S, Stamp>) -> Order<S> {
            Order::Next(umschlag)
        }
    }

    pub struct Stamp;

    pub struct StreamNext<S> {
        rueckkopplung: komm::Rueckkopplung<Order<S>, Stamp>,
    }

    impl<S> komm::Echo<S> for StreamNext<S> {
        fn commit_echo(self, inhalt: S) -> Result<(), komm::EchoError> {
            self.rueckkopplung.commit_echo(inhalt)
        }
    }

    pub struct Welt<S> {
        process: Option<(isize, isize)>,
        sendegeraet: komm::Sendegeraet<Order<S>>,
    }

    pub enum Job<S> {
        Sklave(SklaveJob<Welt<S>, Order<S>>),
    }

    impl<S> From<SklaveJob<Welt<S>, Order<S>>> for Job<S> where S: komm::Stream<isize, StreamNext<S>> {
        fn from(sklave_job: SklaveJob<Welt<S>, Order<S>>) -> Job<S> {
            Job::Sklave(sklave_job)
        }
    }

    impl<S> edeltraud::Job for Job<S> where S: komm::Stream<isize, StreamNext<S>> + Send + 'static {
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
                                            befehl: Order::Start(OrderStreamStart { start, end, stream, }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            assert!(sklavenwelt.process.is_none());
                                            assert!(start < end);
                                            sklavenwelt.process = Some((start + 1, end));
                                            stream
                                                .commit_echo(
                                                    komm::Streamzeug::Zeug {
                                                        zeug: start,
                                                        mehr_stream: StreamNext {
                                                            rueckkopplung: sklavenwelt
                                                                .sendegeraet
                                                                .rueckkopplung(Stamp),
                                                        },
                                                    },
                                                )
                                                .unwrap();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Next(komm::Umschlag { inhalt: stream, stamp: Stamp, }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            let (start, end) =
                                                sklavenwelt.process.as_mut().unwrap();
                                            let zeug = *start;
                                            *start += 1;
                                            let stream_zeug = if zeug < *end {
                                                komm::Streamzeug::Zeug {
                                                    zeug,
                                                    mehr_stream: StreamNext {
                                                        rueckkopplung: sklavenwelt
                                                            .sendegeraet
                                                            .rueckkopplung(Stamp),
                                                    },
                                                }
                                            } else {
                                                sklavenwelt.process = None;
                                                komm::Streamzeug::NichtMehr
                                            };
                                            stream.commit_echo(stream_zeug).unwrap();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Cancel(komm::UmschlagAbbrechen { stamp: Stamp, }),
                                            ..
                                        } =>
                                            unreachable!(),
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

    pub fn start<P, S>(thread_pool: &P) -> Meister<Welt<S>, Order<S>>
    where P: edeltraud::ThreadPool<Job<S>> + Clone + Send + 'static,
          S: komm::Stream<isize, StreamNext<S>> + Send + 'static,
    {
        let freie = Freie::new();
        let sendegeraet =
            komm::Sendegeraet::starten(&freie, thread_pool.clone()).unwrap();
        let welt = Welt {
            process: None,
            sendegeraet,
        };
        freie.versklaven(welt, thread_pool).unwrap()
    }
}
