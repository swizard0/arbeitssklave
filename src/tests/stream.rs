use std::{
    mem,
    sync::{
        mpsc,
    },
};

use crate::{
    komm,
    Freie,
    Gehorsam,
    SklaveJob,
    SklavenBefehl,
};

#[test]
fn basic() {
    enum LocalOrder {
        Start { start: isize, end: isize, },
        GotAnItem(komm::Umschlag<komm::Streamzeug<isize>, LocalStamp>),
        StreamCancel(komm::UmschlagAbbrechen<LocalStamp>),
    }

    impl From<komm::Umschlag<komm::Streamzeug<isize>, LocalStamp>> for LocalOrder {
        fn from(umschlag: komm::Umschlag<komm::Streamzeug<isize>, LocalStamp>) -> LocalOrder {
            LocalOrder::GotAnItem(umschlag)
        }
    }

    impl From<komm::UmschlagAbbrechen<LocalStamp>> for LocalOrder {
        fn from(umschlag_abbrechen: komm::UmschlagAbbrechen<LocalStamp>) -> LocalOrder {
            LocalOrder::StreamCancel(umschlag_abbrechen)
        }
    }

    struct LocalStamp;

    struct Welt {
        tx: mpsc::Sender<Vec<isize>>,
        current: Vec<isize>,
        sendegeraet: komm::Sendegeraet<LocalOrder>,
        stream_sendegeraet: komm::Sendegeraet<stream::Order<Stream>>,
        maybe_stream: Option<komm::Stream<stream::Order<Stream>>>,
    }

    type Stream = komm::Rueckkopplung<LocalOrder, LocalStamp>;

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
                                            let stream = sklavenwelt
                                                .stream_sendegeraet
                                                .stream_starten(stream::OrderStreamStart {
                                                    start,
                                                    end,
                                                    stream_echo: sklavenwelt
                                                        .sendegeraet
                                                        .rueckkopplung(LocalStamp),
                                                })
                                                .unwrap();
                                            assert!(sklavenwelt.maybe_stream.is_none());
                                            sklavenwelt.maybe_stream = Some(stream);
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::GotAnItem(komm::Umschlag {
                                                inhalt: komm::Streamzeug::NichtMehr(..),
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            let current = mem::take(&mut sklavenwelt.current);
                                            assert!(sklavenwelt.maybe_stream.is_some());
                                            sklavenwelt.maybe_stream = None;
                                            sklavenwelt.tx.send(current).ok();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::GotAnItem(komm::Umschlag {
                                                inhalt: komm::Streamzeug::Zeug { zeug, mehr, },
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            sklavenwelt.current.push(zeug);
                                            let stream =
                                                sklavenwelt.maybe_stream.as_ref().unwrap();
                                            assert_eq!(stream.stream_id(), mehr.stream_id());
                                            stream
                                                .mehr(
                                                    stream::OrderStreamNext {
                                                        stream_echo: sklavenwelt
                                                            .sendegeraet
                                                            .rueckkopplung(LocalStamp),
                                                    },
                                                    mehr.into(),
                                                )
                                                .unwrap();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::StreamCancel(komm::UmschlagAbbrechen {
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            let current = mem::take(&mut sklavenwelt.current);
                                            assert!(sklavenwelt.maybe_stream.is_some());
                                            sklavenwelt.maybe_stream = None;
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

    let (_stream_meister, stream_sendegeraet) = stream::start(&edeltraud::ThreadPoolMap::new(thread_pool.clone()));
    let freie = Freie::new();
    let sendegeraet = komm::Sendegeraet::starten(&freie, thread_pool.clone()).unwrap();
    let meister = freie
        .versklaven(
            Welt {
                tx,
                current: Vec::new(),
                sendegeraet,
                stream_sendegeraet,
                maybe_stream: None,
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
        Start(komm::StreamStarten<OrderStreamStart<S>>),
        Next(komm::StreamMehr<OrderStreamNext<S>>),
        Cancel(komm::StreamAbbrechen),
    }

    pub struct OrderStreamStart<S> {
        pub start: isize,
        pub end: isize,
        pub stream_echo: S,
    }

    pub struct OrderStreamNext<S> {
        pub stream_echo: S,
    }

    impl<S> From<komm::StreamStarten<OrderStreamStart<S>>> for Order<S> {
        fn from(order: komm::StreamStarten<OrderStreamStart<S>>) -> Order<S> {
            Order::Start(order)
        }
    }

    impl<S> From<komm::StreamMehr<OrderStreamNext<S>>> for Order<S> {
        fn from(order: komm::StreamMehr<OrderStreamNext<S>>) -> Order<S> {
            Order::Next(order)
        }
    }

    impl<S> From<komm::StreamAbbrechen> for Order<S> {
        fn from(stream_abbrechen: komm::StreamAbbrechen) -> Order<S> {
            Order::Cancel(stream_abbrechen)
        }
    }

    #[derive(Default)]
    pub struct Welt {
        streams: Vec<Stream>,
    }

    struct Stream {
        stream_id: komm::StreamId,
        start: isize,
        end: isize,
    }

    pub enum Job<S> {
        Sklave(SklaveJob<Welt, Order<S>>),
    }

    impl<S> From<SklaveJob<Welt, Order<S>>> for Job<S> {
        fn from(sklave_job: SklaveJob<Welt, Order<S>>) -> Job<S> {
            Job::Sklave(sklave_job)
        }
    }

    impl<S> edeltraud::Job for Job<S> where S: komm::Echo<komm::Streamzeug<isize>> + Send + 'static {
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
                                            befehl: Order::Start(komm::StreamStarten {
                                                inhalt: OrderStreamStart { start, end, stream_echo, },
                                                stream_token,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            if start >= end {
                                                let streamzeug = stream_token.streamzeug_nicht_mehr();
                                                stream_echo.commit_echo(streamzeug).unwrap();
                                            } else {
                                                let stream = Stream {
                                                    start,
                                                    end,
                                                    stream_id: stream_token.stream_id().clone(),
                                                };
                                                sklavenwelt.streams.push(stream);
                                                let streamzeug = stream_token.streamzeug_zeug(start);
                                                stream_echo.commit_echo(streamzeug).unwrap();
                                            }
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Next(komm::StreamMehr {
                                                inhalt: OrderStreamNext { stream_echo, },
                                                stream_token,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            let stream_index = sklavenwelt.streams
                                                .iter()
                                                .enumerate()
                                                .find(|pair| &pair.1.stream_id == stream_token.stream_id())
                                                .map(|pair| pair.0)
                                                .unwrap();
                                            let stream = &mut sklavenwelt.streams[stream_index];
                                            stream.start += 1;
                                            let streamzeug = if stream.start < stream.end {
                                                stream_token.streamzeug_zeug(stream.start)
                                            } else {
                                                sklavenwelt.streams.swap_remove(stream_index);
                                                stream_token.streamzeug_nicht_mehr()
                                            };
                                            stream_echo.commit_echo(streamzeug).unwrap();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: Order::Cancel(komm::StreamAbbrechen { stream_id, }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let sklavenwelt = befehle.sklavenwelt_mut();
                                            let stream_index = sklavenwelt.streams
                                                .iter()
                                                .enumerate()
                                                .find(|pair| pair.1.stream_id == stream_id)
                                                .map(|pair| pair.0)
                                                .unwrap();
                                            sklavenwelt.streams.swap_remove(stream_index);
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

    pub fn start<P, S>(thread_pool: &P) -> (Meister<Welt, Order<S>>, komm::Sendegeraet<Order<S>>)
    where P: edeltraud::ThreadPool<Job<S>> + Clone + Send + 'static,
          S: komm::Echo<komm::Streamzeug<isize>> + Send + 'static,
    {
        let freie = Freie::new();
        let sendegeraet = komm::Sendegeraet::starten(&freie, thread_pool.clone()).unwrap();
        let meister = freie.versklaven(Welt::default(), thread_pool).unwrap();
        (meister, sendegeraet)
    }
}
