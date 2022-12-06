use std::{
    mem,
    sync::{
        mpsc,
        Mutex,
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
        tx: Mutex<mpsc::Sender<Vec<isize>>>,
        current: Vec<isize>,
        local_sendegeraet: komm::Sendegeraet<LocalOrder>,
        stream_sendegeraet: komm::Sendegeraet<stream::Order<StreamInhalt>>,
        maybe_stream: Option<komm::Stream<stream::Order<StreamInhalt>>>,
    }

    type StreamInhalt = komm::Rueckkopplung<LocalOrder, LocalStamp>;

    enum LocalJob {
        Sklave(SklaveJob<Welt, LocalOrder>),
        Stream(stream::Job<StreamInhalt>),
    }

    impl From<SklaveJob<Welt, LocalOrder>> for LocalJob {
        fn from(job: SklaveJob<Welt, LocalOrder>) -> Self {
            Self::Sklave(job)
        }
    }

    impl From<stream::Job<StreamInhalt>> for LocalJob {
        fn from(job: stream::Job<StreamInhalt>) -> Self {
            Self::Stream(job)
        }
    }

    impl From<SklaveJob<stream::Welt, stream::Order<StreamInhalt>>> for LocalJob {
        fn from(job: SklaveJob<stream::Welt, stream::Order<StreamInhalt>>) -> Self {
            Self::Stream(job.into())
        }
    }

    struct LocalJobUnit<J>(edeltraud::JobUnit<J, LocalJob>);

    impl<J> From<edeltraud::JobUnit<J, LocalJob>> for LocalJobUnit<J> {
        fn from(job_unit: edeltraud::JobUnit<J, LocalJob>) -> Self {
            Self(job_unit)
        }
    }

    impl<J> edeltraud::Job for LocalJobUnit<J> {
        fn run(self) {
            match self.0.job {
                LocalJob::Stream(job) => {
                    let job_unit =
                        stream::JobUnit::from(edeltraud::JobUnit { handle: self.0.handle, job, });
                    job_unit.run()
                },
                LocalJob::Sklave(mut sklave_job) =>
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
                                            let inhalt = befehle
                                                .local_sendegeraet
                                                .rueckkopplung(LocalStamp);
                                            let stream = befehle
                                                .stream_sendegeraet
                                                .stream_starten(
                                                    stream::OrderStreamStart {
                                                        start,
                                                        end,
                                                        stream_echo: inhalt,
                                                    },
                                                )
                                                .unwrap();
                                            assert!(befehle.maybe_stream.is_none());
                                            befehle.maybe_stream = Some(stream);
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::GotAnItem(komm::Umschlag {
                                                inhalt: komm::Streamzeug::NichtMehr(..),
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            let current = mem::take(&mut befehle.current);
                                            assert!(befehle.maybe_stream.is_some());
                                            befehle.maybe_stream = None;
                                            let tx_lock = befehle.tx.lock().unwrap();
                                            tx_lock.send(current).ok();
                                        },
                                        SklavenBefehl::Mehr {
                                            befehl: LocalOrder::GotAnItem(komm::Umschlag {
                                                inhalt: komm::Streamzeug::Zeug { zeug, mehr, },
                                                stamp: LocalStamp,
                                            }),
                                            mehr_befehle,
                                        } => {
                                            befehle = mehr_befehle;
                                            befehle.current.push(zeug);
                                            let stream =
                                                befehle.maybe_stream.as_ref().unwrap();
                                            assert_eq!(stream.stream_id(), mehr.stream_id());
                                            let inhalt = befehle
                                                .local_sendegeraet
                                                .rueckkopplung(LocalStamp);
                                            stream
                                                .mehr(
                                                    stream::OrderStreamNext {
                                                        stream_echo: inhalt,
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
                                            let current = mem::take(&mut befehle.current);
                                            assert!(befehle.maybe_stream.is_some());
                                            befehle.maybe_stream = None;
                                            let tx_lock = befehle.tx.lock().unwrap();
                                            tx_lock.send(current).ok();
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
                    },
            }
        }
    }

    let edeltraud = edeltraud::Builder::new()
        .build::<LocalJob, LocalJobUnit<LocalJob>>()
        .unwrap();
    let thread_pool = edeltraud.handle();

    let (tx, rx) = mpsc::channel();

    let stream_meister =
        stream::start(&thread_pool);
    let stream_sendegeraet =
        komm::Sendegeraet::starten(stream_meister, thread_pool.clone());

    let freie = Freie::new();
    let local_sendegeraet = komm::Sendegeraet::starten(
        freie.meister(),
        thread_pool.clone(),
    );

    let meister = freie
        .versklaven(
            Welt {
                tx: Mutex::new(tx),
                current: Vec::new(),
                local_sendegeraet,
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

    pub struct JobUnit<S, J>(edeltraud::JobUnit<J, Job<S>>);

    impl<S, J> From<edeltraud::JobUnit<J, Job<S>>> for JobUnit<S, J> {
        fn from(job_unit: edeltraud::JobUnit<J, Job<S>>) -> Self {
            Self(job_unit)
        }
    }

    impl<S, J> edeltraud::Job for JobUnit<S, J> where S: komm::Echo<komm::Streamzeug<isize>>, {
        fn run(self) {
            match self.0.job {
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
                                            let sklavenwelt = &mut *befehle;
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
                                            let sklavenwelt = &mut *befehle;
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
                                            let sklavenwelt = &mut *befehle;
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

    pub fn start<S, J>(thread_pool: &edeltraud::Handle<J>) -> Meister<Welt, Order<S>>
    where S: komm::Echo<komm::Streamzeug<isize>>,
          J: From<SklaveJob<Welt, Order<S>>>,
    {
        let freie = Freie::new();
        freie.versklaven(Welt::default(), thread_pool).unwrap()
    }
}
