use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    komm::{
        Sendegeraet,
        UmschlagAbbrechen,
    },
    utils,
    Freie,
};

#[test]
fn basic() {
    struct LocalStamp;

    struct LocalOrder(UmschlagAbbrechen<LocalStamp>);

    impl From<UmschlagAbbrechen<LocalStamp>> for LocalOrder {
        fn from(umschlag_abbrechen: UmschlagAbbrechen<LocalStamp>) -> LocalOrder {
            LocalOrder(umschlag_abbrechen)
        }
    }

    let thread_pool: edeltraud::Edeltraud<utils::mpsc_forward_adapter::Job<LocalOrder>> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let freie = Freie::new();
    let sendegeraet = Sendegeraet::starten(&freie, thread_pool.clone()).unwrap();
    let (sync_tx, sync_rx) = mpsc::sync_channel(0);
    let _meister = utils::mpsc_forward_adapter::into(freie, sync_tx, &thread_pool).unwrap();

    let rueckkopplung = sendegeraet.rueckkopplung(LocalStamp);
    drop(rueckkopplung);

    assert!(matches!(
        sync_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Ok(LocalOrder(UmschlagAbbrechen { stamp: LocalStamp, })),
    ));
}
