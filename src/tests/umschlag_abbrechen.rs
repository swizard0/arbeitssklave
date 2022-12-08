use std::{
    sync::{
        mpsc,
    },
};

use crate::{
    komm,
    utils,
};

#[test]
fn basic() {
    struct LocalStamp;

    struct LocalOrder(komm::UmschlagAbbrechen<LocalStamp>);

    impl From<komm::UmschlagAbbrechen<LocalStamp>> for LocalOrder {
        fn from(umschlag_abbrechen: komm::UmschlagAbbrechen<LocalStamp>) -> LocalOrder {
            LocalOrder(umschlag_abbrechen)
        }
    }

    let edeltraud = edeltraud::Builder::new()
        .build::<_, utils::mpsc_forward_adapter::JobUnit<_, _>>()
        .unwrap();
    let thread_pool = edeltraud.handle();

    let (sync_tx, sync_rx) = mpsc::sync_channel(0);
    let adapter =
        utils::mpsc_forward_adapter::Adapter::versklaven(sync_tx, &thread_pool).unwrap();

    let sendegeraet =
        komm::Sendegeraet::starten(
            &adapter.sklave_meister,
            #[allow(clippy::redundant_clone)]
            thread_pool.clone(),
        );
    let rueckkopplung = sendegeraet.rueckkopplung(LocalStamp);
    drop(rueckkopplung);

    assert!(matches!(
        sync_rx.recv_timeout(std::time::Duration::from_millis(1000)),
        Ok(LocalOrder(komm::UmschlagAbbrechen { stamp: LocalStamp, })),
    ));
}
