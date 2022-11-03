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

    let thread_pool: edeltraud::Edeltraud<utils::mpsc_forward_adapter::Job<LocalOrder>> = edeltraud::Builder::new()
        .build()
        .unwrap();

    let (sync_tx, sync_rx) = mpsc::sync_channel(0);
    let adapter =
        utils::mpsc_forward_adapter::Adapter::versklaven(sync_tx, &thread_pool).unwrap();

    let rueckkopplung = adapter.sklave_sendegeraet.rueckkopplung(LocalStamp);
    drop(rueckkopplung);

    assert!(matches!(
        sync_rx.recv_timeout(std::time::Duration::from_millis(100)),
        Ok(LocalOrder(komm::UmschlagAbbrechen { stamp: LocalStamp, })),
    ));
}
