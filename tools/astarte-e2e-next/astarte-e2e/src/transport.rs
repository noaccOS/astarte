// use crate::astarte_event::SimpleEvent;
// use tokio::task::JoinSet;

// mod phoenix_channel;

// pub trait Transport {
//     async fn connect(realm: String, device_id: String, tasks: &mut JoinSet<eyre::Result<()>>) {}
//     async fn next() -> SimpleEvent {}
// }
pub mod phoenix_channel;
