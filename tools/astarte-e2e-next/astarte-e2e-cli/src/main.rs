mod config;
pub mod interfaces;

use std::process::Command;

use astarte_e2e::{
    add, device_client, interfaces::AstarteClient, transport::phoenix_channel::PhoenixChannel,
};
use tokio::task::JoinSet;

use crate::config::{AstarteConfig2, Config};
use clap::Parser;

fn main() {
    let config = Config::parse();
    let result = add(3, 4);
    dbg!(result);
    println!("Hello, world!");
}

pub async fn connect_to_astarte() -> eyre::Result<(PhoenixChannel, AstarteClient)> {
    let astarte_config = AstarteConfig2::parse();
    let appengine_ws = astarte_config.astarte.appengine_websocket()?;
    let store = Command::new("mktemp").arg("-d").output()?.stdout;
    let store = str::from_utf8(&store)?;

    let (tx_cancel, _cancel) = tokio::sync::broadcast::channel::<()>(2);
    let mut tasks = JoinSet::<eyre::Result<()>>::new();

    let channel = PhoenixChannel::connect(
        appengine_ws,
        &astarte_config.astarte.realm,
        &astarte_config.astarte.jwt,
        &astarte_config.astarte.device_id,
        &mut tasks,
        tx_cancel.subscribe(),
    )
    .await?;

    let client = device_client(
        &astarte_config.astarte.realm,
        &astarte_config.astarte.device_id,
        &astarte_config.astarte.jwt,
        &astarte_config.astarte.astarte_pairing_url.path(),
        store,
        astarte_config.astarte.ignore_ssl,
    )
    .await?;

    Ok((channel, client))
}
