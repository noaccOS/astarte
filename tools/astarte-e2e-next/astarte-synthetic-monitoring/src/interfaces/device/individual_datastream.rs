use std::time::Duration;

use astarte_e2e::{
    interfaces::AstarteClient,
    room::Room,
    scenarios::interfaces::device::individual_datastream::{Config, RonundTripStrategy},
};
use tokio::time::sleep;

pub async fn run(config: &Config, client: &mut AstarteClient) -> eyre::Result<()> {
    match config.roudtrip_strategy {
        RonundTripStrategy::Volatile => run_volatile(config, client).await,
        _ => todo!(),
    }
}

pub async fn run_volatile(config: &Config, client: &mut AstarteClient) -> eyre::Result<()> {
    loop {
        config
            .individual_datastream_variant
            .run(channel, client)
            .await?;
        sleep(Duration::from_secs(config.check_interval)).await;
    }
}
