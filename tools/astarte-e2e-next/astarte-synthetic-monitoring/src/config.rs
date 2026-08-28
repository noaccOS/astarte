use astarte_e2e::scenarios::{self, interfaces::device::individual_datastream};

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
pub struct Config {
    #[command(subcommand)]
    pub monitor: Monitor,
    #[command(flatten)]
    pub e2e_config: astarte_e2e::config::Config,
}

impl Config {
    pub fn run(&self) -> eyre::Result<()> {

    }
}

#[derive(Debug, Subcommand)]
pub enum Monitor {
    IndividualDatastream(individual_datastream::Config),
}

impl Monitor {
    pub async fn run(&self) -> eyre::Result<()> {
        match self {
            Self::IndividualDatastream(config) =>
        }
    }
}
