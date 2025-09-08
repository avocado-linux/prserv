use clap::Subcommand;

pub mod client;
pub mod server;

#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the PR server
    Server(server::Args),

    /// PR client commands
    Client(client::Args),
}
