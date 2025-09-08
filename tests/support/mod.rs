use assert_cmd::Command;

/// Create a new Command instance for the CLI binary
pub fn cli() -> Command {
    Command::cargo_bin(env!("CARGO_PKG_NAME")).unwrap()
}
