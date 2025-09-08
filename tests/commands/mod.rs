use predicates::prelude::*;
use tempfile::TempDir;

use crate::support::cli;

pub mod run;

#[test]
fn test_cli_version() {
    cli()
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::contains(env!("CARGO_PKG_NAME")));
}

#[test]
fn test_cli_help() {
    cli()
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Package Revision (PR) server for BitBake builds",
        ))
        .stdout(predicate::str::contains("Commands:"))
        .stdout(predicate::str::contains("run"));
}

#[test]
fn test_invalid_command() {
    cli()
        .arg("invalid-command")
        .assert()
        .failure()
        .stderr(predicate::str::contains("unrecognized subcommand"));
}

#[test]
fn test_global_verbose_flag() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("-vv")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("INFO"));
}

#[test]
fn test_global_log_level() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("-L")
        .arg("debug")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("DEBUG"));
}

#[test]
fn test_syslog_numeric_levels() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    // Test numeric level 6 (info)
    cli()
        .current_dir(temp_dir.path())
        .arg("-L")
        .arg("6")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("INFO"));
}

#[test]
fn test_case_insensitive_log_levels() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    // Test uppercase
    cli()
        .current_dir(temp_dir.path())
        .arg("-L")
        .arg("INFO")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("INFO"));

    // Test mixed case
    cli()
        .current_dir(temp_dir.path())
        .arg("-L")
        .arg("Info")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("INFO"));
}

#[test]
fn test_config_flag() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    let config_file = temp_dir.path().join("config.json");
    std::fs::write(&config_file, r#"{"test": true}"#).unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("-C")
        .arg(config_file.to_str().unwrap())
        .arg("-vv")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("Using configuration file"));
}

#[test]
fn test_verbose_increment() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    // Test multiple -v flags
    cli()
        .current_dir(temp_dir.path())
        .arg("-v")
        .arg("-v")
        .arg("-v")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("INFO"));
}

#[test]
fn test_log_level_with_verbose() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    std::fs::write(&test_file, "test content").unwrap();

    // Test log level with verbose increment
    cli()
        .current_dir(temp_dir.path())
        .arg("-L")
        .arg("warn")
        .arg("-vv")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stderr(predicate::str::contains("INFO"));
}
