//! Build script for prserv
//!
//! This script sets up build-time environment variables.

use std::env;
use std::process::Command;

fn main() {
    // Get git hash, fallback to "unknown" if git is not available
    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map_or_else(|| "unknown".to_string(), |s| s.trim().to_string());

    // version
    println!(
        "cargo:rustc-env=CARGO_PKG_VERSION={} {}",
        env!("CARGO_PKG_VERSION"),
        git_hash
    );

    // target
    let target = env::var("TARGET").unwrap_or_else(|_| "unknown".to_string());
    println!("cargo:rustc-env=TARGET={target}");
}
