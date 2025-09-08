use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

use crate::support::cli;

#[test]
fn test_run_with_valid_input() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("input.txt");
    fs::write(
        &test_file,
        "Hello, world!\nThis is a test file.\nIt has lines.\n",
    )
    .unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("Lines:"))
        .stdout(predicate::str::contains("Words:"))
        .stdout(predicate::str::contains("Bytes:"))
        .stdout(predicate::str::contains("[SUCCESS]"));
}

#[test]
fn test_run_with_nonexistent_file() {
    cli()
        .arg("run")
        .arg("--input")
        .arg("nonexistent.txt")
        .assert()
        .failure()
        .stderr(predicate::str::contains("File not found: nonexistent.txt"));
}

#[test]
fn test_run_stats_only() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("data.txt");
    fs::write(
        &test_file,
        "Hello, world!\nThis is a test file.\nIt has lines.\n",
    )
    .unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .arg("--stats-only")
        .assert()
        .success()
        .stdout(predicate::str::contains("File statistics for"))
        .stdout(predicate::str::contains("Lines: 3"))
        .stdout(predicate::str::contains("Words: 10"))
        .stdout(predicate::str::contains("Bytes: 49"));
}

#[test]
fn test_run_with_output_file() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("input.txt");
    fs::write(&test_file, "hello world").unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .arg("--output")
        .arg("output.txt")
        .assert()
        .success()
        .stdout(predicate::str::contains("Output written to: output.txt"))
        .stdout(predicate::str::contains("[SUCCESS]"));

    // Verify output file was created with uppercase content
    let output_content = fs::read_to_string(temp_dir.path().join("output.txt")).unwrap();
    assert_eq!(output_content, "HELLO WORLD");
}

#[test]
fn test_run_with_verbose_logging() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    fs::write(&test_file, "test content").unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("-vv")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .arg("--stats-only")
        .assert()
        .success()
        .stderr(predicate::str::contains("INFO"))
        .stderr(predicate::str::contains("Processing file"));
}

#[test]
fn test_run_with_debug_logging() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    fs::write(&test_file, "test content").unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("-L")
        .arg("debug")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .arg("--stats-only")
        .assert()
        .success()
        .stderr(predicate::str::contains("DEBUG"))
        .stderr(predicate::str::contains("Reading file contents"));
}

#[test]
fn test_run_with_large_file() {
    let temp_dir = TempDir::new().unwrap();
    let large_content = (0..1000)
        .map(|i| {
            format!(
                "Line {}: This is test content for line number {}.",
                i + 1,
                i + 1
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let test_file = temp_dir.path().join("large.txt");
    fs::write(&test_file, &large_content).unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .arg("--stats-only")
        .assert()
        .success()
        .stdout(predicate::str::contains("Lines: 1000"))
        .stdout(predicate::str::contains("Words: 10000"));
}

#[test]
fn test_run_with_json_file() {
    let temp_dir = TempDir::new().unwrap();
    let json_content = r#"{
  "test": true,
  "value": 42,
  "items": ["one", "two", "three"]
}"#;
    let test_file = temp_dir.path().join("data.json");
    fs::write(&test_file, json_content).unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("Lines:"))
        .stdout(predicate::str::contains("Words:"))
        .stdout(predicate::str::contains("Bytes:"))
        .stdout(predicate::str::contains("Lines: 5"));
}

#[test]
fn test_run_output_overwrites_existing() {
    let temp_dir = TempDir::new().unwrap();

    // Create initial output file
    let output_file = temp_dir.path().join("output.txt");
    fs::write(&output_file, "old content").unwrap();

    // Create input file
    let input = temp_dir.path().join("input.txt");
    fs::write(&input, "new content").unwrap();

    // Run command to overwrite
    cli()
        .current_dir(temp_dir.path())
        .arg("run")
        .arg("--input")
        .arg(input.to_str().unwrap())
        .arg("--output")
        .arg("output.txt")
        .assert()
        .success();

    // Verify file was overwritten
    let content = fs::read_to_string(&output_file).unwrap();
    assert_eq!(content, "NEW CONTENT");
}

#[test]
fn test_run_with_config_option() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("input.txt");
    fs::write(&test_file, "test").unwrap();

    let config_file = temp_dir.path().join("custom.json");
    fs::write(&config_file, r#"{"test": true}"#).unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("-C")
        .arg(config_file.to_str().unwrap())
        .arg("-vv")
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .arg("--stats-only")
        .assert()
        .success()
        .stderr(predicate::str::contains("Using configuration file"));
}

#[test]
fn test_run_empty_file() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("empty.txt");
    fs::write(&test_file, "").unwrap();

    cli()
        .current_dir(temp_dir.path())
        .arg("run")
        .arg("--input")
        .arg(test_file.to_str().unwrap())
        .assert()
        .success()
        .stdout(predicate::str::contains("Lines: 0"))
        .stdout(predicate::str::contains("Words: 0"))
        .stdout(predicate::str::contains("Bytes: 0"));
}

#[test]
fn test_run_with_multiple_verbose_flags() {
    let temp_dir = TempDir::new().unwrap();
    let test_file = temp_dir.path().join("test.txt");
    fs::write(&test_file, "content").unwrap();

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
fn test_run_help() {
    cli()
        .arg("run")
        .arg("--help")
        .assert()
        .success()
        .stdout(predicate::str::contains("Run the main functionality"))
        .stdout(predicate::str::contains("--input"))
        .stdout(predicate::str::contains("--output"))
        .stdout(predicate::str::contains("--stats-only"));
}
