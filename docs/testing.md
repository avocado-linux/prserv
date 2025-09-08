# Testing

## Structure

- Unit tests: Inline with source in `#[cfg(test)]` modules
- Integration tests: `tests/` directory with command-level tests
- Shared utilities: `tests/support/mod.rs` for test fixtures

## CI checks

The `scripts/checks.sh` script runs all CI checks locally:
- Format verification
- Clippy lints with warnings as errors
- Compilation check for all targets
- Security audit (cargo-audit)
- Test coverage with cargo-llvm-cov

## Coverage

HTML reports generated locally, Cobertura XML in CI for Codecov integration.

## Environment isolation

Tests that modify environment variables run single-threaded to prevent interference.
