#!/usr/bin/env bash

set -e

# Check for cargo-llvm-cov (skip check in CI as it's installed by workflow)
if [ -z "$CI" ]; then
    if ! command -v cargo-llvm-cov &> /dev/null; then
        echo -e "\033[1;31m[ERROR]\033[0m cargo-llvm-cov is not installed."
        echo "cargo install cargo-llvm-cov"
        exit 1
    fi
fi

# Run tests and measure code coverage during
if [ -z "$CI" ]; then
    cargo llvm-cov --all-features --workspace --html
else
    cargo llvm-cov --all-features --workspace --cobertura --output-path cobertura.xml
fi
