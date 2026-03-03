# Run all quality checks: format, lint, build, and test.
check:
    cargo fmt
    cargo fmt --check
    cargo clippy --all-targets
    cargo build
    cargo nextest run --no-tests pass
