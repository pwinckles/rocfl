export RUST_BACKTRACE := "1"

default:
  just --list

# Build debug version
build:
  cargo build

# Build release version
build-release:
  cargo build --release

# Lint
lint:
  cargo clippy --all --all-targets --all-features

# Format code
fmt:
  cargo +nightly fmt --all

# Run fmt, lint, and test
check: fmt lint test

# Update dependencies
update:
  cargo update

# Install locally
install:
  cargo install --path .

# Run tests
test:
  cargo test

# Run tests that match the pattern
test-filter PATTERN:
  cargo test {{PATTERN}}

# Run tests against S3. Requires: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and OCFL_TEST_S3_BUCKET
test-s3:
  cargo test -- --test-threads=1

# Run the tests and dump trycmd output to `dump`
test-dump:
  TRYCMD=dump cargo test

# Run the tests and overwrite snapshots
test-overwrite:
 TRYCMD=overwrite cargo test

# Create a release version and publish it
release *FLAGS:
  cargo release --dev-version {{FLAGS}}
