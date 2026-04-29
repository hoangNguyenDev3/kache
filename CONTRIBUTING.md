# Contributing to Kache

Thank you for your interest in contributing to Kache! This document provides guidelines to help you get started.

## Getting Started

### Prerequisites

- Go 1.21 or later
- golangci-lint (for local linting)

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/hoangNguyenDev3/kache.git
   cd kache
   ```

2. Build the project:
   ```bash
   make build
   ```

## Development Workflow

1. Fork the repository and create your branch from `main`.
2. Make your changes, following the code style guidelines below.
3. Run the test suite and linter to ensure everything passes.
4. Commit your changes using the commit message format described below.
5. Open a pull request.

## Running Tests

| Command | Description |
|---------|-------------|
| `make test` | Run all unit tests |
| `make bench` | Run benchmarks |
| `make test-cover` | Run tests with coverage report |

## Code Style

- All Go code must be formatted with `gofmt`.
- Linting with `golangci-lint` must pass without errors.
- Use meaningful variable and function names.
- Add `godoc` comments to all exported symbols.

## Commit Messages

Use conventional commit prefixes:

- `feat:` — new feature (e.g., `feat: add sorted set support`)
- `fix:` — bug fix (e.g., `fix: handle nil pointer in HGET`)
- `docs:` — documentation changes (e.g., `docs: update README benchmarks`)
- `test:` — test changes (e.g., `test: add concurrent hash test`)

Reference related GitHub issues when applicable.

## Pull Request Process

1. Create a descriptive pull request explaining what changed and why.
2. Ensure CI checks pass.
3. Add tests for any new features or bug fixes.
4. Update the README if your change affects user-facing behavior.
5. Pull requests require at least one approval before merging.

## Reporting Issues

Please use [GitHub Issues](https://github.com/hoangNguyenDev3/kache/issues) to report bugs or request features.

When reporting a bug, include:

- Go version (`go version`)
- Operating system and version
- Steps to reproduce the issue
- Expected vs. actual behavior
