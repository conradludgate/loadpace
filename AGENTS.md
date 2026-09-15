# Contribution guide

## Repository shape

This is a Cargo workspace targeting Rust 2024. The core and Tower crates
support Rust 1.85 or newer; the Rama adapter requires Rust 1.96 or newer to
match Rama 0.4's MSRV. The workspace contains:

- `loadpace`: runtime-independent controller, GCRA pacer, RTT estimator,
  probes, and deterministic simulator;
- `loadpace-tower`: Tower service and discovery integration;
- `loadpace-rama`: Rama service and layer integration.

Keep framework integrations in their own crates. The core crate must remain
independent of async runtimes and frameworks.

## Invariants

- Preserve bounded admission and visible backpressure. Do not add hidden,
  unbounded queues.
- Keep GCRA as the normal pacing actuator; the inflight cap is an emergency
  safety limit.
- Preserve fractional controller values and deterministic simulator behavior.
- Keep the crates free of unsafe code. Each library crate forbids unsafe code.
- Prefer standard-library or small, well-established dependencies. Use
  `pin_project_lite` for custom pinned futures rather than writing unsafe pin
  projections.
- Use the current dependency lines already established by the workspace,
  including `rand` 0.10 and Rama 0.4.

## Before committing

Use conventional commits, with one small logical change per commit. Run the
following commands from the repository root and do not push while any command
fails:

```text
cargo fmt --all -- --check
cargo check --workspace --no-default-features --lib
cargo test --workspace --no-default-features --all-targets
cargo clippy --workspace --all-features --all-targets -- -D warnings
cargo test --workspace --all-features --all-targets
cargo doc --workspace --no-deps --all-features
```

When a formatter or test exposes an issue, fix it before making the commit.
For controller changes, add deterministic tests and run the simulator or
fairness scenarios that exercise the affected behavior.

## Git workflow

Inspect `git status` before editing and preserve unrelated user changes. Make
small commits after each verified logical chunk, then push those commits so
the history remains easy to review. Never include credentials in remotes,
files, command output, or commit messages.

## Release workflow

The workspace uses release-plz to generate changelog entries from conventional
commits. Do not edit crate changelogs manually as part of implementation work;
write accurate conventional commit messages and leave changelog updates to the
release-plz release PR. Keep package versions and inter-package dependency
requirements consistent, and verify release-plz configuration changes
separately from implementation changes. Crates must exist on crates.io before
trusted publishing can be configured for them.
