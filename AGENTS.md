# Repository Guidelines

Check the root `CLAUDE.md` file for repo defaults.
Consult crate-specific `CLAUDE.md` or `AGENTS.md` files when changing files in those crates.

## Branch Conventions

This repo uses the following branch roles for Sui upgrades and local modifications:

- `mainnet-vX.Y.Z` tag: exact upstream Sui mainnet release baseline.
- `arb/mainnet-vX.Y.Z`: local modified branch based on the matching upstream `mainnet-vX.Y.Z` tag.
- `main`: the latest confirmed local modified version. This branch follows the current stable `arb/mainnet-vX.Y.Z`.

Rules:

- Do not align local `main` with upstream `main`.
- Treat upstream `main` as the development branch, not the mainnet release line.
- When a new Sui mainnet tag is released, push that tag to `origin`, then derive a new `arb/mainnet-vX.Y.Z` branch from it.
- After the new `arb/mainnet-vX.Y.Z` branch is validated, move `main` to that branch.
