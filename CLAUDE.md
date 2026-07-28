# MtconnectAdapter (Claude Code)

EdgeCommons southbound protocol adapter (Rust), `com.mbreissi.edgecommons.MtconnectAdapter`. The full picture — what
this component is, the device seam, config location, and the org conventions it inherits — lives in
`AGENTS.md` and is shared with every agent tool. It is imported here in full:

@AGENTS.md

## Local-dev notes

- **`--dep-source local`** (the default): `Cargo.toml`'s `edgecommons` dependency is already a path
  dependency into your sibling `libs/rust` checkout — no extra override needed, and it tracks your
  working copy live.
- **`--dep-source pinned-rev`**: `Cargo.toml` carries a git `rev` pin instead; the gitignored
  `.cargo/config.toml` `[patch]` block (emitted alongside it) points a plain `cargo build` at your
  local sibling checkout instead of fetching the pin, without touching the committed pin CI uses.
