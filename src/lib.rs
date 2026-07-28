//! # Library crate — the seam, exposed for integration tests
//!
//! The engine is exposed as a library so the thin `main` binary and the integration tests
//! (`tests/`) build against **one** public surface. The binary is a shim: it constructs the
//! `edgecommons` runtime and hands control to [`supervisor::App`]. The pure, unit-tested behavior —
//! the device config/backoff/health/connectivity logic ([`app`]), the device seam ([`device`]), the
//! `sb/*` command surface ([`commands`]), and the operational metrics ([`metrics`]) — lives in those
//! modules; the async connect/poll/reconnect **drivers** that `.await` a live session live in
//! [`supervisor`] and are driven end-to-end from `tests/live_sim.rs` once you replace the shipped
//! simulator with a real protocol.

pub mod app;
pub mod commands;
pub mod device;
pub mod metrics;
pub mod supervisor;
