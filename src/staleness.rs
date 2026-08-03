//! # Passive quality — the link facts a held reading is judged against (HLD §6)
//!
//! MTConnect is an **on-change** protocol: an unchanged value is not a silent one, so a signal's own
//! age says nothing about whether its value is still true. What does say so is the **liveness
//! clock** — the moment the agent last vouched for currency, either by delivering a Streams document
//! (data or heartbeat) or by answering a `/current` cycle. This module owns that vocabulary, apart
//! from the device seam, so it is decided on an explicit clock and unit-tested without a runtime.
//!
//! [`PassiveLink`] is what a session reports through
//! [`DeviceSession::passive_input`](crate::device::DeviceSession::passive_input): the connectivity
//! verdict of the one authority that owns it, plus the liveness age and the window that age is
//! measured against. A backend whose read IS its liveness reports `None` and is never judged
//! passively at all.

use std::time::Duration;

/// The link facts a passive-quality evaluation reads.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PassiveLink {
    /// The connectivity authority says the agent is not delivering.
    pub unreachable: bool,
    /// Time since the agent last vouched for data currency. `None` before first contact.
    pub liveness_age: Option<Duration>,
    /// "One missed heartbeat/poll" for this acquisition mode — the threshold `liveness_age` is
    /// judged against.
    pub liveness_window: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_link_facts_are_a_plain_value_a_test_can_pin() {
        let fresh = PassiveLink {
            unreachable: false,
            liveness_age: Some(Duration::from_millis(250)),
            liveness_window: Duration::from_secs(10),
        };
        assert_eq!(fresh, fresh);
        assert!(fresh
            .liveness_age
            .is_some_and(|age| age < fresh.liveness_window));

        // Before first contact there is no age to judge — and that is not the same as "old".
        let cold = PassiveLink {
            liveness_age: None,
            ..fresh
        };
        assert_ne!(cold, fresh);
        assert!(
            !cold.unreachable,
            "never having answered is not the same as having gone away"
        );
    }
}
