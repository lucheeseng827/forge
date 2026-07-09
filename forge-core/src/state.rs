//! The item state machine — `Pending → Leased → Done | DeadLetter`.
//!
//! Three states, **zero inter-item edges**. This is the dagron boundary expressed
//! as a type: there is no transition that depends on *another* item's state, no
//! "successor", no fan-in. It is a work queue, not a scheduler.

use serde::{Deserialize, Serialize};

/// The lifecycle of a single fan-out item.
///
/// ```text
///   Pending ──lease──▶ Leased ──ack(after store write)──▶ Done
///      ▲                  │
///      └──reap(expired)───┘ ──attempts ≥ max──▶ DeadLetter
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ItemState {
    /// Waiting to be leased.
    #[default]
    Pending,
    /// Leased to a worker until `leased_until`; counted as in-flight.
    Leased,
    /// Result durably written to the store, then acked. Terminal, success.
    Done,
    /// Retries exhausted (a poison item). Terminal, failure. Quarantined to the
    /// dead-letter JSONL so one bad prompt cannot wedge the whole job.
    DeadLetter,
}

impl ItemState {
    /// Terminal states are never leased again.
    pub fn is_terminal(self) -> bool {
        matches!(self, ItemState::Done | ItemState::DeadLetter)
    }

    /// Whether a transition is legal. The queue backend must reject anything else,
    /// so an out-of-order ack/reap can never corrupt progress.
    pub fn can_transition_to(self, next: ItemState) -> bool {
        use ItemState::*;
        matches!(
            (self, next),
            (Pending, Leased)        // lease
                | (Leased, Done)     // ack, only after a successful store write
                | (Leased, Pending)  // reaper re-queues an expired lease
                | (Leased, DeadLetter) // retries exhausted
        )
    }

    /// Wire/SQL token for this state.
    pub fn as_str(self) -> &'static str {
        match self {
            ItemState::Pending => "pending",
            ItemState::Leased => "leased",
            ItemState::Done => "done",
            ItemState::DeadLetter => "dead_letter",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_is_pending() {
        assert_eq!(ItemState::default(), ItemState::Pending);
    }

    #[test]
    fn legal_transitions_only() {
        assert!(ItemState::Pending.can_transition_to(ItemState::Leased));
        assert!(ItemState::Leased.can_transition_to(ItemState::Done));
        assert!(ItemState::Leased.can_transition_to(ItemState::Pending));
        assert!(ItemState::Leased.can_transition_to(ItemState::DeadLetter));
        // illegal: skip Leased, resurrect a terminal, etc.
        assert!(!ItemState::Pending.can_transition_to(ItemState::Done));
        assert!(!ItemState::Done.can_transition_to(ItemState::Leased));
        assert!(!ItemState::DeadLetter.can_transition_to(ItemState::Pending));
    }

    #[test]
    fn terminals() {
        assert!(ItemState::Done.is_terminal());
        assert!(ItemState::DeadLetter.is_terminal());
        assert!(!ItemState::Leased.is_terminal());
    }
}
