package domain

// Kind is the type of a wager transaction.
type Kind string

// Transaction kinds. OPENING is internal only; the others are external.
const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseExternalKind validates a kind received from a provider (HTTP or SQS).
// OPENING is reserved for the internal wallet opening and is rejected.
func ParseExternalKind(s string) (Kind, error) {
	switch k := Kind(s); k {
	case KindBet, KindWin, KindLoss, KindRefund, KindRollback:
		return k, nil
	case KindOpening:
		return "", invalidArg("kind OPENING is reserved for internal wallet opening")
	default:
		return "", invalidArg("unknown kind %q", s)
	}
}

// ParseKind validates any kind, including OPENING (used on rehydration).
func ParseKind(s string) (Kind, error) {
	if Kind(s) == KindOpening {
		return KindOpening, nil
	}
	return ParseExternalKind(s)
}

// RequiresReference reports whether the kind always needs a reference.
func (k Kind) RequiresReference() bool {
	return k == KindRefund || k == KindRollback
}

// IsReversal reports whether the kind reverses a previous transaction.
func (k Kind) IsReversal() bool { return k.RequiresReference() }

// Status is the lifecycle state of a wager transaction.
//
// State machine (all other transitions are rejected):
//
//	PENDING ──────────────┬──> PROCESSED  (terminal)
//	   │                  ├──> REJECTED   (terminal)
//	   v                  └──> FAILED     (terminal)
//	PENDING_REFERENCE ────┘
//	   ^  │
//	   └──┘ (reschedule: attempts++ / next attempt moved forward)
type Status string

// Transaction statuses.
const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// ParseStatus validates a status string (rehydration).
func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return st, nil
	default:
		return "", invalidArg("unknown status %q", s)
	}
}

// IsTerminal reports whether no further transition is allowed.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Direction is the side of a ledger entry.
type Direction string

// Ledger directions.
const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// ParseDirection validates a direction string (rehydration).
func ParseDirection(s string) (Direction, error) {
	switch d := Direction(s); d {
	case DirectionDebit, DirectionCredit:
		return d, nil
	default:
		return "", invalidArg("unknown direction %q", s)
	}
}

// Opposite returns the reverse direction (used by ROLLBACK).
func (d Direction) Opposite() Direction {
	if d == DirectionDebit {
		return DirectionCredit
	}
	return DirectionDebit
}

// Origin distinguishes internally created transactions (OPENING) from
// external provider operations.
type Origin string

// Transaction origins.
const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)
