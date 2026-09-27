package domain

// Kind é o tipo de uma transação de aposta.
type Kind string

// Tipos de transação. OPENING é somente interno; os demais são externos.
const (
	KindOpening  Kind = "OPENING"
	KindBet      Kind = "BET"
	KindWin      Kind = "WIN"
	KindLoss     Kind = "LOSS"
	KindRefund   Kind = "REFUND"
	KindRollback Kind = "ROLLBACK"
)

// ParseExternalKind valida um tipo recebido de um provedor (HTTP ou SQS).
// OPENING é reservado para a abertura interna de carteiras e é rejeitado.
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

// ParseKind valida qualquer tipo, incluindo OPENING (usado na reidratação).
func ParseKind(s string) (Kind, error) {
	if Kind(s) == KindOpening {
		return KindOpening, nil
	}
	return ParseExternalKind(s)
}

// RequiresReference informa se o tipo sempre precisa de uma referência.
func (k Kind) RequiresReference() bool {
	return k == KindRefund || k == KindRollback
}

// IsReversal informa se o tipo reverte uma transação anterior.
func (k Kind) IsReversal() bool { return k.RequiresReference() }

// Status é o estado do ciclo de vida de uma transação de aposta.
//
// Máquina de estados (todas as outras transições são rejeitadas):
//
//	PENDING ──────────────┬──> PROCESSED  (terminal)
//	   │                  ├──> REJECTED   (terminal)
//	   v                  └──> FAILED     (terminal)
//	PENDING_REFERENCE ────┘
//	   ^  │
//	   └──┘ (reagendar: attempts++ / próxima tentativa avança)
type Status string

// Status das transações.
const (
	StatusPending          Status = "PENDING"
	StatusPendingReference Status = "PENDING_REFERENCE"
	StatusProcessed        Status = "PROCESSED"
	StatusRejected         Status = "REJECTED"
	StatusFailed           Status = "FAILED"
)

// ParseStatus valida uma string de status (reidratação).
func ParseStatus(s string) (Status, error) {
	switch st := Status(s); st {
	case StatusPending, StatusPendingReference, StatusProcessed, StatusRejected, StatusFailed:
		return st, nil
	default:
		return "", invalidArg("unknown status %q", s)
	}
}

// IsTerminal informa se nenhuma transição adicional é permitida.
func (s Status) IsTerminal() bool {
	return s == StatusProcessed || s == StatusRejected || s == StatusFailed
}

// Direction é o lado de uma entrada do ledger.
type Direction string

// Direções do ledger.
const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// ParseDirection valida uma string de direção (reidratação).
func ParseDirection(s string) (Direction, error) {
	switch d := Direction(s); d {
	case DirectionDebit, DirectionCredit:
		return d, nil
	default:
		return "", invalidArg("unknown direction %q", s)
	}
}

// Opposite retorna a direção inversa (usada pelo ROLLBACK).
func (d Direction) Opposite() Direction {
	if d == DirectionDebit {
		return DirectionCredit
	}
	return DirectionDebit
}

// Origin distingue transações criadas internamente (OPENING) de operações
// externas de provedores.
type Origin string

// Origens das transações.
const (
	OriginInternal Origin = "INTERNAL"
	OriginExternal Origin = "EXTERNAL"
)
