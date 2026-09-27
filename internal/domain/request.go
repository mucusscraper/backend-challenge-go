package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"unicode"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain/money"
)

// Field limits for external identifiers.
const (
	maxIdentifierLength     = 128
	maxIdempotencyKeyLength = 255
)

// RawExternalRequest is the untrusted, transport-neutral shape of a provider
// operation. HTTP and SQS adapters both fill this struct and hand it to
// NewExternalRequest, which guarantees identical validation and hashing.
type RawExternalRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

// ExternalRequest is a validated provider operation. Its fields are
// unexported so it can only be obtained through NewExternalRequest.
type ExternalRequest struct {
	providerID            string
	externalTransactionID string
	idempotencyKey        string
	playerID              uuid.UUID
	walletID              uuid.UUID
	roundID               string
	gameID                string
	kind                  Kind
	money                 money.Money
	referenceExternalID   string
	payloadHash           string
}

// NewExternalRequest validates a raw request and computes its payload hash.
// All validation problems are joined into a single error wrapping
// ErrInvalidArgument, so a client gets every issue at once.
//
// Zero-amount policy: LOSS must be exactly 0.00; BET, WIN, REFUND and
// ROLLBACK must be > 0. Negative amounts are always rejected.
func NewExternalRequest(raw RawExternalRequest) (ExternalRequest, error) {
	var errs []error
	check := func(err error) {
		if err != nil {
			errs = append(errs, err)
		}
	}
	check(validateIdentifier("providerId", raw.ProviderID, maxIdentifierLength))
	check(validateIdentifier("externalTransactionId", raw.ExternalTransactionID, maxIdentifierLength))
	check(validateIdentifier("idempotencyKey", raw.IdempotencyKey, maxIdempotencyKeyLength))
	check(validateIdentifier("roundId", raw.RoundID, maxIdentifierLength))
	check(validateIdentifier("gameId", raw.GameID, maxIdentifierLength))

	playerID, err := parseUUID("playerId", raw.PlayerID)
	check(err)
	walletID, err := parseUUID("walletId", raw.WalletID)
	check(err)

	kind, err := ParseExternalKind(raw.Kind)
	check(err)

	m, err := money.ParseNonNegative(raw.Amount, raw.Currency)
	if err != nil {
		errs = append(errs, invalidArg("%v", err))
	} else if kind != "" {
		check(validateAmountForKind(kind, m))
	}

	if raw.ReferenceExternalTransactionID != "" {
		check(validateIdentifier("referenceExternalTransactionId", raw.ReferenceExternalTransactionID, maxIdentifierLength))
		if kind == KindBet || kind == KindLoss {
			errs = append(errs, invalidArg("referenceExternalTransactionId is not allowed for %s", kind))
		}
		if raw.ReferenceExternalTransactionID == raw.ExternalTransactionID {
			errs = append(errs, invalidArg("a transaction cannot reference itself"))
		}
	} else if kind.RequiresReference() {
		errs = append(errs, invalidArg("referenceExternalTransactionId is required for %s", kind))
	}

	if len(errs) > 0 {
		return ExternalRequest{}, errors.Join(errs...)
	}
	r := ExternalRequest{
		providerID:            raw.ProviderID,
		externalTransactionID: raw.ExternalTransactionID,
		idempotencyKey:        raw.IdempotencyKey,
		playerID:              playerID,
		walletID:              walletID,
		roundID:               raw.RoundID,
		gameID:                raw.GameID,
		kind:                  kind,
		money:                 m,
		referenceExternalID:   raw.ReferenceExternalTransactionID,
	}
	r.payloadHash = r.computeHash()
	return r, nil
}

// validateAmountForKind applies the zero-amount policy of each kind.
func validateAmountForKind(kind Kind, m money.Money) error {
	switch kind {
	case KindLoss:
		if !m.IsZero() {
			return invalidArg("LOSS requires money.amount to be 0.00")
		}
	default:
		if !m.IsPositive() {
			return invalidArg("%s requires a positive amount", kind)
		}
	}
	return nil
}

func validateIdentifier(field, value string, maxLen int) error {
	if value == "" {
		return invalidArg("%s is required", field)
	}
	if len(value) > maxLen {
		return invalidArg("%s exceeds %d characters", field, maxLen)
	}
	if strings.TrimSpace(value) != value {
		return invalidArg("%s has leading or trailing whitespace", field)
	}
	for _, r := range value {
		if !unicode.IsPrint(r) {
			return invalidArg("%s contains non-printable characters", field)
		}
	}
	return nil
}

// parseUUID accepts only the canonical 36-char form and returns a UUID; the
// canonical lower-case string is what enters the hash.
func parseUUID(field, value string) (uuid.UUID, error) {
	if len(value) != 36 {
		return uuid.Nil, invalidArg("%s must be a UUID", field)
	}
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, invalidArg("%s must be a non-nil UUID", field)
	}
	return id, nil
}

// computeHash returns the SHA-256 (hex) of the canonical JSON of the
// business fields.
//
// Canonical form:
//   - a JSON object whose keys are sorted lexicographically (encoding/json
//     sorts map keys), with no insignificant whitespace;
//   - fields: externalTransactionId, gameId, kind, money{amount,currency},
//     playerId, providerId, referenceExternalTransactionId (only when
//     present), roundId, walletId;
//   - UUIDs in lower-case canonical form, amount normalised to exactly two
//     decimals ("25" -> "25.00"), currency upper-case ISO code;
//   - the idempotency key, messageId, correlation ids, timestamps and any
//     other transport metadata are excluded.
//
// HTTP and SQS build the same RawExternalRequest, therefore the same
// operation yields the same hash through both entry points.
func (r ExternalRequest) computeHash() string {
	doc := map[string]any{
		"providerId":            r.providerID,
		"externalTransactionId": r.externalTransactionID,
		"playerId":              r.playerID.String(),
		"walletId":              r.walletID.String(),
		"roundId":               r.roundID,
		"gameId":                r.gameID,
		"kind":                  string(r.kind),
		"money": map[string]any{
			"amount":   r.money.Amount(),
			"currency": r.money.Currency().Code(),
		},
	}
	if r.referenceExternalID != "" {
		doc["referenceExternalTransactionId"] = r.referenceExternalID
	}
	b, err := json.Marshal(doc)
	if err != nil {
		// Marshalling a map of strings cannot fail.
		panic("domain: canonical json: " + err.Error())
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ProviderID returns the provider identifier.
func (r ExternalRequest) ProviderID() string { return r.providerID }

// ExternalTransactionID returns the provider-side transaction identifier.
func (r ExternalRequest) ExternalTransactionID() string { return r.externalTransactionID }

// IdempotencyKey returns the key supplied by the client.
func (r ExternalRequest) IdempotencyKey() string { return r.idempotencyKey }

// PlayerID returns the player identifier.
func (r ExternalRequest) PlayerID() uuid.UUID { return r.playerID }

// WalletID returns the wallet identifier.
func (r ExternalRequest) WalletID() uuid.UUID { return r.walletID }

// RoundID returns the round identifier.
func (r ExternalRequest) RoundID() string { return r.roundID }

// GameID returns the game identifier.
func (r ExternalRequest) GameID() string { return r.gameID }

// Kind returns the operation kind.
func (r ExternalRequest) Kind() Kind { return r.kind }

// Money returns the operation amount.
func (r ExternalRequest) Money() money.Money { return r.money }

// ReferenceExternalTransactionID returns the optional reference.
func (r ExternalRequest) ReferenceExternalTransactionID() string { return r.referenceExternalID }

// PayloadHash returns the canonical business hash.
func (r ExternalRequest) PayloadHash() string { return r.payloadHash }
