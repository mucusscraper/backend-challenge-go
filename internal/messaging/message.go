package messaging

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
)

// MessageTypeWagerTransactionRequested is the only accepted inbound type.
const MessageTypeWagerTransactionRequested = "WagerTransactionRequested"

// ErrInvalidMessage marks a message that can never be processed (bad JSON,
// wrong type, missing ids). Such messages go straight to the DLQ.
var ErrInvalidMessage = errors.New("invalid message")

// InboundEnvelope is the JSON body of an inbound message.
type InboundEnvelope struct {
	MessageID     string          `json:"messageId"`
	Type          string          `json:"type"`
	OccurredAt    string          `json:"occurredAt"`
	CorrelationID string          `json:"correlationId,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// WagerRequestData is the "data" of a WagerTransactionRequested message.
// Money uses the external contract {"amount":"25.00","currency":"BRL"}.
type WagerRequestData struct {
	ProviderID            string `json:"providerId"`
	ExternalTransactionID string `json:"externalTransactionId"`
	IdempotencyKey        string `json:"idempotencyKey"`
	PlayerID              string `json:"playerId"`
	WalletID              string `json:"walletId"`
	RoundID               string `json:"roundId"`
	GameID                string `json:"gameId"`
	Kind                  string `json:"kind"`
	Money                 struct {
		Amount   string `json:"amount"`
		Currency string `json:"currency"`
	} `json:"money"`
	ReferenceExternalTransactionID string `json:"referenceExternalTransactionId,omitempty"`
}

// ParsedMessage is a structurally valid inbound message.
type ParsedMessage struct {
	Envelope InboundEnvelope
	Raw      domain.RawExternalRequest
}

// ParseInbound decodes and structurally validates a message body. Unknown
// fields are rejected so that typos cannot silently drop business data.
func ParseInbound(body string) (ParsedMessage, error) {
	var env InboundEnvelope
	dec := json.NewDecoder(bytes.NewReader([]byte(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&env); err != nil {
		return ParsedMessage{}, fmt.Errorf("%w: envelope: %v", ErrInvalidMessage, err)
	}
	if env.MessageID == "" || len(env.MessageID) > 128 {
		return ParsedMessage{}, fmt.Errorf("%w: messageId is required (max 128 chars)", ErrInvalidMessage)
	}
	if env.Type != MessageTypeWagerTransactionRequested {
		return ParsedMessage{}, fmt.Errorf("%w: unsupported type %q", ErrInvalidMessage, env.Type)
	}
	var data WagerRequestData
	dec = json.NewDecoder(bytes.NewReader(env.Data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&data); err != nil {
		return ParsedMessage{}, fmt.Errorf("%w: data: %v", ErrInvalidMessage, err)
	}
	return ParsedMessage{
		Envelope: env,
		Raw: domain.RawExternalRequest{
			ProviderID:                     data.ProviderID,
			ExternalTransactionID:          data.ExternalTransactionID,
			IdempotencyKey:                 data.IdempotencyKey,
			PlayerID:                       data.PlayerID,
			WalletID:                       data.WalletID,
			RoundID:                        data.RoundID,
			GameID:                         data.GameID,
			Kind:                           data.Kind,
			Amount:                         data.Money.Amount,
			Currency:                       data.Money.Currency,
			ReferenceExternalTransactionID: data.ReferenceExternalTransactionID,
		},
	}, nil
}

// MessageHash is the inbox hash of a message: SHA-256 over the message
// type, the idempotency key and the canonical business payload hash. A
// redelivery of the same messageId must produce the same value.
func MessageHash(msgType string, req domain.ExternalRequest) string {
	sum := sha256.Sum256([]byte(msgType + "\n" + req.IdempotencyKey() + "\n" + req.PayloadHash()))
	return hex.EncodeToString(sum[:])
}
