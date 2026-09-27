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

// MessageTypeWagerTransactionRequested é o único tipo de entrada aceito.
const MessageTypeWagerTransactionRequested = "WagerTransactionRequested"

// ErrInvalidMessage marca uma mensagem que nunca pode ser processada (JSON inválido,
// tipo errado, ids ausentes). Essas mensagens vão direto para a DLQ.
var ErrInvalidMessage = errors.New("invalid message")

// InboundEnvelope é o corpo JSON de uma mensagem de entrada.
type InboundEnvelope struct {
	MessageID     string          `json:"messageId"`
	Type          string          `json:"type"`
	OccurredAt    string          `json:"occurredAt"`
	CorrelationID string          `json:"correlationId,omitempty"`
	Data          json.RawMessage `json:"data"`
}

// WagerRequestData é o "data" de uma mensagem WagerTransactionRequested.
// Money usa o contrato externo {"amount":"25.00","currency":"BRL"}.
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

// ParsedMessage é uma mensagem de entrada estruturalmente válida.
type ParsedMessage struct {
	Envelope InboundEnvelope
	Raw      domain.RawExternalRequest
}

// ParseInbound decodifica e valida estruturalmente o corpo de uma mensagem.
// Campos desconhecidos são rejeitados para que erros de digitação não descartam
// silenciosamente dados de negócio.
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

// MessageHash é o hash de inbox de uma mensagem: SHA-256 sobre o tipo de
// mensagem, a chave de idempotência e o hash canônico de payload de negócio.
// Uma reentrega do mesmo messageId deve produzir o mesmo valor.
func MessageHash(msgType string, req domain.ExternalRequest) string {
	sum := sha256.Sum256([]byte(msgType + "\n" + req.IdempotencyKey() + "\n" + req.PayloadHash()))
	return hex.EncodeToString(sum[:])
}
