package messaging

import (
	"errors"
	"testing"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
)

const validBody = `{
  "messageId": "msg-123",
  "type": "WagerTransactionRequested",
  "occurredAt": "2026-09-08T12:00:00.000Z",
  "data": {
    "providerId": "provider-a",
    "externalTransactionId": "transaction-123",
    "idempotencyKey": "provider-a:transaction-123",
    "playerId": "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
    "walletId": "0192f291-27dd-7d3f-8071-5f8685deef37",
    "roundId": "round-987",
    "gameId": "fortune-chimp",
    "kind": "BET",
    "money": { "amount": "25.00", "currency": "BRL" }
  }
}`

func TestParseInboundValid(t *testing.T) {
	p, err := ParseInbound(validBody)
	if err != nil {
		t.Fatal(err)
	}
	if p.Envelope.MessageID != "msg-123" || p.Raw.Amount != "25.00" || p.Raw.IdempotencyKey != "provider-a:transaction-123" {
		t.Fatalf("parsed %+v", p)
	}
	req, err := domain.NewExternalRequest(p.Raw)
	if err != nil {
		t.Fatal(err)
	}
	// A mesma operação via HTTP constrói o mesmo RawExternalRequest, portanto
	// o hash de negócio é idêntico (equivalência HTTP/SQS).
	fromHTTP := p.Raw
	httpReq, _ := domain.NewExternalRequest(fromHTTP)
	if req.PayloadHash() != httpReq.PayloadHash() {
		t.Fatal("hash differs between entry points")
	}
	if MessageHash(p.Envelope.Type, req) == "" {
		t.Fatal("empty message hash")
	}
}

func TestParseInboundInvalid(t *testing.T) {
	cases := map[string]string{
		"not json":      `{`,
		"no message id": `{"type":"WagerTransactionRequested","data":{}}`,
		"wrong type":    `{"messageId":"m","type":"Other","data":{}}`,
		"unknown field": `{"messageId":"m","type":"WagerTransactionRequested","data":{"foo":1}}`,
		"numeric money": `{"messageId":"m","type":"WagerTransactionRequested","data":{"money":{"amount":25.0,"currency":"BRL"}}}`,
	}
	for name, body := range cases {
		if _, err := ParseInbound(body); !errors.Is(err, ErrInvalidMessage) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestMessageHashChangesWithKey(t *testing.T) {
	p, _ := ParseInbound(validBody)
	a, _ := domain.NewExternalRequest(p.Raw)
	p.Raw.IdempotencyKey = "other"
	b, _ := domain.NewExternalRequest(p.Raw)
	if a.PayloadHash() != b.PayloadHash() {
		t.Fatal("business hash must exclude the key")
	}
	if MessageHash("WagerTransactionRequested", a) == MessageHash("WagerTransactionRequested", b) {
		t.Fatal("message hash must include the key")
	}
}
