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

// Limites de campo para identificadores externos.
const (
	maxIdentifierLength     = 128
	maxIdempotencyKeyLength = 255
)

// RawExternalRequest é o formato não confiável e neutro em relação ao transporte
// de uma operação de provedor. Os adaptadores HTTP e SQS preenchem esta struct
// e a passam para NewExternalRequest, que garante validação e hashing idênticos.
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

// ExternalRequest é uma operação de provedor validada. Seus campos são
// não exportados para que só possa ser obtida por NewExternalRequest.
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

// NewExternalRequest valida uma requisição bruta e calcula seu hash de payload.
// Todos os problemas de validação são unidos em um único erro encapsulando
// ErrInvalidArgument, de modo que um cliente recebe todos os problemas de uma vez.
//
// Política de zero: LOSS deve ser exatamente 0.00; BET, WIN, REFUND e
// ROLLBACK devem ser > 0. Valores negativos são sempre rejeitados.
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

// validateAmountForKind aplica a política de zero de cada tipo.
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

// parseUUID aceita apenas a forma canônica de 36 caracteres e retorna um UUID;
// a string canônica em minúsculas é o que entra no hash.
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

// computeHash retorna o SHA-256 (hex) do JSON canônico dos campos de negócio.
//
// Forma canônica:
//   - um objeto JSON cujas chaves são ordenadas lexicograficamente (encoding/json
//     ordena as chaves de map), sem espaços em branco insignificantes;
//   - campos: externalTransactionId, gameId, kind, money{amount,currency},
//     playerId, providerId, referenceExternalTransactionId (apenas quando
//     presente), roundId, walletId;
//   - UUIDs em forma canônica minúscula, valor normalizado para exatamente dois
//     decimais ("25" -> "25.00"), código de moeda ISO maiúsculo;
//   - a chave de idempotência, messageId, correlation ids, timestamps e
//     quaisquer outros metadados de transporte são excluídos.
//
// HTTP e SQS constroem o mesmo RawExternalRequest, portanto a mesma
// operação produz o mesmo hash por ambos os pontos de entrada.
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
		// Fazer marshal de um map de strings não pode falhar.
		panic("domain: canonical json: " + err.Error())
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// ProviderID retorna o identificador do provedor.
func (r ExternalRequest) ProviderID() string { return r.providerID }

// ExternalTransactionID retorna o identificador da transação no lado do provedor.
func (r ExternalRequest) ExternalTransactionID() string { return r.externalTransactionID }

// IdempotencyKey retorna a chave fornecida pelo cliente.
func (r ExternalRequest) IdempotencyKey() string { return r.idempotencyKey }

// PlayerID retorna o identificador do jogador.
func (r ExternalRequest) PlayerID() uuid.UUID { return r.playerID }

// WalletID retorna o identificador da carteira.
func (r ExternalRequest) WalletID() uuid.UUID { return r.walletID }

// RoundID retorna o identificador da rodada.
func (r ExternalRequest) RoundID() string { return r.roundID }

// GameID retorna o identificador do jogo.
func (r ExternalRequest) GameID() string { return r.gameID }

// Kind retorna o tipo da operação.
func (r ExternalRequest) Kind() Kind { return r.kind }

// Money retorna o valor da operação.
func (r ExternalRequest) Money() money.Money { return r.money }

// ReferenceExternalTransactionID retorna a referência opcional.
func (r ExternalRequest) ReferenceExternalTransactionID() string { return r.referenceExternalID }

// PayloadHash retorna o hash canônico de negócio.
func (r ExternalRequest) PayloadHash() string { return r.payloadHash }
