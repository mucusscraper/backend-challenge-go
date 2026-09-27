// Package domain contém o núcleo financeiro do serviço: o agregado Wallet,
// o ledger append-only, as transações de aposta e sua máquina de estados,
// as regras de negócio para os cinco tipos de operação externa e os eventos
// de integração que eles produzem.
//
// O pacote é deliberadamente independente de Fx, HTTP, SQS e qualquer
// biblioteca de persistência. Depende apenas da biblioteca padrão, do objeto
// de valor money e de github.com/google/uuid para identificadores.
package domain

import (
	"errors"
	"fmt"
)

// Erros sentinela usados em todo o domínio. São classificáveis com
// errors.Is; rejeições de negócio usam *RejectionError (errors.As).
var (
	// ErrInvalidArgument marca entrada inválida para um construtor ou método.
	ErrInvalidArgument = errors.New("domain: invalid argument")
	// ErrInvalidTransition marca uma transição de máquina de estados proibida, por
	// exemplo qualquer transição a partir de um estado terminal.
	ErrInvalidTransition = errors.New("domain: invalid state transition")
	// ErrInvariantViolation marca uma inconsistência interna (ex.: uma entrada
	// do ledger cuja aritmética não fecha). Indica um bug ou dados corrompidos,
	// nunca um erro do usuário.
	ErrInvariantViolation = errors.New("domain: invariant violation")
)

// invalidArg encapsula ErrInvalidArgument com um detalhe formatado.
func invalidArg(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// FailureCode é um código estável e documentado anexado a transações REJECTED e
// FAILED. Os códigos nunca mudam de significado após publicados.
type FailureCode string

// Códigos de rejeição de negócio (terminais, persistidos como REJECTED).
const (
	// FailureInsufficientFunds: um BET deixaria a carteira negativa.
	FailureInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
	// FailureReversalInsufficientFunds: um ROLLBACK que precisa debitar (reverte
	// um WIN ou REFUND) deixaria a carteira negativa. Mantido distinto de
	// INSUFFICIENT_FUNDS para poder ser auditado separadamente.
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	// FailureCurrencyMismatch: a moeda da operação difere da carteira.
	FailureCurrencyMismatch FailureCode = "CURRENCY_MISMATCH"
	// FailurePlayerWalletMismatch: a carteira não pertence ao jogador.
	FailurePlayerWalletMismatch FailureCode = "PLAYER_WALLET_MISMATCH"
	// FailureReferenceNotFound: a transação referenciada nunca chegou
	// antes do orçamento de retry / TTL ser esgotado.
	FailureReferenceNotFound FailureCode = "REFERENCE_NOT_FOUND"
	// FailureReferenceNotProcessed: a referência existe mas terminou REJECTED
	// ou FAILED, portanto não há nada a reverter.
	FailureReferenceNotProcessed FailureCode = "REFERENCE_NOT_PROCESSED"
	// FailureReferenceMismatch: provedor, jogador, carteira, moeda ou rodada
	// da referência diferem da operação.
	FailureReferenceMismatch FailureCode = "REFERENCE_MISMATCH"
	// FailureReferenceKindNotAllowed: o tipo da referência não pode ser alvo
	// desta operação (ex.: REFUND de um WIN, ROLLBACK de um ROLLBACK).
	FailureReferenceKindNotAllowed FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	// FailureReversalAmountMismatch: reversões parciais não são suportadas.
	FailureReversalAmountMismatch FailureCode = "REVERSAL_AMOUNT_MISMATCH"
	// FailureReferenceAlreadyReversed: a referência já tem um REFUND ou
	// ROLLBACK bem-sucedido.
	FailureReferenceAlreadyReversed FailureCode = "REFERENCE_ALREADY_REVERSED"
)

// Códigos de falha de infraestrutura (terminais, persistidos como FAILED para auditoria).
const (
	// FailureProcessingFailed: um erro permanente, não de negócio, ocorreu ao
	// retomar uma operação persistida (ex.: dados corrompidos). Nunca retentado.
	FailureProcessingFailed FailureCode = "PROCESSING_FAILED"
)

// RejectionError é retornado por operações de domínio que recusam uma operação
// por motivo de negócio. Carrega o FailureCode estável.
type RejectionError struct {
	Code   FailureCode
	Detail string
}

// Error implementa error.
func (e *RejectionError) Error() string {
	if e.Detail == "" {
		return "rejected: " + string(e.Code)
	}
	return fmt.Sprintf("rejected: %s: %s", e.Code, e.Detail)
}

// Is faz errors.Is(err, &RejectionError{Code: X}) corresponder pelo código.
func (e *RejectionError) Is(target error) bool {
	t, ok := target.(*RejectionError)
	if !ok {
		return false
	}
	return t.Code == "" || t.Code == e.Code
}

// reject constrói um *RejectionError.
func reject(code FailureCode, format string, args ...any) *RejectionError {
	return &RejectionError{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// ErrInsufficientFunds pode ser usado com errors.Is para detectar um débito
// de carteira recusado por falta de saldo.
var ErrInsufficientFunds = &RejectionError{Code: FailureInsufficientFunds}
