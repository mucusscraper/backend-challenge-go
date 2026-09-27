package app

import "errors"

// Erros da aplicação. Adaptadores os traduzem para semântica de transporte
// (códigos HTTP, decisões de delete/retry/DLQ do SQS).
var (
	// ErrNotFound: o recurso solicitado não existe (ou não é visível ao chamador).
	ErrNotFound = errors.New("not found")
	// ErrWalletNotFound: a carteira de uma operação não existe.
	ErrWalletNotFound = errors.New("wallet not found")
	// ErrWalletAlreadyExists: (playerId, currency) já possui uma carteira.
	ErrWalletAlreadyExists = errors.New("wallet already exists for player and currency")
	// ErrIdempotencyConflict: a chave de idempotência foi reutilizada com um
	// payload de negócio diferente.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrExternalIDConflict: (providerId, externalTransactionId) já foi usado
	// com outra chave de idempotência.
	ErrExternalIDConflict = errors.New("external transaction already registered with another idempotency key")
	// ErrMessageConflict: um messageId de entrada foi reenviado com um hash
	// de conteúdo diferente.
	ErrMessageConflict = errors.New("message id reused with a different payload")
	// ErrRetryableConflict: uma corrida de concorrência (corrida de constraint
	// única, deadlock, falha de serialização) que é seguro retentear imediatamente.
	ErrRetryableConflict = errors.New("retryable concurrency conflict")
	// ErrConcurrentUpdate: verificação de versão otimista falhou.
	ErrConcurrentUpdate = errors.New("concurrent update detected")
	// ErrTransient: indisponibilidade temporária de infraestrutura (banco caiu,
	// timeout de lock, broker inacessível). Os chamadores devem tentar novamente mais tarde.
	ErrTransient = errors.New("temporarily unavailable")
)

// IsRetryable informa se um erro é uma corrida de concorrência que o caso
// de uso pode retentear imediatamente.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRetryableConflict) || errors.Is(err, ErrConcurrentUpdate)
}

// IsTransient informa se um erro deve ser retentado mais tarde (com
// backoff) em vez de ser tratado como permanente.
func IsTransient(err error) bool {
	return errors.Is(err, ErrTransient) || IsRetryable(err)
}
