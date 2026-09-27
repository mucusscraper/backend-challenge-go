package app

import "errors"

// Application errors. Adapters translate them to transport semantics (HTTP
// status codes, SQS delete/retry/DLQ decisions).
var (
	// ErrNotFound: the requested resource does not exist (or is not visible
	// to the caller).
	ErrNotFound = errors.New("not found")
	// ErrWalletNotFound: the wallet of an operation does not exist.
	ErrWalletNotFound = errors.New("wallet not found")
	// ErrWalletAlreadyExists: (playerId, currency) already has a wallet.
	ErrWalletAlreadyExists = errors.New("wallet already exists for player and currency")
	// ErrIdempotencyConflict: the idempotency key was reused with a
	// different business payload.
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	// ErrExternalIDConflict: (providerId, externalTransactionId) was already
	// used with another idempotency key.
	ErrExternalIDConflict = errors.New("external transaction already registered with another idempotency key")
	// ErrMessageConflict: an inbound messageId was redelivered with a
	// different content hash.
	ErrMessageConflict = errors.New("message id reused with a different payload")
	// ErrRetryableConflict: a concurrency race (unique constraint race,
	// deadlock, serialization failure) that is safe to retry immediately.
	ErrRetryableConflict = errors.New("retryable concurrency conflict")
	// ErrConcurrentUpdate: optimistic version check failed.
	ErrConcurrentUpdate = errors.New("concurrent update detected")
	// ErrTransient: temporary infrastructure unavailability (database down,
	// lock timeout, broker unreachable). Callers should retry later.
	ErrTransient = errors.New("temporarily unavailable")
)

// IsRetryable reports whether an error is a concurrency race that the use
// case may retry right away.
func IsRetryable(err error) bool {
	return errors.Is(err, ErrRetryableConflict) || errors.Is(err, ErrConcurrentUpdate)
}

// IsTransient reports whether an error should be retried later (with
// backoff) rather than treated as permanent.
func IsTransient(err error) bool {
	return errors.Is(err, ErrTransient) || IsRetryable(err)
}
