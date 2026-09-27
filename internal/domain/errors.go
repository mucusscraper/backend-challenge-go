// Package domain contains the financial core of the service: the Wallet
// aggregate, the append-only ledger, wager transactions and their state
// machine, business rules for the five external operation kinds and the
// integration events they produce.
//
// The package is deliberately independent from Fx, HTTP, SQS and any
// persistence library. It only depends on the standard library, the money
// value object and github.com/google/uuid for identifiers.
package domain

import (
	"errors"
	"fmt"
)

// Sentinel errors used across the domain. They are classifiable with
// errors.Is; business rejections use *RejectionError (errors.As).
var (
	// ErrInvalidArgument marks invalid input to a constructor or method.
	ErrInvalidArgument = errors.New("domain: invalid argument")
	// ErrInvalidTransition marks a forbidden state-machine transition, for
	// example any transition out of a terminal state.
	ErrInvalidTransition = errors.New("domain: invalid state transition")
	// ErrInvariantViolation marks an internal inconsistency (e.g. a ledger
	// entry whose arithmetic does not add up). It indicates a bug or corrupt
	// data, never a user error.
	ErrInvariantViolation = errors.New("domain: invariant violation")
)

// invalidArg wraps ErrInvalidArgument with a formatted detail.
func invalidArg(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidArgument, fmt.Sprintf(format, args...))
}

// FailureCode is a stable, documented code attached to REJECTED and FAILED
// transactions. Codes never change meaning once published.
type FailureCode string

// Business rejection codes (terminal, persisted as REJECTED).
const (
	// FailureInsufficientFunds: a BET would leave the wallet negative.
	FailureInsufficientFunds FailureCode = "INSUFFICIENT_FUNDS"
	// FailureReversalInsufficientFunds: a ROLLBACK that has to debit (it
	// reverses a WIN or REFUND) would leave the wallet negative. Kept distinct
	// from INSUFFICIENT_FUNDS so it can be audited separately.
	FailureReversalInsufficientFunds FailureCode = "REVERSAL_INSUFFICIENT_FUNDS"
	// FailureCurrencyMismatch: the operation currency differs from the wallet.
	FailureCurrencyMismatch FailureCode = "CURRENCY_MISMATCH"
	// FailurePlayerWalletMismatch: the wallet does not belong to the player.
	FailurePlayerWalletMismatch FailureCode = "PLAYER_WALLET_MISMATCH"
	// FailureReferenceNotFound: the referenced transaction never arrived
	// before the retry budget / TTL was exhausted.
	FailureReferenceNotFound FailureCode = "REFERENCE_NOT_FOUND"
	// FailureReferenceNotProcessed: the reference exists but ended REJECTED
	// or FAILED, so there is nothing to reverse.
	FailureReferenceNotProcessed FailureCode = "REFERENCE_NOT_PROCESSED"
	// FailureReferenceMismatch: provider, player, wallet, currency or round
	// of the reference differ from the operation.
	FailureReferenceMismatch FailureCode = "REFERENCE_MISMATCH"
	// FailureReferenceKindNotAllowed: the reference kind cannot be targeted
	// by this operation (e.g. REFUND of a WIN, ROLLBACK of a ROLLBACK).
	FailureReferenceKindNotAllowed FailureCode = "REFERENCE_KIND_NOT_ALLOWED"
	// FailureReversalAmountMismatch: partial reversals are not supported.
	FailureReversalAmountMismatch FailureCode = "REVERSAL_AMOUNT_MISMATCH"
	// FailureReferenceAlreadyReversed: the reference already has a
	// successful REFUND or ROLLBACK.
	FailureReferenceAlreadyReversed FailureCode = "REFERENCE_ALREADY_REVERSED"
)

// Infrastructure failure codes (terminal, persisted as FAILED for audit).
const (
	// FailureProcessingFailed: a permanent, non-business error happened while
	// resuming a persisted operation (e.g. corrupt data). Never retried.
	FailureProcessingFailed FailureCode = "PROCESSING_FAILED"
)

// RejectionError is returned by domain operations that refuse an operation
// for a business reason. It carries the stable FailureCode.
type RejectionError struct {
	Code   FailureCode
	Detail string
}

// Error implements error.
func (e *RejectionError) Error() string {
	if e.Detail == "" {
		return "rejected: " + string(e.Code)
	}
	return fmt.Sprintf("rejected: %s: %s", e.Code, e.Detail)
}

// Is makes errors.Is(err, &RejectionError{Code: X}) match on the code.
func (e *RejectionError) Is(target error) bool {
	t, ok := target.(*RejectionError)
	if !ok {
		return false
	}
	return t.Code == "" || t.Code == e.Code
}

// reject builds a *RejectionError.
func reject(code FailureCode, format string, args ...any) *RejectionError {
	return &RejectionError{Code: code, Detail: fmt.Sprintf(format, args...)}
}

// ErrInsufficientFunds can be used with errors.Is to detect a wallet debit
// refused for lack of balance.
var ErrInsufficientFunds = &RejectionError{Code: FailureInsufficientFunds}
