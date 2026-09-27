package app

import (
	"errors"
	"testing"
)

func TestCursorRoundTrip(t *testing.T) {
	for _, seq := range []int64{0, 1, 42, 1 << 40} {
		got, err := DecodeCursor(EncodeCursor(seq))
		if err != nil || got != seq {
			t.Fatalf("seq %d -> %d %v", seq, got, err)
		}
	}
	if got, err := DecodeCursor(""); err != nil || got != 0 {
		t.Fatal("empty cursor must start from the beginning")
	}
	for _, bad := range []string{"%%%", "djI6MTA", "v1:-1", EncodeCursor(-1)} {
		if _, err := DecodeCursor(bad); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("cursor %q: %v", bad, err)
		}
	}
}

func TestErrorClassification(t *testing.T) {
	if !IsRetryable(ErrRetryableConflict) || !IsRetryable(ErrConcurrentUpdate) || IsRetryable(ErrTransient) {
		t.Fatal("retryable classification")
	}
	if !IsTransient(ErrTransient) || IsTransient(ErrIdempotencyConflict) {
		t.Fatal("transient classification")
	}
}
