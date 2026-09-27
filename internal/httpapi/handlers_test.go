package httpapi

import (
	"net/http"
	"testing"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
)

func TestStatusFor(t *testing.T) {
	cases := map[domain.Status]int{
		domain.StatusProcessed:        http.StatusOK,
		domain.StatusRejected:         http.StatusUnprocessableEntity,
		domain.StatusPending:          http.StatusAccepted,
		domain.StatusPendingReference: http.StatusAccepted,
		domain.StatusFailed:           http.StatusInternalServerError,
	}
	for s, want := range cases {
		if got := statusFor(s); got != want {
			t.Errorf("%s -> %d, want %d", s, got, want)
		}
	}
}

func TestValidCorrelationID(t *testing.T) {
	if !validCorrelationID("abc-123") || validCorrelationID("") || validCorrelationID("with space") {
		t.Fatal("correlation id validation")
	}
}
