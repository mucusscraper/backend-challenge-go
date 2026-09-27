package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
)

// OutboxStore gives the relay access to the outbox table. It works outside
// of the business transactions: rows become visible only after the
// transaction that created them committed, which is what guarantees that
// nothing is ever published before its commit.
type OutboxStore struct {
	pool *pgxpool.Pool
}

// NewOutboxStore builds the store.
func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

var _ app.OutboxStore = (*OutboxStore)(nil)

// Claim leases up to limit due events to owner for lease. Concurrent
// publishers never claim the same row at the same time: candidates are
// locked with FOR UPDATE SKIP LOCKED, and a row stays leased until
// locked_until. If a publisher dies, its lease expires and another instance
// claims the row again (abandoned-work recovery).
func (s *OutboxStore) Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]app.OutboxMessage, error) {
	rows, err := s.pool.Query(ctx, `
		UPDATE outbox_events o
		   SET locked_by = $1,
		       locked_until = now() + ($2::bigint * interval '1 millisecond'),
		       attempts = o.attempts + 1
		 WHERE o.id IN (
		       SELECT id FROM outbox_events
		        WHERE published_at IS NULL
		          AND next_attempt_at <= now()
		          AND (locked_until IS NULL OR locked_until < now())
		        ORDER BY occurred_at, id
		        LIMIT $3
		        FOR UPDATE SKIP LOCKED)
		RETURNING o.id, o.aggregate_id, o.event_type, o.payload::text, o.attempts, o.occurred_at`,
		owner, lease.Milliseconds(), limit)
	if err != nil {
		return nil, mapErr(err)
	}
	defer rows.Close()
	var out []app.OutboxMessage
	for rows.Next() {
		var m app.OutboxMessage
		if err := rows.Scan(&m.ID, &m.AggregateID, &m.EventType, &m.Payload, &m.Attempts, &m.OccurredAt); err != nil {
			return nil, mapErr(err)
		}
		out = append(out, m)
	}
	return out, mapErr(rows.Err())
}

// MarkPublished confirms publication. It succeeds whoever owns the lease:
// if two publishers raced after a lease expiry, both published the same
// eventId (consumers deduplicate on it) and the first confirmation wins.
func (s *OutboxStore) MarkPublished(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		   SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		 WHERE id = $1 AND published_at IS NULL`, id)
	return mapErr(err)
}

// MarkFailed releases the lease and schedules the next attempt (backoff).
func (s *OutboxStore) MarkFailed(ctx context.Context, id uuid.UUID, owner, lastError string, next time.Time) error {
	if len(lastError) > 500 {
		lastError = lastError[:500]
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		   SET locked_by = NULL, locked_until = NULL, last_error = $3, next_attempt_at = $4
		 WHERE id = $1 AND locked_by = $2 AND published_at IS NULL`, id, owner, lastError, next)
	return mapErr(err)
}

// Lag returns the age of the oldest unpublished event (0 when none).
func (s *OutboxStore) Lag(ctx context.Context) (time.Duration, error) {
	var seconds float64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(EXTRACT(EPOCH FROM (now() - MIN(occurred_at))), 0)::float8
		  FROM outbox_events WHERE published_at IS NULL`).Scan(&seconds)
	if err != nil {
		return 0, mapErr(err)
	}
	return time.Duration(seconds * float64(time.Second)), nil
}
