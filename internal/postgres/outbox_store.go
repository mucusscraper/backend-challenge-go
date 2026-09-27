package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
)

// OutboxStore dá ao relay acesso à tabela outbox. Opera fora das transações
// de negócio: as linhas só ficam visíveis após o commit da transação que as
// criou, garantindo que nada seja publicado antes do seu commit.
type OutboxStore struct {
	pool *pgxpool.Pool
}

// NewOutboxStore constrói o store.
func NewOutboxStore(pool *pgxpool.Pool) *OutboxStore { return &OutboxStore{pool: pool} }

var _ app.OutboxStore = (*OutboxStore)(nil)

// Claim aloca até limit eventos devidos ao owner pelo período lease.
// Publishers concorrentes nunca reclamam a mesma linha ao mesmo tempo: os
// candidatos são bloqueados com FOR UPDATE SKIP LOCKED, e uma linha fica
// alocada até locked_until. Se um publisher morrer, seu lease expira e outra
// instância reivindica a linha novamente (recuperação de trabalho abandonado).
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

// MarkPublished confirma a publicação. Tem sucesso para quem quer que detenha
// o lease: se dois publishers correram após a expiração do lease, ambos
// publicaram o mesmo eventId (consumidores deduplicam por ele) e a primeira
// confirmação vence.
func (s *OutboxStore) MarkPublished(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE outbox_events
		   SET published_at = now(), locked_by = NULL, locked_until = NULL, last_error = NULL
		 WHERE id = $1 AND published_at IS NULL`, id)
	return mapErr(err)
}

// MarkFailed libera o lease e agenda a próxima tentativa (backoff).
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

// Lag retorna a idade do evento não publicado mais antigo (0 quando nenhum).
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
