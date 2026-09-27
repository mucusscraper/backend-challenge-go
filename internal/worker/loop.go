// Package worker contém os workers de fundo: o outbox relay e o resolvedor de
// referências pendentes. Ambos rodam em um Loop cujo ciclo de vida é dirigido
// pelo fx (hooks Start/Stop), com cancelamento, desligamento limitado e
// terminação observável (canal Done).
package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Loop executa fn periodicamente até ser parado. Quando fn reporta que fez
// trabalho (n > 0), a próxima iteração começa imediatamente, drenando filas
// rapidamente; caso contrário o loop dorme por interval (com jitter, para que
// várias instâncias não façam polling em sincronia).
type Loop struct {
	name     string
	interval time.Duration
	fn       func(ctx context.Context) (int, error)
	log      *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewLoop constrói um loop; não faz nada até Start.
func NewLoop(name string, interval time.Duration, log *slog.Logger, fn func(ctx context.Context) (int, error)) *Loop {
	return &Loop{name: name, interval: interval, fn: fn, log: log.With("worker", name)}
}

// Start lança a goroutine do loop. O ctx passado pelo fx ao OnStart é
// válido apenas durante a inicialização, portanto o loop possui seu próprio contexto.
func (l *Loop) Start(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done != nil {
		return errors.New("worker: already started")
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.cancel = cancel
	l.done = make(chan struct{})
	go l.run(ctx)
	l.log.Info("worker started")
	return nil
}

func (l *Loop) run(ctx context.Context) {
	defer close(l.done)
	for {
		n, err := l.fn(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			l.log.Warn("worker iteration failed", "error", err)
		}
		if n > 0 && err == nil {
			continue
		}
		jitter := time.Duration(rand.Int64N(int64(l.interval)/4 + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(l.interval + jitter):
		}
	}
}

// Stop cancela o loop e aguarda a iteração atual terminar, ou o ctx expirar.
// O trabalho interrompido pelo cancelamento é deixado em um estado que outra
// instância pode retomar (leases expiram, transações fazem rollback).
func (l *Loop) Stop(ctx context.Context) error {
	l.mu.Lock()
	cancel, done := l.cancel, l.done
	l.mu.Unlock()
	if cancel == nil {
		return nil
	}
	cancel()
	select {
	case <-done:
		l.log.Info("worker stopped")
		return nil
	case <-ctx.Done():
		l.log.Warn("worker did not stop before the deadline")
		return ctx.Err()
	}
}

// Done é fechado quando a goroutine do loop terminou.
func (l *Loop) Done() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done
}
