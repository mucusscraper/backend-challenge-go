// Package worker contains the background workers: the outbox relay and the
// pending-reference resolver. Both run on a Loop whose lifecycle is driven by
// fx (Start/Stop hooks), with cancellation, bounded shutdown and observable
// termination (Done channel).
package worker

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Loop runs fn periodically until stopped. When fn reports that it did work
// (n > 0) the next iteration starts immediately, so backlogs drain quickly;
// otherwise the loop sleeps for interval (with jitter, so several instances
// do not poll in lockstep).
type Loop struct {
	name     string
	interval time.Duration
	fn       func(ctx context.Context) (int, error)
	log      *slog.Logger

	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewLoop builds a loop; it does nothing until Start.
func NewLoop(name string, interval time.Duration, log *slog.Logger, fn func(ctx context.Context) (int, error)) *Loop {
	return &Loop{name: name, interval: interval, fn: fn, log: log.With("worker", name)}
}

// Start launches the loop goroutine. The ctx passed by fx to OnStart is
// only valid during startup, so the loop owns a separate context.
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

// Stop cancels the loop and waits for the current iteration to finish, or
// for ctx to expire. Work interrupted by cancellation is left in a state
// another instance can resume (leases expire, transactions roll back).
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

// Done is closed when the loop goroutine has exited.
func (l *Loop) Done() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.done
}
