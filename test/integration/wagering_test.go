//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/app"
	"github.com/mucusscraper/backend-challenge-go/internal/domain"
	tu "github.com/mucusscraper/backend-challenge-go/test/testutil"
)

func submit(s *tu.Services, req domain.ExternalRequest) (app.SubmitResult, error) {
	return s.Wagering.Submit(context.Background(), app.SubmitCommand{Request: req, CorrelationID: "it", Source: "test"})
}

// instances simula N instâncias independentes do serviço: cada uma tem seu
// próprio pool de conexões e seus próprios objetos de serviço (sem coordenação
// por memória compartilhada). Execuções entre processos são cobertas pela suíte e2e.
func instances(t *testing.T, n int, policy domain.ReferencePolicy) []*tu.Services {
	out := make([]*tu.Services, n)
	for i := range out {
		out[i] = tu.NewServices(t, policy)
	}
	return out
}

// TestSameBetFiftyTimesInParallel: 50 entregas concorrentes da mesma
// operação produzem exatamente um débito; 49 são replays idempotentes.
func TestSameBetFiftyTimesInParallel(t *testing.T) {
	inst := instances(t, 3, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, inst[0], "100.00")
	req := tu.Req(t, "provider-a", w, domain.KindBet, "25.00", "bet-"+uuid.NewString(), "")

	var wg sync.WaitGroup
	var replays, fresh atomic.Int32
	ids := sync.Map{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := submit(inst[i%3], req)
			if err != nil {
				t.Errorf("submit %d: %v", i, err)
				return
			}
			ids.Store(res.Transaction.ID(), true)
			if res.Replay {
				replays.Add(1)
			} else {
				fresh.Add(1)
			}
			if rb, _ := res.Transaction.ResultBalance(); rb.Amount() != "75.00" {
				t.Errorf("result balance %s", rb)
			}
		}(i)
	}
	wg.Wait()
	if fresh.Load() != 1 || replays.Load() != 49 {
		t.Fatalf("fresh=%d replays=%d", fresh.Load(), replays.Load())
	}
	n := 0
	ids.Range(func(any, any) bool { n++; return true })
	if n != 1 {
		t.Fatalf("%d distinct transactions", n)
	}
	entries, net, debits := tu.LedgerStats(t, inst[0].Pool, w.ID())
	if entries != 2 || debits != 1 || net != 7500 {
		t.Fatalf("ledger entries=%d debits=%d net=%d", entries, debits, net)
	}
	tu.AssertConsistent(t, inst[0].Pool, w.ID())
}

// TestTwoConcurrentBetsOnLimitedBalance é a corrida obrigatória: 100.00 BRL,
// duas apostas diferentes de 80.00 ao mesmo tempo -> uma PROCESSED, uma REJECTED
// (INSUFFICIENT_FUNDS), saldo final 20.00, um débito; reenvio não altera nada.
func TestTwoConcurrentBetsOnLimitedBalance(t *testing.T) {
	for round := 0; round < 10; round++ {
		inst := instances(t, 3, domain.DefaultReferencePolicy)
		w := tu.OpenWallet(t, inst[0], "100.00")
		reqs := []domain.ExternalRequest{
			tu.Req(t, "provider-a", w, domain.KindBet, "80.00", "race-a-"+uuid.NewString(), ""),
			tu.Req(t, "provider-a", w, domain.KindBet, "80.00", "race-b-"+uuid.NewString(), ""),
		}
		results := make([]app.SubmitResult, 2)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range reqs {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				r, err := submit(inst[i], reqs[i])
				if err != nil {
					t.Errorf("submit: %v", err)
				}
				results[i] = r
			}(i)
		}
		close(start)
		wg.Wait()
		assertRaceOutcome(t, inst[0], w.ID(), results)

		// Reenvio de ambas (da terceira instância): nada muda.
		for i := range reqs {
			r, err := submit(inst[2], reqs[i])
			if err != nil || !r.Replay || r.Transaction.Status() != results[i].Transaction.Status() {
				t.Fatalf("resend %d: %v replay=%v", i, err, r.Replay)
			}
		}
		assertRaceOutcome(t, inst[0], w.ID(), results)
	}
}

func assertRaceOutcome(t *testing.T, s *tu.Services, walletID uuid.UUID, results []app.SubmitResult) {
	t.Helper()
	statuses := map[domain.Status]int{}
	for _, r := range results {
		statuses[r.Transaction.Status()]++
		if r.Transaction.Status() == domain.StatusRejected && r.Transaction.FailureCode() != domain.FailureInsufficientFunds {
			t.Fatalf("rejection code %s", r.Transaction.FailureCode())
		}
	}
	if statuses[domain.StatusProcessed] != 1 || statuses[domain.StatusRejected] != 1 {
		t.Fatalf("statuses %v", statuses)
	}
	bal, _ := tu.StoredBalance(t, s.Pool, walletID)
	_, _, debits := tu.LedgerStats(t, s.Pool, walletID)
	if bal != 2000 || debits != 1 {
		t.Fatalf("balance=%d debits=%d", bal, debits)
	}
	tu.AssertConsistent(t, s.Pool, walletID)
}

// TestIndependentWalletsProgressInParallel: enquanto uma carteira está bloqueada por uma
// transação longa, operações em outras carteiras são concluídas (sem bloqueio global).
func TestIndependentWalletsProgressInParallel(t *testing.T) {
	inst := instances(t, 3, domain.DefaultReferencePolicy)
	blocked := tu.OpenWallet(t, inst[0], "100.00")

	// Mantém o lock de "blocked" em uma transação aberta.
	owner := tu.OwnerPool(t)
	ctx := context.Background()
	lockTx, err := owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, blocked.ID()); err != nil {
		t.Fatal(err)
	}
	defer lockTx.Rollback(ctx)

	wallets := make([]*domain.Wallet, 30)
	for i := range wallets {
		wallets[i] = tu.OpenWallet(t, inst[i%3], "50.00")
	}
	started := time.Now()
	var wg sync.WaitGroup
	for i, w := range wallets {
		wg.Add(1)
		go func(i int, w *domain.Wallet) {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				req := tu.Req(t, "provider-a", w, domain.KindBet, "10.00", fmt.Sprintf("par-%s-%d", w.ID(), j), "")
				if r, err := submit(inst[i%3], req); err != nil || r.Transaction.Status() != domain.StatusProcessed {
					t.Errorf("wallet %d bet %d: %v", i, j, err)
				}
			}
		}(i, w)
	}
	wg.Wait()
	if elapsed := time.Since(started); elapsed > 4*time.Second {
		t.Fatalf("independent wallets were slowed down by an unrelated lock: %s", elapsed)
	}
	for _, w := range wallets {
		bal, ver := tu.StoredBalance(t, inst[0].Pool, w.ID())
		if bal != 2000 || ver != 4 {
			t.Fatalf("wallet %s balance=%d version=%d", w.ID(), bal, ver)
		}
		tu.AssertConsistent(t, inst[0].Pool, w.ID())
	}
}

// TestManyConcurrentMixedOperationsOnOneWallet: alta contenção em uma
// carteira nunca produz saldo negativo ou atualização perdida.
func TestManyConcurrentMixedOperationsOnOneWallet(t *testing.T) {
	inst := instances(t, 3, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, inst[0], "100.00")
	var wg sync.WaitGroup
	var processedBets, wins atomic.Int64
	for i := 0; i < 60; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			kind, amount := domain.KindBet, "7.00"
			if i%3 == 0 {
				kind, amount = domain.KindWin, "5.00"
			}
			r, err := submit(inst[i%3], tu.Req(t, "provider-a", w, kind, amount, fmt.Sprintf("mix-%s-%d", w.ID(), i), ""))
			if err != nil {
				t.Errorf("submit: %v", err)
				return
			}
			if r.Transaction.Status() == domain.StatusProcessed {
				if kind == domain.KindBet {
					processedBets.Add(1)
				} else {
					wins.Add(1)
				}
			}
		}(i)
	}
	wg.Wait()
	bal, ver := tu.StoredBalance(t, inst[0].Pool, w.ID())
	want := int64(10000) + wins.Load()*500 - processedBets.Load()*700
	if bal != want || bal < 0 {
		t.Fatalf("balance %d want %d", bal, want)
	}
	if ver != 1+wins.Load()+processedBets.Load() {
		t.Fatalf("version %d does not match the number of movements", ver)
	}
	tu.AssertConsistent(t, inst[0].Pool, w.ID())
}

// TestIdempotencyRules cobre replay com o resultado original, reutilização de chave
// com payload diferente, e reutilização de id externo com outra chave.
func TestIdempotencyRules(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, s, "100.00")
	ext := "idem-" + uuid.NewString()
	req := tu.Req(t, "provider-a", w, domain.KindBet, "10.00", ext, "")
	first := tu.Submit(t, s, req)
	if first.Replay {
		t.Fatal("first submission flagged as replay")
	}
	// Outros movimentos ocorrem depois...
	tu.Submit(t, s, tu.Req(t, "provider-a", w, domain.KindWin, "50.00", "idem-win-"+uuid.NewString(), ""))
	// ...mas o replay ainda retorna o saldo observado originalmente.
	replay := tu.Submit(t, s, req)
	rb, _ := replay.Transaction.ResultBalance()
	if !replay.Replay || rb.Amount() != "90.00" || replay.Transaction.ID() != first.Transaction.ID() {
		t.Fatalf("replay: %v %s", replay.Replay, rb)
	}

	// Mesma chave, payload diferente -> conflito.
	raw := tu.RawReq("provider-a", w, domain.KindBet, "11.00", ext, "")
	changed, _ := domain.NewExternalRequest(raw)
	if _, err := submit(s, changed); !errors.Is(err, app.ErrIdempotencyConflict) {
		t.Fatalf("expected idempotency conflict, got %v", err)
	}
	// Mesmo id externo, outra chave -> conflito (sem reaplicação).
	raw = tu.RawReq("provider-a", w, domain.KindBet, "10.00", ext, "")
	raw.IdempotencyKey = "another-key-" + uuid.NewString()
	otherKey, _ := domain.NewExternalRequest(raw)
	if _, err := submit(s, otherKey); !errors.Is(err, app.ErrExternalIDConflict) {
		t.Fatalf("expected external id conflict, got %v", err)
	}
	_, _, debits := tu.LedgerStats(t, s.Pool, w.ID())
	if debits != 1 {
		t.Fatalf("debits %d", debits)
	}

	// Idempotência sobrevive a um "restart": serviço/pool totalmente novo.
	restarted := tu.NewServices(t, domain.DefaultReferencePolicy)
	again := tu.Submit(t, restarted, req)
	if !again.Replay || again.Transaction.ID() != first.Transaction.ID() {
		t.Fatal("replay after restart failed")
	}
	tu.AssertConsistent(t, s.Pool, w.ID())
}

// TestReversalBeforeReferenceIsResolvedLater: um REFUND chega antes do seu
// BET, aguarda em PENDING_REFERENCE, e o worker o liquida depois.
func TestReversalBeforeReferenceIsResolvedLater(t *testing.T) {
	policy := domain.ReferencePolicy{MaxAttempts: 20, TTL: time.Minute, BaseBackoff: 50 * time.Millisecond, MaxBackoff: 200 * time.Millisecond}
	s := tu.NewServices(t, policy)
	w := tu.OpenWallet(t, s, "100.00")
	betExt := "late-bet-" + uuid.NewString()
	refund := tu.Submit(t, s, tu.Req(t, "provider-a", w, domain.KindRefund, "30.00", "refund-"+uuid.NewString(), betExt))
	if refund.Transaction.Status() != domain.StatusPendingReference {
		t.Fatalf("refund status %s", refund.Transaction.Status())
	}
	bet := tu.Submit(t, s, tu.Req(t, "provider-a", w, domain.KindBet, "30.00", betExt, ""))
	if bet.Transaction.Status() != domain.StatusProcessed {
		t.Fatal("bet not processed")
	}
	// Uma instância diferente (pool novo) retoma o reembolso pendente.
	worker := tu.NewServices(t, policy)
	final := waitFinal(t, worker, refund.Transaction.ID(), 10*time.Second)
	if final.Status() != domain.StatusProcessed || final.ReferenceTransactionID() != bet.Transaction.ID() {
		t.Fatalf("refund final %s %s", final.Status(), final.FailureCode())
	}
	bal, _ := tu.StoredBalance(t, s.Pool, w.ID())
	if bal != 10000 {
		t.Fatalf("balance %d", bal)
	}
	// Um segundo reembolso da mesma aposta é rejeitado.
	dup := tu.Submit(t, s, tu.Req(t, "provider-a", w, domain.KindRefund, "30.00", "refund2-"+uuid.NewString(), betExt))
	if dup.Transaction.FailureCode() != domain.FailureReferenceAlreadyReversed {
		t.Fatalf("second refund: %s", dup.Transaction.FailureCode())
	}
	tu.AssertConsistent(t, s.Pool, w.ID())
}

// TestPendingReferenceExpires: a referência nunca chega -> REJECTED
// com REFERENCE_NOT_FOUND após o orçamento de retentativas.
func TestPendingReferenceExpires(t *testing.T) {
	policy := domain.ReferencePolicy{MaxAttempts: 3, TTL: time.Minute, BaseBackoff: 50 * time.Millisecond, MaxBackoff: 100 * time.Millisecond}
	s := tu.NewServices(t, policy)
	w := tu.OpenWallet(t, s, "100.00")
	rb := tu.Submit(t, s, tu.Req(t, "provider-a", w, domain.KindRollback, "30.00", "rb-"+uuid.NewString(), "never-"+uuid.NewString()))
	final := waitFinal(t, s, rb.Transaction.ID(), 10*time.Second)
	if final.Status() != domain.StatusRejected || final.FailureCode() != domain.FailureReferenceNotFound {
		t.Fatalf("final %s %s", final.Status(), final.FailureCode())
	}
	var events int
	_ = s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE aggregate_id = $1 AND event_type IN ('WagerTransactionPendingReference','WagerTransactionRejected')`,
		rb.Transaction.ID().String()).Scan(&events)
	if events != 2 {
		t.Fatalf("expected pending + rejected events, got %d", events)
	}
	tu.AssertConsistent(t, s.Pool, w.ID())
}

// TestConcurrentReversalsOfSameBet: REFUND e ROLLBACK da mesma aposta
// competem; exatamente um tem sucesso (o outro é REFERENCE_ALREADY_REVERSED).
func TestConcurrentReversalsOfSameBet(t *testing.T) {
	inst := instances(t, 2, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, inst[0], "100.00")
	betExt := "rev-bet-" + uuid.NewString()
	tu.Submit(t, inst[0], tu.Req(t, "provider-a", w, domain.KindBet, "40.00", betExt, ""))
	reqs := []domain.ExternalRequest{
		tu.Req(t, "provider-a", w, domain.KindRefund, "40.00", "rev-refund-"+uuid.NewString(), betExt),
		tu.Req(t, "provider-a", w, domain.KindRollback, "40.00", "rev-rollback-"+uuid.NewString(), betExt),
	}
	var wg sync.WaitGroup
	statuses := make([]domain.Status, 2)
	for i := range reqs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r, err := submit(inst[i], reqs[i])
			if err != nil {
				t.Error(err)
				return
			}
			statuses[i] = r.Transaction.Status()
		}(i)
	}
	wg.Wait()
	if (statuses[0] == domain.StatusProcessed) == (statuses[1] == domain.StatusProcessed) {
		t.Fatalf("exactly one reversal must succeed: %v", statuses)
	}
	bal, _ := tu.StoredBalance(t, inst[0].Pool, w.ID())
	if bal != 10000 {
		t.Fatalf("balance %d", bal)
	}
	tu.AssertConsistent(t, inst[0].Pool, w.ID())
}

// TestResumeCrashedPending: uma linha PENDING confirmada por uma instância que
// morreu antes de liquidar (simulada por inserção direta) é retomada por
// outra instância.
func TestResumeCrashedPending(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, s, "100.00")
	req := tu.Req(t, "provider-a", w, domain.KindBet, "15.00", "crashed-"+uuid.NewString(), "")
	txID := uuid.Must(uuid.NewV7())
	_, err := tu.OwnerPool(t).Exec(context.Background(), `
		INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, amount_minor, currency,
		  provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
		  correlation_id, created_at, updated_at)
		VALUES ($1,'EXTERNAL','BET','PENDING',$2,$3,1500,'BRL','provider-a',$4,$5,$6,'round-1','fortune-chimp','crash',now(),now())`,
		txID, w.ID(), w.PlayerID(), req.ExternalTransactionID(), req.IdempotencyKey(), req.PayloadHash())
	if err != nil {
		t.Fatal(err)
	}
	// Enquanto pendente, um replay reporta PENDING e não aplica duas vezes.
	r := tu.Submit(t, s, req)
	if !r.Replay || r.Transaction.Status() != domain.StatusPending {
		t.Fatalf("replay of pending: %v %s", r.Replay, r.Transaction.Status())
	}
	other := tu.NewServices(t, domain.DefaultReferencePolicy)
	final := waitFinal(t, other, txID, 10*time.Second)
	if final.Status() != domain.StatusProcessed {
		t.Fatalf("resumed status %s", final.Status())
	}
	bal, _ := tu.StoredBalance(t, s.Pool, w.ID())
	if bal != 8500 {
		t.Fatalf("balance %d", bal)
	}
	tu.AssertConsistent(t, s.Pool, w.ID())
}

// TestReconciliation verifica o caso de uso do endpoint em uma carteira movimentada.
func TestReconciliation(t *testing.T) {
	s := tu.NewServices(t, domain.DefaultReferencePolicy)
	w := tu.OpenWallet(t, s, "1000.00")
	tu.Submit(t, s, tu.Req(t, "provider-a", w, domain.KindBet, "25.00", "rec-"+uuid.NewString(), ""))
	rec, err := s.Wallets.Reconcile(context.Background(), w.ID())
	if err != nil {
		t.Fatal(err)
	}
	if !rec.Consistent || rec.StoredBalance.Amount() != "975.00" || rec.CalculatedBalance.Amount() != "975.00" ||
		rec.Difference.Amount() != "0.00" || rec.CheckedEntries != 2 {
		t.Fatalf("reconciliation %+v", rec)
	}
}

// waitFinal aciona ResumeDue até a transação ser terminal.
func waitFinal(t *testing.T, s *tu.Services, id uuid.UUID, timeout time.Duration) *domain.WagerTransaction {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.Wagering.ResumeDue(context.Background(), 100); err != nil {
			t.Fatal(err)
		}
		tx, err := s.Wagering.GetTransaction(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if tx.Status().IsTerminal() {
			return tx
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("transaction %s not terminal after %s", id, timeout)
	return nil
}
