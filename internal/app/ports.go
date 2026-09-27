// Package app contém os casos de uso do serviço (abrir carteira, enviar uma
// transação de aposta, retomar transações pendentes, consultas, reconciliação).
//
// Handlers HTTP e o consumidor SQS chamam os mesmos casos de uso, de modo que
// ambos os pontos de entrada compartilham validação, idempotência e garantias
// financeiras.
//
// O pacote depende do domínio e das ports declaradas neste arquivo;
// o adaptador PostgreSQL as implementa. O limite da transação SQL é
// explícito: UnitOfWork.Run abre uma transação do banco de dados e cada
// repositório obtido do argumento Tx participa dela, de modo que as gravações
// de carteira, transação, ledger, inbox e outbox fazem commit (ou rollback)
// atomicamente.
package app

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/mucusscraper/backend-challenge-go/internal/domain"
)

// UnitOfWork delimita transações SQL.
type UnitOfWork interface {
	// Run executa fn dentro de uma transação READ COMMITTED com um timeout de
	// lock limitado. O erro de fn faz rollback; nil faz commit. Repositórios
	// de tx não devem ser usados após fn retornar.
	Run(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
	// Snapshot executa fn dentro de uma transação REPEATABLE READ, READ ONLY:
	// cada consulta vê o mesmo snapshot consistente (usado pela reconciliação).
	Snapshot(ctx context.Context, fn func(ctx context.Context, tx Tx) error) error
}

// Tx dá acesso aos repositórios vinculados a uma transação SQL.
type Tx interface {
	Wallets() WalletRepository
	Transactions() TransactionRepository
	Ledger() LedgerRepository
	Outbox() OutboxRepository
	Inbox() InboxRepository
}

// WalletRepository persiste o agregado Wallet.
type WalletRepository interface {
	// Insert armazena uma nova carteira; ErrWalletAlreadyExists quando
	// (player, currency) já tem uma carteira.
	Insert(ctx context.Context, w *domain.Wallet) error
	// Get carrega uma carteira sem bloquear; ErrWalletNotFound se ausente.
	Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// GetForUpdate carrega e faz row-lock de uma carteira (SELECT ... FOR UPDATE).
	// Este é o ponto de coordenação por carteira: todo escritor de uma carteira
	// passa por aqui, e carteiras nunca compartilham um lock.
	GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// TryGetForUpdate é GetForUpdate com SKIP LOCKED: retorna
	// (nil, nil) quando outra transação mantém o lock.
	TryGetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	// UpdateBalance grava balance/version/updatedAt com compare-and-set
	// em expectedVersion; ErrConcurrentUpdate quando nenhuma linha correspondeu.
	UpdateBalance(ctx context.Context, w *domain.Wallet, expectedVersion int64) error
}

// TransactionRepository persiste transações de aposta.
type TransactionRepository interface {
	// Insert armazena uma transação. Corridas de constraint única são relatadas
	// como ErrRetryableConflict.
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	// Update persiste uma transição, protegida pelo status anterior.
	Update(ctx context.Context, t *domain.WagerTransaction, expectedStatus domain.Status) error
	// Get carrega uma transação; ErrNotFound se ausente.
	Get(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	// FindByExternalID resolve (providerId, externalTransactionId); retorna
	// (nil, nil) quando ausente.
	FindByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	// FindByIdempotency retorna as transações do provedor que correspondem
	// à chave de idempotência ou ao id externo (0, 1 ou 2 linhas).
	FindByIdempotency(ctx context.Context, providerID, key, externalID string) ([]*domain.WagerTransaction, error)
	// HasSuccessfulReversal informa se um REFUND ou ROLLBACK PROCESSED já
	// tem como alvo a transação.
	HasSuccessfulReversal(ctx context.Context, referenceID uuid.UUID) (bool, error)
	// ListDue retorna transações abertas cuja próxima tentativa está devida.
	ListDue(ctx context.Context, now time.Time, limit int) ([]DueTransaction, error)
}

// DueTransaction identifica uma transação que o worker de pendentes deve retomar.
type DueTransaction struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

// LedgerRepository adiciona e lê entradas do ledger.
type LedgerRepository interface {
	Insert(ctx context.Context, e domain.LedgerEntry) error
	// List retorna até limit entradas com seq > afterSeq, ordenadas por seq.
	List(ctx context.Context, walletID uuid.UUID, afterSeq int64, limit int) ([]LedgerRow, error)
	// Totals soma créditos e débitos (unidades menores) e conta entradas.
	Totals(ctx context.Context, walletID uuid.UUID) (LedgerTotals, error)
}

// LedgerRow é uma entrada do ledger com sua sequência de paginação.
type LedgerRow struct {
	Seq   int64
	Entry domain.LedgerEntry
}

// LedgerTotals agrega o ledger de uma carteira.
type LedgerTotals struct {
	CreditsMinor int64
	DebitsMinor  int64
	Count        int64
}

// OutboxRepository armazena eventos a serem publicados após o commit.
type OutboxRepository interface {
	Append(ctx context.Context, events ...domain.Event) error
}

// InboxRepository registra o tratamento concluído de mensagens de entrada.
type InboxRepository interface {
	// Get retorna o registro ou (nil, nil) quando ausente.
	Get(ctx context.Context, consumer, messageID string) (*InboxRecord, error)
	// Insert armazena o registro; uma duplicata é ErrRetryableConflict.
	Insert(ctx context.Context, r InboxRecord) error
}

// InboxRecord é o rastro durável de uma mensagem tratada.
type InboxRecord struct {
	Consumer      string
	MessageID     string
	PayloadHash   string
	TransactionID uuid.UUID
	Outcome       string
	ReceivedAt    time.Time
	CompletedAt   time.Time
}

// OutboxMessage é um evento do outbox reivindicado e pronto para publicação.
// Payload é o snapshot imutável do envelope armazenado no momento do commit.
type OutboxMessage struct {
	ID          uuid.UUID
	AggregateID string
	EventType   string
	Payload     string
	Attempts    int
	OccurredAt  time.Time
}

// OutboxStore é usado pelo relay do outbox, fora das transações de negócio.
type OutboxStore interface {
	// Claim reivindica lease de até limit eventos devidos para o owner.
	Claim(ctx context.Context, owner string, lease time.Duration, limit int) ([]OutboxMessage, error)
	// MarkPublished confirma uma publicação.
	MarkPublished(ctx context.Context, id uuid.UUID) error
	// MarkFailed libera o lease e agenda a próxima tentativa.
	MarkFailed(ctx context.Context, id uuid.UUID, owner, lastError string, next time.Time) error
	// Lag retorna a idade do evento não publicado mais antigo.
	Lag(ctx context.Context) (time.Duration, error)
}

// EventPublisher entrega um evento do outbox ao message broker.
type EventPublisher interface {
	Publish(ctx context.Context, m OutboxMessage) error
}

// Clock retorna o tempo atual (injetável para testes).
type Clock func() time.Time

// SystemClock é o relógio de produção (UTC).
func SystemClock() time.Time { return time.Now().UTC() }

// NewUUIDv7 gera identificadores ordenados por tempo, o que mantém as
// inserções em B-tree locais e torna os ids aproximadamente ordenáveis por criação.
func NewUUIDv7() uuid.UUID { return uuid.Must(uuid.NewV7()) }
