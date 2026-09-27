// Package auth valida tokens de acesso OAuth 2.0 emitidos pelo provedor OIDC
// externo (Keycloak) e deriva o Principal do chamador.
//
// Validação: assinatura RS256 contra o JWKS do issuer (chaves cacheadas e
// atualizadas na rotação pelo go-oidc), "iss" igual ao issuer configurado,
// "aud" contendo a audience da API, "exp" verificado, e "typ" igual a
// "Bearer" para que tokens de ID ou de refresh não possam ser reaproveitados
// como tokens de acesso.
//
// Modelo de autorização (papéis do realm em realm_access.roles):
//   - "wagering-provider": um provedor de jogo. A identidade do provedor vem
//     exclusivamente do claim provider_id (um mapper de claim hard-coded no
//     cliente Keycloak do provedor); o corpo da requisição nunca pode ampliá-la.
//   - "wallet-operator": o serviço interno que abre carteiras, as lê,
//     lê o ledger, reconcilia e pode ler qualquer transação.
package auth

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"

	"github.com/mucusscraper/backend-challenge-go/internal/config"
)

// Papéis do realm reconhecidos pela API.
const (
	RoleProvider       = "wagering-provider"
	RoleWalletOperator = "wallet-operator"
)

// Erros retornados por Authenticate.
var (
	ErrMissingToken = errors.New("missing bearer token")
	ErrInvalidToken = errors.New("invalid or expired token")
)

// Principal é o chamador autenticado.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	Roles      []string
}

// HasRole informa se o principal possui um papel do realm.
func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

// IsProvider informa se o chamador age como provedor de jogo. São necessários
// tanto o papel quanto um claim provider_id não vazio.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// IsWalletOperator informa se o chamador é o serviço interno.
func (p Principal) IsWalletOperator() bool { return p.HasRole(RoleWalletOperator) }

// CanAccessProvider informa se o chamador pode ver dados do providerID.
func (p Principal) CanAccessProvider(providerID string) bool {
	return p.IsWalletOperator() || (p.IsProvider() && p.ProviderID == providerID)
}

// Verifier valida bearer tokens.
type Verifier struct {
	verifier      *oidc.IDTokenVerifier
	providerClaim string
}

// NewVerifier constrói um verifier. A URL do JWKS pode apontar para um hostname
// interno enquanto o issuer é o público (como no docker compose).
func NewVerifier(cfg config.OIDCConfig) *Verifier {
	keySet := oidc.NewRemoteKeySet(context.Background(), cfg.JWKSURL)
	v := oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return &Verifier{verifier: v, providerClaim: cfg.ProviderClaim}
}

// claims são os campos lidos de um access token do Keycloak.
type claims struct {
	Type        string `json:"typ"`
	AZP         string `json:"azp"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Authenticate valida o valor do header Authorization e retorna o principal.
// Os erros nunca contêm o próprio token.
func (v *Verifier) Authenticate(ctx context.Context, authorization string) (Principal, error) {
	raw, ok := strings.CutPrefix(authorization, "Bearer ")
	if !ok || strings.TrimSpace(raw) == "" {
		return Principal{}, ErrMissingToken
	}
	tok, err := v.verifier.Verify(ctx, strings.TrimSpace(raw))
	if err != nil {
		return Principal{}, fmt.Errorf("%w: %s", ErrInvalidToken, redact(err))
	}
	var c claims
	if err := tok.Claims(&c); err != nil {
		return Principal{}, fmt.Errorf("%w: claims", ErrInvalidToken)
	}
	if !strings.EqualFold(c.Type, "Bearer") {
		return Principal{}, fmt.Errorf("%w: not an access token", ErrInvalidToken)
	}
	var extra map[string]any
	if err := tok.Claims(&extra); err != nil {
		return Principal{}, fmt.Errorf("%w: claims", ErrInvalidToken)
	}
	provider, _ := extra[v.providerClaim].(string)
	return Principal{
		Subject:    tok.Subject,
		ClientID:   c.AZP,
		ProviderID: provider,
		Roles:      c.RealmAccess.Roles,
	}, nil
}

// redact mantém apenas a categoria do erro das mensagens do go-oidc.
func redact(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "expired"):
		return "token expired"
	case strings.Contains(msg, "audience"):
		return "audience mismatch"
	case strings.Contains(msg, "issuer"):
		return "issuer mismatch"
	case strings.Contains(msg, "signature"):
		return "bad signature"
	default:
		return "malformed token"
	}
}

type principalKey struct{}

// WithPrincipal armazena o principal no contexto.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext retorna o principal armazenado pelo middleware.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
