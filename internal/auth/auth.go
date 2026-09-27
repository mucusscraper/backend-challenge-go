// Package auth validates OAuth 2.0 access tokens issued by the external
// OIDC provider (Keycloak) and derives the caller's Principal.
//
// Validation: RS256 signature against the issuer's JWKS (keys cached and
// refreshed on rotation by go-oidc), "iss" equal to the configured issuer,
// "aud" containing the API audience, "exp" checked, and "typ" equal
// to "Bearer" so ID tokens or refresh tokens cannot be replayed as access
// tokens.
//
// Authorization model (realm roles in realm_access.roles):
//   - "wagering-provider": a game provider. The provider identity comes
//     exclusively from the provider_id claim (a hard-coded claim mapper on
//     the provider's Keycloak client); request bodies can never widen it.
//   - "wallet-operator": the internal service that opens wallets, reads them,
//     their ledger, reconciles, and may read any transaction.
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

// Realm roles understood by the API.
const (
	RoleProvider       = "wagering-provider"
	RoleWalletOperator = "wallet-operator"
)

// Errors returned by Authenticate.
var (
	ErrMissingToken = errors.New("missing bearer token")
	ErrInvalidToken = errors.New("invalid or expired token")
)

// Principal is the authenticated caller.
type Principal struct {
	Subject    string
	ClientID   string
	ProviderID string
	Roles      []string
}

// HasRole reports whether the principal has a realm role.
func (p Principal) HasRole(role string) bool { return slices.Contains(p.Roles, role) }

// IsProvider reports whether the caller acts as a game provider. Both the
// role and a non-empty provider_id claim are required.
func (p Principal) IsProvider() bool { return p.HasRole(RoleProvider) && p.ProviderID != "" }

// IsWalletOperator reports whether the caller is the internal service.
func (p Principal) IsWalletOperator() bool { return p.HasRole(RoleWalletOperator) }

// CanAccessProvider reports whether the caller may see data of providerID.
func (p Principal) CanAccessProvider(providerID string) bool {
	return p.IsWalletOperator() || (p.IsProvider() && p.ProviderID == providerID)
}

// Verifier validates bearer tokens.
type Verifier struct {
	verifier      *oidc.IDTokenVerifier
	providerClaim string
}

// NewVerifier builds a verifier. The JWKS URL may point to an internal
// hostname while the issuer is the public one (as in docker compose).
func NewVerifier(cfg config.OIDCConfig) *Verifier {
	keySet := oidc.NewRemoteKeySet(context.Background(), cfg.JWKSURL)
	v := oidc.NewVerifier(cfg.Issuer, keySet, &oidc.Config{
		ClientID:             cfg.Audience,
		SupportedSigningAlgs: []string{oidc.RS256},
	})
	return &Verifier{verifier: v, providerClaim: cfg.ProviderClaim}
}

// claims are the fields read from a Keycloak access token.
type claims struct {
	Type        string `json:"typ"`
	AZP         string `json:"azp"`
	RealmAccess struct {
		Roles []string `json:"roles"`
	} `json:"realm_access"`
}

// Authenticate validates the Authorization header value and returns the
// principal. Errors never contain the token itself.
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

// redact keeps only the error category from go-oidc messages.
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

// WithPrincipal stores the principal in the context.
func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, principalKey{}, p)
}

// FromContext returns the principal stored by the middleware.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}
