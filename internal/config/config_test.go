package config

import (
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadRequiresMandatoryValues(t *testing.T) {
	_, err := LoadFrom(env(map[string]string{"INSTANCE_ID": "x"}))
	if err == nil || !strings.Contains(err.Error(), "DATABASE_URL") || !strings.Contains(err.Error(), "OIDC_ISSUER") {
		t.Fatalf("expected missing values, got %v", err)
	}
}

func TestLoadDefaultsAndDerivedJWKS(t *testing.T) {
	c, err := LoadFrom(env(map[string]string{
		"INSTANCE_ID": "x", "DATABASE_URL": "postgres://x", "OIDC_ISSUER": "http://idp/realms/r",
	}))
	if err != nil {
		t.Fatal(err)
	}
	if c.OIDC.JWKSURL != "http://idp/realms/r/protocol/openid-connect/certs" || c.SQS.InboundQueue != "wager-transactions.fifo" {
		t.Fatalf("defaults: %+v", c)
	}
}

func TestValidateTimeouts(t *testing.T) {
	_, err := LoadFrom(env(map[string]string{
		"INSTANCE_ID": "x", "DATABASE_URL": "postgres://x", "OIDC_ISSUER": "http://idp",
		"SQS_HANDLER_TIMEOUT": "40s", "SQS_VISIBILITY_TIMEOUT": "30s",
	}))
	if err == nil || !strings.Contains(err.Error(), "SQS_HANDLER_TIMEOUT") {
		t.Fatalf("expected timeout validation error, got %v", err)
	}
	_, err = LoadFrom(env(map[string]string{"INSTANCE_ID": "x", "DATABASE_URL": "d", "OIDC_ISSUER": "i", "SQS_POLLERS": "abc"}))
	if err == nil {
		t.Fatal("expected parse error")
	}
}
