// Package auth is the development identity adapter: stateless HMAC-signed
// bearer tokens naming a subject. A real OIDC/JWT adapter replaces Verify
// without touching handlers, which only see an Identity.
//
// Subjects:
//
//	tenant:<slug>  member of exactly one tenant
//	operator       may read every tenant and the /monitor stats
package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"strings"
)

type Identity struct {
	Subject    string
	TenantSlug string // set for tenant identities
	Operator   bool
}

// CanRead reports whether the identity is a member of the tenant.
func (id Identity) CanRead(slug string) bool {
	return id.Operator || (id.TenantSlug != "" && id.TenantSlug == slug)
}

type Signer struct{ key []byte }

func NewSigner(secret string) (*Signer, error) {
	if len(secret) < 16 {
		return nil, errors.New("AUTH_SECRET must be at least 16 characters")
	}
	return &Signer{key: []byte(secret)}, nil
}

const prefix = "tpdev."

func (s *Signer) Issue(subject string) string {
	p := base64.RawURLEncoding.EncodeToString([]byte(subject))
	return prefix + p + "." + s.sign(p)
}

func (s *Signer) sign(payload string) string {
	m := hmac.New(sha256.New, s.key)
	m.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(m.Sum(nil))
}

var ErrInvalidToken = errors.New("invalid token")

func (s *Signer) Verify(token string) (Identity, error) {
	token = strings.TrimSpace(strings.TrimPrefix(token, "Bearer "))
	if !strings.HasPrefix(token, prefix) {
		return Identity{}, ErrInvalidToken
	}
	parts := strings.Split(token[len(prefix):], ".")
	if len(parts) != 2 || !hmac.Equal([]byte(s.sign(parts[0])), []byte(parts[1])) {
		return Identity{}, ErrInvalidToken
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Identity{}, ErrInvalidToken
	}
	return ParseSubject(string(raw))
}

func ParseSubject(sub string) (Identity, error) {
	switch {
	case sub == "operator":
		return Identity{Subject: sub, Operator: true}, nil
	case strings.HasPrefix(sub, "tenant:") && len(sub) > len("tenant:"):
		return Identity{Subject: sub, TenantSlug: sub[len("tenant:"):]}, nil
	}
	return Identity{}, ErrInvalidToken
}
